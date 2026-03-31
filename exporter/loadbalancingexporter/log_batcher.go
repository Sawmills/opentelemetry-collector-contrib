// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	logFlushReasonDirect         = "direct"
	logFlushReasonSize           = "size"
	logFlushReasonTimeout        = "timeout"
	logFlushReasonShutdown       = "shutdown"
	logFlushReasonResolverChange = "resolver_change"

	defaultLogBatchMaxRecords   = 512
	defaultLogBatchMaxBytes     = 1 << 20
	defaultLogBatchFlushTimeout = 100 * time.Millisecond
)

var errLogBatcherExporterStopping = errors.New("log batcher exporter is stopping")

var errLogBatcherRemovedBackendPartial = errors.New("removed backend reroute partially dropped records")

const (
	removedBackendOutcomeEmpty          = "empty"
	removedBackendOutcomeFailed         = "failed"
	removedBackendOutcomePartial        = "partial"
	removedBackendOutcomeRerouted       = "rerouted"
	removedBackendOutcomeRetryFailed    = "retry_failed"
	removedBackendOutcomeRetrySucceeded = "retry_succeeded"
)

type logBatcherSettings struct {
	maxRecords    int
	maxBytes      int
	flushInterval time.Duration
}

type logBatcherSendFunc func(context.Context, *wrappedExporter, plog.Logs, string) error

type logBatcherOnBackendRemovedFunc func(context.Context, string, plog.Logs, int, int) error

type logBatcherOption interface {
	apply(*logBatcher)
}

type logBatcherOptionFunc func(*logBatcher)

func (f logBatcherOptionFunc) apply(b *logBatcher) {
	f(b)
}

func withLogBatcherOnBackendRemoved(fn logBatcherOnBackendRemovedFunc) logBatcherOption {
	return logBatcherOptionFunc(func(b *logBatcher) {
		b.onBackendRemoved = fn
	})
}

type logBatcher struct {
	logger   *zap.Logger
	settings logBatcherSettings
	send     logBatcherSendFunc

	telemetry        *logBatcherTelemetry
	onBackendRemoved logBatcherOnBackendRemovedFunc

	mu       sync.RWMutex
	backends map[string]*backendLogBatcher
	stopped  bool
}

type logBatcherRequest struct {
	kind   logBatcherRequestKind
	logs   plog.Logs
	ctx    context.Context
	reason string
	done   chan error
	drain  chan logBatcherDrainResult
}

type logBatcherRequestKind int

const (
	logBatcherRequestEnqueue logBatcherRequestKind = iota
	logBatcherRequestFlushAndStop
	logBatcherRequestDrainAndStop
)

type logBatcherDrainResult struct {
	logs    plog.Logs
	records int
	bytes   int
}

type backendLogBatcher struct {
	endpoint string
	logger   *zap.Logger
	settings logBatcherSettings
	send     logBatcherSendFunc

	telemetry *logBatcherTelemetry

	exporterMu sync.RWMutex
	exp        *wrappedExporter

	requests chan logBatcherRequest
	done     chan struct{}
	inflight sync.WaitGroup

	pendingRecords atomic.Int64
	pendingBytes   atomic.Int64
}

type logBatcherTelemetry struct {
	logger         *zap.Logger
	meter          metric.Meter
	batchSize      metric.Int64Histogram
	batchBytes     metric.Int64Histogram
	flushTotal     metric.Int64Counter
	flushErrors    metric.Int64Counter
	droppedRecords metric.Int64Counter
	overflowTotal  metric.Int64Counter
	pendingRecords metric.Int64ObservableGauge
	pendingBytes   metric.Int64ObservableGauge
	removedTotal   metric.Int64Counter
	removedRecords metric.Int64Histogram
	removedBytes   metric.Int64Histogram

	mu            sync.Mutex
	registrations []metric.Registration
}

func newLogBatcher(
	logger *zap.Logger,
	settings component.TelemetrySettings,
	cfg logBatcherSettings,
	send logBatcherSendFunc,
	options ...logBatcherOption,
) (*logBatcher, error) {
	telemetry, err := newLogBatcherTelemetry(settings)
	if err != nil {
		return nil, err
	}

	lb := &logBatcher{
		logger:    logger,
		settings:  cfg,
		send:      send,
		telemetry: telemetry,
		backends:  make(map[string]*backendLogBatcher),
	}
	for _, option := range options {
		option.apply(lb)
	}

	if err := telemetry.start(lb.snapshotPending); err != nil {
		return nil, err
	}

	return lb, nil
}

func (b *logBatcher) Enqueue(ctx context.Context, endpoint string, exp *wrappedExporter, logs plog.Logs) error {
	backend, err := b.acquireBackend(endpoint, exp)
	if err != nil {
		return err
	}
	defer backend.inflight.Done()
	select {
	case backend.requests <- logBatcherRequest{kind: logBatcherRequestEnqueue, logs: logs}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-backend.done:
		return errors.New("log batcher backend is stopped")
	}
}

func (b *logBatcher) Remove(ctx context.Context, endpoint string, exp *wrappedExporter) error {
	b.mu.Lock()
	backend, ok := b.backends[endpoint]
	if ok {
		delete(b.backends, endpoint)
	}
	b.mu.Unlock()
	if !ok {
		return nil
	}
	if exp != nil {
		backend.setExporter(exp)
	}
	if b.onBackendRemoved != nil {
		return b.removeAndReroute(ctx, endpoint, backend)
	}
	if err := waitForInflight(ctx, &backend.inflight); err != nil {
		b.scheduleBackendCleanup(backend, logFlushReasonResolverChange)
		return err
	}
	return backend.stopAndFlush(ctx, logFlushReasonResolverChange)
}

func (b *logBatcher) removeAndReroute(ctx context.Context, endpoint string, backend *backendLogBatcher) error {
	if err := waitForInflight(ctx, &backend.inflight); err != nil {
		b.scheduleBackendDrain(backend)
		return err
	}
	if err := b.drainRemovedBackend(ctx, endpoint, backend); err != nil {
		select {
		case <-backend.done:
		default:
			b.scheduleBackendDrain(backend)
		}
		return err
	}
	return nil
}

func (b *logBatcher) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	b.stopped = true
	backends := make([]*backendLogBatcher, 0, len(b.backends))
	for endpoint, backend := range b.backends {
		backends = append(backends, backend)
		delete(b.backends, endpoint)
	}
	b.mu.Unlock()

	var errs error
	for _, backend := range backends {
		if err := waitForInflight(ctx, &backend.inflight); err != nil {
			b.scheduleBackendCleanup(backend, logFlushReasonShutdown)
			errs = errors.Join(errs, err)
			continue
		}
		errs = errors.Join(errs, backend.stopAndFlush(ctx, logFlushReasonShutdown))
	}
	b.telemetry.shutdown()
	return errs
}

func (b *logBatcher) scheduleBackendCleanup(backend *backendLogBatcher, reason string) {
	go func() {
		if err := waitForInflight(context.Background(), &backend.inflight); err != nil {
			b.logger.Warn("failed waiting for inflight log batcher requests during background cleanup", zap.String("endpoint", backend.endpoint), zap.Error(err))
			return
		}
		if err := backend.stopAndFlush(context.Background(), reason); err != nil {
			b.logger.Warn("failed to stop log batcher backend during background cleanup", zap.String("endpoint", backend.endpoint), zap.String("reason", reason), zap.Error(err))
		}
	}()
}

func (b *logBatcher) scheduleBackendDrain(backend *backendLogBatcher) {
	go func() {
		if err := waitForInflight(context.Background(), &backend.inflight); err != nil {
			b.recordRemovedBackendOutcome(context.Background(), backend.endpoint, removedBackendOutcomeFailed)
			b.logger.Error("failed waiting for inflight log batcher requests during background removed-backend drain", zap.String("endpoint", backend.endpoint), zap.Error(err))
			return
		}
		if err := b.drainRemovedBackend(context.Background(), backend.endpoint, backend); err != nil {
			b.logger.Error("failed to reroute removed backend log batch", zap.String("removed_endpoint", backend.endpoint), zap.Error(err))
		}
	}()
}

func (b *logBatcher) drainRemovedBackend(ctx context.Context, endpoint string, backend *backendLogBatcher) error {
	drained, err := backend.stopAndDrain(ctx)
	if err != nil {
		b.recordRemovedBackendOutcome(ctx, endpoint, removedBackendOutcomeFailed)
		return err
	}
	if drained.records == 0 {
		b.recordRemovedBackendOutcome(ctx, endpoint, removedBackendOutcomeEmpty)
		return nil
	}
	b.recordRemovedBackendSize(ctx, endpoint, drained.records, drained.bytes)
	if err := b.onBackendRemoved(ctx, endpoint, drained.logs, drained.records, drained.bytes); err != nil {
		if errors.Is(err, errLogBatcherRemovedBackendPartial) {
			b.recordRemovedBackendOutcome(ctx, endpoint, removedBackendOutcomePartial)
			return nil
		}
		b.recordRemovedBackendOutcome(ctx, endpoint, removedBackendOutcomeFailed)
		return err
	}
	b.recordRemovedBackendOutcome(ctx, endpoint, removedBackendOutcomeRerouted)
	return nil
}

func (b *logBatcher) snapshotPending() []logBatcherPending {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pending := make([]logBatcherPending, 0, len(b.backends))
	for endpoint, backend := range b.backends {
		pending = append(pending, logBatcherPending{
			endpoint: endpoint,
			records:  backend.pendingRecords.Load(),
			bytes:    backend.pendingBytes.Load(),
		})
	}
	return pending
}

func (b *logBatcher) acquireBackend(endpoint string, exp *wrappedExporter) (*backendLogBatcher, error) {
	b.mu.RLock()
	backend, ok := b.backends[endpoint]
	if ok {
		if exp != nil && exp.isStopping() {
			b.mu.RUnlock()
			return nil, errLogBatcherExporterStopping
		}
		backend.setExporter(exp)
		backend.inflight.Add(1)
		b.mu.RUnlock()
		return backend, nil
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	backend, ok = b.backends[endpoint]
	if ok {
		if exp != nil && exp.isStopping() {
			return nil, errLogBatcherExporterStopping
		}
		backend.setExporter(exp)
		backend.inflight.Add(1)
		return backend, nil
	}
	if exp != nil && exp.isStopping() {
		return nil, errLogBatcherExporterStopping
	}
	if b.stopped {
		return nil, errLogBatcherExporterStopping
	}

	backend = newBackendLogBatcher(endpoint, exp, b.logger, b.settings, b.telemetry, b.send)
	backend.inflight.Add(1)
	b.backends[endpoint] = backend
	return backend, nil
}

func newBackendLogBatcher(
	endpoint string,
	exp *wrappedExporter,
	logger *zap.Logger,
	settings logBatcherSettings,
	telemetry *logBatcherTelemetry,
	send logBatcherSendFunc,
) *backendLogBatcher {
	backend := &backendLogBatcher{
		endpoint:  endpoint,
		exp:       exp,
		logger:    logger.With(zap.String("endpoint", endpoint)),
		settings:  settings,
		send:      send,
		telemetry: telemetry,
		requests:  make(chan logBatcherRequest, 16),
		done:      make(chan struct{}),
	}

	go backend.run()
	return backend
}

func (b *backendLogBatcher) stopAndFlush(ctx context.Context, reason string) error {
	done := make(chan error, 1)
	select {
	case b.requests <- logBatcherRequest{
		kind:   logBatcherRequestFlushAndStop,
		ctx:    ctx,
		reason: reason,
		done:   done,
	}:
	case <-b.done:
		return nil
	}

	select {
	case err := <-done:
		<-b.done
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *backendLogBatcher) stopAndDrain(ctx context.Context) (logBatcherDrainResult, error) {
	done := make(chan logBatcherDrainResult, 1)
	select {
	case b.requests <- logBatcherRequest{
		kind:  logBatcherRequestDrainAndStop,
		ctx:   ctx,
		drain: done,
	}:
	case <-ctx.Done():
		return logBatcherDrainResult{}, ctx.Err()
	case <-b.done:
		return logBatcherDrainResult{}, nil
	}

	select {
	case result := <-done:
		<-b.done
		return result, nil
	case <-ctx.Done():
		return logBatcherDrainResult{}, ctx.Err()
	}
}

func (b *backendLogBatcher) setExporter(exp *wrappedExporter) {
	b.exporterMu.Lock()
	b.exp = exp
	b.exporterMu.Unlock()
}

func (b *backendLogBatcher) exporter() *wrappedExporter {
	b.exporterMu.RLock()
	defer b.exporterMu.RUnlock()
	return b.exp
}

func waitForInflight(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *backendLogBatcher) run() {
	defer close(b.done)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time

	pending := plog.NewLogs()
	pendingBytes := 0
	pendingRecords := 0
	sizer := &plog.ProtoMarshaler{}
	var nextReq *logBatcherRequest

	for {
		if nextReq != nil {
			req := *nextReq
			nextReq = nil
			if b.handleRequest(req, sizer, &pending, &pendingRecords, &pendingBytes, &nextReq, timer, &timerC) {
				return
			}
			continue
		}

		select {
		case req := <-b.requests:
			if b.handleRequest(req, sizer, &pending, &pendingRecords, &pendingBytes, &nextReq, timer, &timerC) {
				return
			}
		case <-timerC:
			if err := b.flush(context.Background(), &pending, &pendingRecords, &pendingBytes, logFlushReasonTimeout, timer, &timerC); err != nil {
				b.logger.Warn("failed to flush log batch", zap.String("reason", logFlushReasonTimeout), zap.Error(err))
			}
		}
	}
}

func (b *backendLogBatcher) handleRequest(
	req logBatcherRequest,
	sizer *plog.ProtoMarshaler,
	pending *plog.Logs,
	pendingRecords *int,
	pendingBytes *int,
	nextReq **logBatcherRequest,
	timer *time.Timer,
	timerC *<-chan time.Time,
) bool {
	switch req.kind {
	case logBatcherRequestEnqueue:
		*pendingRecords += b.mergeQueuedRequests(pending, req, nextReq)
		// Track max_bytes using the actual serialized merged OTLP payload.
		// Resource/scope dedup during merge means chunk sizes are not additive.
		*pendingBytes = sizer.LogsSize(*pending)
		b.pendingRecords.Store(int64(*pendingRecords))
		b.pendingBytes.Store(int64(*pendingBytes))
		if *pendingRecords > 0 && *timerC == nil {
			timer.Reset(b.settings.flushInterval)
			*timerC = timer.C
		}
		if *pendingRecords >= b.settings.maxRecords || *pendingBytes >= b.settings.maxBytes {
			b.telemetry.overflowTotal.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
			if err := b.flush(context.Background(), pending, pendingRecords, pendingBytes, logFlushReasonSize, timer, timerC); err != nil {
				b.logger.Warn("failed to flush log batch", zap.String("reason", logFlushReasonSize), zap.Error(err))
			}
		}
	case logBatcherRequestFlushAndStop:
		err := b.flush(req.ctx, pending, pendingRecords, pendingBytes, req.reason, timer, timerC)
		req.done <- err
		return true
	case logBatcherRequestDrainAndStop:
		req.drain <- b.drain(pending, pendingRecords, pendingBytes, timer, timerC)
		return true
	}
	return false
}

func (b *backendLogBatcher) mergeQueuedRequests(pending *plog.Logs, first logBatcherRequest, nextReq **logBatcherRequest) int {
	recordCount := first.logs.LogRecordCount()
	*pending = mergeLogs(*pending, first.logs)

	for i := 0; i < cap(b.requests); i++ {
		select {
		case req := <-b.requests:
			if req.kind != logBatcherRequestEnqueue {
				*nextReq = &req
				return recordCount
			}
			recordCount += req.logs.LogRecordCount()
			*pending = mergeLogs(*pending, req.logs)
		default:
			return recordCount
		}
	}

	return recordCount
}

func (b *backendLogBatcher) flush(
	ctx context.Context,
	pending *plog.Logs,
	pendingRecords *int,
	pendingBytes *int,
	reason string,
	timer *time.Timer,
	timerC *<-chan time.Time,
) error {
	if !timer.Stop() && *timerC != nil {
		select {
		case <-timer.C:
		default:
		}
	}
	*timerC = nil

	if pending.LogRecordCount() == 0 {
		return nil
	}

	drained := *pending
	records := *pendingRecords
	bytes := *pendingBytes
	*pending = plog.NewLogs()
	*pendingRecords = 0
	*pendingBytes = 0
	b.pendingRecords.Store(0)
	b.pendingBytes.Store(0)

	b.telemetry.batchSize.Record(ctx, int64(records), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	b.telemetry.batchBytes.Record(ctx, int64(bytes), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	b.telemetry.flushTotal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint), attribute.String("reason", reason))))

	err := b.send(ctx, b.exporter(), drained, reason)
	if err != nil {
		b.telemetry.flushErrors.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
		b.telemetry.droppedRecords.Add(ctx, int64(records), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	}
	return err
}

func (b *backendLogBatcher) drain(
	pending *plog.Logs,
	pendingRecords *int,
	pendingBytes *int,
	timer *time.Timer,
	timerC *<-chan time.Time,
) logBatcherDrainResult {
	if !timer.Stop() && *timerC != nil {
		select {
		case <-timer.C:
		default:
		}
	}
	*timerC = nil

	if pending.LogRecordCount() == 0 {
		return logBatcherDrainResult{logs: plog.NewLogs()}
	}

	drained := *pending
	records := *pendingRecords
	bytes := *pendingBytes
	*pending = plog.NewLogs()
	*pendingRecords = 0
	*pendingBytes = 0
	b.pendingRecords.Store(0)
	b.pendingBytes.Store(0)

	return logBatcherDrainResult{
		logs:    drained,
		records: records,
		bytes:   bytes,
	}
}

type logBatcherPending struct {
	endpoint string
	records  int64
	bytes    int64
}

func newLogBatcherTelemetry(settings component.TelemetrySettings) (*logBatcherTelemetry, error) {
	meter := metadata.Meter(settings)
	var err, errs error

	t := &logBatcherTelemetry{logger: settings.Logger, meter: meter}
	t.batchSize, err = meter.Int64Histogram(
		"otelcol_loadbalancer_log_batch_size",
		metric.WithDescription("Number of log records per flushed backend batch."),
		metric.WithUnit("{records}"),
	)
	errs = errors.Join(errs, err)
	t.batchBytes, err = meter.Int64Histogram(
		"otelcol_loadbalancer_log_batch_bytes",
		metric.WithDescription("Serialized OTLP bytes per flushed backend batch before compression."),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)
	t.flushTotal, err = meter.Int64Counter(
		"otelcol_loadbalancer_log_batch_flush_total",
		metric.WithDescription("Number of log batch flushes by endpoint and reason."),
		metric.WithUnit("{flushes}"),
	)
	errs = errors.Join(errs, err)
	t.flushErrors, err = meter.Int64Counter(
		"otelcol_loadbalancer_log_batch_flush_errors",
		metric.WithDescription("Number of log batch flush errors."),
		metric.WithUnit("{errors}"),
	)
	errs = errors.Join(errs, err)
	t.droppedRecords, err = meter.Int64Counter(
		"otelcol_loadbalancer_log_batch_dropped_records",
		metric.WithDescription("Number of dropped log records in the internal log batcher."),
		metric.WithUnit("{records}"),
	)
	errs = errors.Join(errs, err)
	t.overflowTotal, err = meter.Int64Counter(
		"otelcol_loadbalancer_log_batch_overflow_total",
		metric.WithDescription("Number of times an internal log batch hit a size bound and was force-flushed."),
		metric.WithUnit("{overflows}"),
	)
	errs = errors.Join(errs, err)
	t.pendingRecords, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_log_batch_pending_records",
		metric.WithDescription("Current number of pending log records per backend batch."),
		metric.WithUnit("{records}"),
	)
	errs = errors.Join(errs, err)
	t.pendingBytes, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_log_batch_pending_bytes",
		metric.WithDescription("Current serialized OTLP bytes per pending backend batch before compression."),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)
	t.removedTotal, err = meter.Int64Counter(
		"otelcol_loadbalancer_log_batch_removed_backend_total",
		metric.WithDescription("Number of removed backend drain attempts by outcome."),
		metric.WithUnit("{removals}"),
	)
	errs = errors.Join(errs, err)
	t.removedRecords, err = meter.Int64Histogram(
		"otelcol_loadbalancer_log_batch_removed_backend_records",
		metric.WithDescription("Number of log records drained from removed backends before reroute."),
		metric.WithUnit("{records}"),
	)
	errs = errors.Join(errs, err)
	t.removedBytes, err = meter.Int64Histogram(
		"otelcol_loadbalancer_log_batch_removed_backend_bytes",
		metric.WithDescription("Serialized OTLP bytes drained from removed backends before reroute."),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)

	return t, errs
}

func (b *logBatcher) recordRemovedBackendOutcome(ctx context.Context, endpoint, outcome string) {
	b.telemetry.removedTotal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("endpoint", endpoint),
		attribute.String("outcome", outcome),
	)))
}

func (b *logBatcher) recordRemovedBackendSize(ctx context.Context, endpoint string, records, bytes int) {
	if records > 0 {
		b.telemetry.removedRecords.Record(ctx, int64(records), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", endpoint))))
	}
	if bytes > 0 {
		b.telemetry.removedBytes.Record(ctx, int64(bytes), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", endpoint))))
	}
}

func (t *logBatcherTelemetry) start(snapshot func() []logBatcherPending) error {
	reg, err := t.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		for _, pending := range snapshot() {
			attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", pending.endpoint)))
			observer.ObserveInt64(t.pendingRecords, pending.records, attrs)
			observer.ObserveInt64(t.pendingBytes, pending.bytes, attrs)
		}
		return nil
	}, t.pendingRecords, t.pendingBytes)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.registrations = append(t.registrations, reg)
	t.mu.Unlock()
	return nil
}

func (t *logBatcherTelemetry) shutdown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, reg := range t.registrations {
		if err := reg.Unregister(); err != nil {
			t.logger.Warn("failed to unregister log batcher metric callback", zap.Error(err))
		}
	}
	t.registrations = nil
}
