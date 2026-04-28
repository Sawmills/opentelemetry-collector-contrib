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

type logBatcherSettings struct {
	maxRecords    int
	maxBytes      int
	flushInterval time.Duration
}

type logBatcherSendFunc func(context.Context, *wrappedExporter, plog.Logs, string) error
type logBatcherDrainFailureFunc func(context.Context, plog.Logs, string) error

type logBatcher struct {
	logger   *zap.Logger
	settings logBatcherSettings
	send     logBatcherSendFunc
	drainErr logBatcherDrainFailureFunc

	telemetry *logBatcherTelemetry

	mu       sync.RWMutex
	backends map[string]*backendLogBatcher
	stopped  atomic.Bool
}

type logBatcherRequest struct {
	kind               logBatcherRequestKind
	logs               plog.Logs
	ctx                context.Context
	reason             string
	done               chan error
	enqueuedAtUnixNano int64
}

type logBatcherRequestKind int

const (
	logBatcherRequestEnqueue logBatcherRequestKind = iota
	logBatcherRequestFlushAndStop
)

type backendLogBatcher struct {
	endpoint string
	logger   *zap.Logger
	settings logBatcherSettings
	send     logBatcherSendFunc
	drainErr logBatcherDrainFailureFunc

	telemetry *logBatcherTelemetry

	exporterMu sync.RWMutex
	exp        *wrappedExporter

	requests chan logBatcherRequest
	done     chan struct{}
	inflight sync.WaitGroup

	pendingRecords atomic.Int64
	pendingBytes   atomic.Int64
	oldestEnqueue  atomic.Int64
}

type logBatcherTelemetry struct {
	logger               *zap.Logger
	meter                metric.Meter
	batchSize            metric.Int64Histogram
	batchBytes           metric.Int64Histogram
	flushTotal           metric.Int64Counter
	flushErrors          metric.Int64Counter
	flushOldestRecordAge metric.Int64Histogram
	droppedRecords       metric.Int64Counter
	overflowTotal        metric.Int64Counter
	pendingRecords       metric.Int64ObservableGauge
	pendingBytes         metric.Int64ObservableGauge
	pendingOldestAge     metric.Int64ObservableGauge
	pendingOldestAgeMax  metric.Int64ObservableGauge

	mu            sync.Mutex
	registrations []metric.Registration
}

func newLogBatcher(
	logger *zap.Logger,
	settings component.TelemetrySettings,
	cfg logBatcherSettings,
	send logBatcherSendFunc,
	drainErr ...logBatcherDrainFailureFunc,
) (*logBatcher, error) {
	telemetry, err := newLogBatcherTelemetry(settings)
	if err != nil {
		return nil, err
	}
	var drainFailure logBatcherDrainFailureFunc
	if len(drainErr) > 0 {
		drainFailure = drainErr[0]
	}

	lb := &logBatcher{
		logger:    logger,
		settings:  cfg,
		send:      send,
		drainErr:  drainFailure,
		telemetry: telemetry,
		backends:  make(map[string]*backendLogBatcher),
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
	enqueuedAtUnixNano := time.Now().UnixNano()
	select {
	case backend.requests <- logBatcherRequest{kind: logBatcherRequestEnqueue, logs: logs, enqueuedAtUnixNano: enqueuedAtUnixNano}:
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
	if err := waitForInflight(ctx, &backend.inflight); err != nil {
		b.scheduleBackendCleanup(backend, logFlushReasonResolverChange)
		return err
	}
	return backend.stopAndFlush(ctx, logFlushReasonResolverChange)
}

func (b *logBatcher) Shutdown(ctx context.Context) error {
	b.stopped.Store(true)

	b.mu.Lock()
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

type logBatcherSnapshot struct {
	pending            []logBatcherPending
	maxOldestAgeMillis int64
}

func (b *logBatcher) snapshotPending() logBatcherSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	now := time.Now()
	pending := make([]logBatcherPending, 0, len(b.backends))
	var maxOldestAgeMillis int64
	for endpoint, backend := range b.backends {
		records := backend.pendingRecords.Load()
		bytes := backend.pendingBytes.Load()
		var oldestAgeMillis int64
		if records > 0 {
			oldestAgeMillis = ageMillisFromUnixNano(now, backend.oldestEnqueue.Load())
		}
		if oldestAgeMillis > maxOldestAgeMillis {
			maxOldestAgeMillis = oldestAgeMillis
		}
		pending = append(pending, logBatcherPending{
			endpoint:        endpoint,
			records:         records,
			bytes:           bytes,
			oldestAgeMillis: oldestAgeMillis,
		})
	}
	return logBatcherSnapshot{pending: pending, maxOldestAgeMillis: maxOldestAgeMillis}
}

func (b *logBatcher) acquireBackend(endpoint string, exp *wrappedExporter) (*backendLogBatcher, error) {
	if b.stopped.Load() {
		return nil, errLogBatcherExporterStopping
	}

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
	if b.stopped.Load() {
		return nil, errLogBatcherExporterStopping
	}
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

	backend = newBackendLogBatcher(endpoint, exp, b.logger, b.settings, b.telemetry, b.send, b.drainErr)
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
	drainErr logBatcherDrainFailureFunc,
) *backendLogBatcher {
	backend := &backendLogBatcher{
		endpoint:  endpoint,
		exp:       exp,
		logger:    logger.With(zap.String("endpoint", endpoint)),
		settings:  settings,
		send:      send,
		drainErr:  drainErr,
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
		recordsAdded, oldestEnqueue := b.mergeQueuedRequests(pending, req, nextReq)
		*pendingRecords += recordsAdded
		if *pendingRecords > 0 && b.oldestEnqueue.Load() == 0 {
			b.oldestEnqueue.Store(oldestEnqueue)
		}
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
	}
	return false
}

func (b *backendLogBatcher) mergeQueuedRequests(pending *plog.Logs, first logBatcherRequest, nextReq **logBatcherRequest) (int, int64) {
	recordCount := 0
	var oldestEnqueue int64

	mergeRequest := func(req logBatcherRequest) {
		count := req.logs.LogRecordCount()
		if count == 0 {
			return
		}
		recordCount += count
		if oldestEnqueue == 0 || req.enqueuedAtUnixNano < oldestEnqueue {
			oldestEnqueue = req.enqueuedAtUnixNano
		}
		*pending = mergeLogs(*pending, req.logs)
	}

	mergeRequest(first)

	for i := 0; i < cap(b.requests); i++ {
		select {
		case req := <-b.requests:
			if req.kind != logBatcherRequestEnqueue {
				*nextReq = &req
				return recordCount, oldestEnqueue
			}
			mergeRequest(req)
		default:
			return recordCount, oldestEnqueue
		}
	}

	return recordCount, oldestEnqueue
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
	oldestAgeMillis := ageMillisFromUnixNano(time.Now(), b.oldestEnqueue.Load())
	*pending = plog.NewLogs()
	*pendingRecords = 0
	*pendingBytes = 0
	b.pendingRecords.Store(0)
	b.pendingBytes.Store(0)
	b.oldestEnqueue.Store(0)

	b.telemetry.batchSize.Record(ctx, int64(records), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	b.telemetry.batchBytes.Record(ctx, int64(bytes), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	b.telemetry.flushOldestRecordAge.Record(ctx, oldestAgeMillis, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint), attribute.String("reason", reason))))
	b.telemetry.flushTotal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint), attribute.String("reason", reason))))

	var rerouteLogs plog.Logs
	if b.drainErr != nil && reason != logFlushReasonShutdown {
		rerouteLogs = plog.NewLogs()
		drained.CopyTo(rerouteLogs)
	}
	err := b.send(ctx, b.exporter(), drained, reason)
	if err != nil {
		b.telemetry.flushErrors.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
		if b.drainErr != nil && reason != logFlushReasonShutdown {
			var rerouteable logBatcherRerouteableError
			if errors.As(err, &rerouteable) {
				rerouteErr := b.drainErr(ctx, rerouteLogs, reason)
				if rerouteErr == nil {
					return nil
				}
				err = errors.Join(err, rerouteErr)
			}
		}
		b.telemetry.droppedRecords.Add(ctx, int64(records), metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint))))
	}
	return err
}

type logBatcherPending struct {
	endpoint        string
	records         int64
	bytes           int64
	oldestAgeMillis int64
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
	t.flushOldestRecordAge, err = meter.Int64Histogram(
		"otelcol_loadbalancer_log_batch_flush_oldest_record_age",
		metric.WithDescription("Age in ms of the oldest log record in a flushed backend batch."),
		metric.WithUnit("ms"),
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
	t.pendingOldestAge, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_log_batch_pending_oldest_record_age",
		metric.WithDescription("Age in ms of the oldest pending log record per backend batch."),
		metric.WithUnit("ms"),
	)
	errs = errors.Join(errs, err)
	t.pendingOldestAgeMax, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_log_batch_pending_oldest_record_age_max",
		metric.WithDescription("Maximum age in ms of the oldest pending log record across backend batches."),
		metric.WithUnit("ms"),
	)
	errs = errors.Join(errs, err)

	return t, errs
}

func (t *logBatcherTelemetry) start(snapshot func() logBatcherSnapshot) error {
	reg, err := t.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		state := snapshot()
		for _, pending := range state.pending {
			attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", pending.endpoint)))
			observer.ObserveInt64(t.pendingRecords, pending.records, attrs)
			observer.ObserveInt64(t.pendingBytes, pending.bytes, attrs)
			observer.ObserveInt64(t.pendingOldestAge, pending.oldestAgeMillis, attrs)
		}
		observer.ObserveInt64(t.pendingOldestAgeMax, state.maxOldestAgeMillis)
		return nil
	}, t.pendingRecords, t.pendingBytes, t.pendingOldestAge, t.pendingOldestAgeMax)
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

func ageMillisFromUnixNano(now time.Time, unixNano int64) int64 {
	if unixNano <= 0 {
		return 0
	}

	age := now.Sub(time.Unix(0, unixNano)).Milliseconds()
	if age < 0 {
		return 0
	}
	return age
}
