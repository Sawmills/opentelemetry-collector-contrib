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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
	expmetrics "github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics"
)

const (
	metricFlushReasonSize           = "size"
	metricFlushReasonTimeout        = "timeout"
	metricFlushReasonShutdown       = "shutdown"
	metricFlushReasonResolverChange = "resolver_change"

	defaultMetricBatchMaxDataPoints = 8192
	defaultMetricBatchMaxBytes      = 2 << 20
	defaultMetricBatchFlushTimeout  = 200 * time.Millisecond
)

var errMetricBatcherExporterStopping = errors.New("metric batcher exporter is stopping")

type metricBatcherSettings struct {
	maxDataPoints int
	maxBytes      int
	flushInterval time.Duration
}

type metricBatcherSendFunc func(context.Context, *wrappedExporter, pmetric.Metrics, string) error

type metricBatcher struct {
	logger   *zap.Logger
	settings metricBatcherSettings
	send     metricBatcherSendFunc

	telemetry *metricBatcherTelemetry

	mu       sync.RWMutex
	backends map[string]*backendMetricBatcher
	stopped  atomic.Bool
}

type metricBatcherRequest struct {
	kind   metricBatcherRequestKind
	md     pmetric.Metrics
	ctx    context.Context
	reason string
	done   chan error
}

type metricBatcherRequestKind int

const (
	metricBatcherRequestEnqueue metricBatcherRequestKind = iota
	metricBatcherRequestFlushAndStop
)

type backendMetricBatcher struct {
	endpoint string
	logger   *zap.Logger
	settings metricBatcherSettings
	send     metricBatcherSendFunc

	telemetry *metricBatcherTelemetry

	exporterMu sync.RWMutex
	exp        *wrappedExporter

	requests chan metricBatcherRequest
	done     chan struct{}
	inflight sync.WaitGroup

	pendingDataPoints atomic.Int64
	pendingBytes      atomic.Int64
}

type metricBatcherTelemetry struct {
	logger            *zap.Logger
	meter             metric.Meter
	batchDataPoints   metric.Int64Histogram
	batchBytes        metric.Int64Histogram
	flushTotal        metric.Int64Counter
	flushErrors       metric.Int64Counter
	droppedDataPoints metric.Int64Counter
	overflowTotal     metric.Int64Counter
	pendingDataPoints metric.Int64ObservableGauge
	pendingBytes      metric.Int64ObservableGauge

	mu            sync.Mutex
	registrations []metric.Registration
}

func newMetricBatcher(
	logger *zap.Logger,
	settings component.TelemetrySettings,
	cfg metricBatcherSettings,
	send metricBatcherSendFunc,
) (*metricBatcher, error) {
	telemetry, err := newMetricBatcherTelemetry(settings)
	if err != nil {
		return nil, err
	}

	b := &metricBatcher{
		logger:    logger,
		settings:  cfg,
		send:      send,
		telemetry: telemetry,
		backends:  make(map[string]*backendMetricBatcher),
	}

	if err := telemetry.start(b.snapshotPending); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *metricBatcher) TryEnqueue(endpoint string, exp *wrappedExporter, md pmetric.Metrics) (bool, error) {
	backend, err := b.acquireBackend(endpoint, exp)
	if err != nil {
		return false, err
	}
	defer backend.inflight.Done()

	select {
	case <-backend.done:
		return false, errors.New("metric batcher backend is stopped")
	default:
	}

	select {
	case backend.requests <- metricBatcherRequest{kind: metricBatcherRequestEnqueue, md: md}:
		return true, nil
	case <-backend.done:
		return false, errors.New("metric batcher backend is stopped")
	default:
		return false, nil
	}
}

func (b *metricBatcher) Remove(ctx context.Context, endpoint string, exp *wrappedExporter) error {
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
		b.scheduleBackendCleanup(backend, metricFlushReasonResolverChange)
		return err
	}
	return backend.stopAndFlush(ctx, metricFlushReasonResolverChange)
}

func (b *metricBatcher) Shutdown(ctx context.Context) error {
	b.stopped.Store(true)

	b.mu.Lock()
	backends := make([]*backendMetricBatcher, 0, len(b.backends))
	for endpoint, backend := range b.backends {
		backends = append(backends, backend)
		delete(b.backends, endpoint)
	}
	b.mu.Unlock()

	var errs error
	for _, backend := range backends {
		if err := waitForInflight(ctx, &backend.inflight); err != nil {
			b.scheduleBackendCleanup(backend, metricFlushReasonShutdown)
			errs = errors.Join(errs, err)
			continue
		}
		errs = errors.Join(errs, backend.stopAndFlush(ctx, metricFlushReasonShutdown))
	}
	b.telemetry.shutdown()
	return errs
}

func (b *metricBatcher) scheduleBackendCleanup(backend *backendMetricBatcher, reason string) {
	go func() {
		if err := waitForInflight(context.Background(), &backend.inflight); err != nil {
			b.logger.Warn("failed waiting for inflight metric batcher requests during background cleanup", zap.String("endpoint", backend.endpoint), zap.Error(err))
			return
		}
		if err := backend.stopAndFlush(context.Background(), reason); err != nil {
			b.logger.Warn("failed to stop metric batcher backend during background cleanup", zap.String("endpoint", backend.endpoint), zap.String("reason", reason), zap.Error(err))
		}
	}()
}

type metricBatcherPending struct {
	endpoint   string
	datapoints int64
	bytes      int64
}

func (b *metricBatcher) snapshotPending() []metricBatcherPending {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pending := make([]metricBatcherPending, 0, len(b.backends))
	for endpoint, backend := range b.backends {
		pending = append(pending, metricBatcherPending{
			endpoint:   endpoint,
			datapoints: backend.pendingDataPoints.Load(),
			bytes:      backend.pendingBytes.Load(),
		})
	}
	return pending
}

func (b *metricBatcher) acquireBackend(endpoint string, exp *wrappedExporter) (*backendMetricBatcher, error) {
	if b.stopped.Load() {
		return nil, errMetricBatcherExporterStopping
	}

	b.mu.RLock()
	backend, ok := b.backends[endpoint]
	if ok {
		if exp != nil && exp.isStopping() {
			b.mu.RUnlock()
			return nil, errMetricBatcherExporterStopping
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
		return nil, errMetricBatcherExporterStopping
	}
	backend, ok = b.backends[endpoint]
	if ok {
		if exp != nil && exp.isStopping() {
			return nil, errMetricBatcherExporterStopping
		}
		backend.setExporter(exp)
		backend.inflight.Add(1)
		return backend, nil
	}
	if exp != nil && exp.isStopping() {
		return nil, errMetricBatcherExporterStopping
	}

	backend = newBackendMetricBatcher(endpoint, exp, b.logger, b.settings, b.telemetry, b.send)
	backend.inflight.Add(1)
	b.backends[endpoint] = backend
	return backend, nil
}

func newBackendMetricBatcher(
	endpoint string,
	exp *wrappedExporter,
	logger *zap.Logger,
	settings metricBatcherSettings,
	telemetry *metricBatcherTelemetry,
	send metricBatcherSendFunc,
) *backendMetricBatcher {
	backend := &backendMetricBatcher{
		endpoint:  endpoint,
		exp:       exp,
		logger:    logger.With(zap.String("endpoint", endpoint)),
		settings:  settings,
		telemetry: telemetry,
		send:      send,
		requests:  make(chan metricBatcherRequest, 16),
		done:      make(chan struct{}),
	}

	go backend.run()
	return backend
}

func (b *backendMetricBatcher) stopAndFlush(ctx context.Context, reason string) error {
	done := make(chan error, 1)
	select {
	case b.requests <- metricBatcherRequest{kind: metricBatcherRequestFlushAndStop, ctx: ctx, reason: reason, done: done}:
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

func (b *backendMetricBatcher) setExporter(exp *wrappedExporter) {
	b.exporterMu.Lock()
	b.exp = exp
	b.exporterMu.Unlock()
}

func (b *backendMetricBatcher) exporter() *wrappedExporter {
	b.exporterMu.RLock()
	defer b.exporterMu.RUnlock()
	return b.exp
}

func (b *backendMetricBatcher) run() {
	defer close(b.done)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	var timerC <-chan time.Time

	pending := pmetric.NewMetrics()
	pendingBytes := 0
	pendingDataPoints := 0
	sizer := &pmetric.ProtoMarshaler{}
	var nextReq *metricBatcherRequest

	for {
		if nextReq != nil {
			req := *nextReq
			nextReq = nil
			if b.handleRequest(req, sizer, &pending, &pendingDataPoints, &pendingBytes, &nextReq, timer, &timerC) {
				return
			}
			continue
		}

		select {
		case req := <-b.requests:
			if b.handleRequest(req, sizer, &pending, &pendingDataPoints, &pendingBytes, &nextReq, timer, &timerC) {
				return
			}
		case <-timerC:
			if err := b.flush(context.Background(), &pending, &pendingDataPoints, &pendingBytes, metricFlushReasonTimeout, timer, &timerC); err != nil {
				b.logger.Warn("failed to flush metric batch", zap.String("reason", metricFlushReasonTimeout), zap.Error(err))
			}
		}
	}
}

func (b *backendMetricBatcher) handleRequest(
	req metricBatcherRequest,
	sizer *pmetric.ProtoMarshaler,
	pending *pmetric.Metrics,
	pendingDataPoints *int,
	pendingBytes *int,
	nextReq **metricBatcherRequest,
	timer *time.Timer,
	timerC *<-chan time.Time,
) bool {
	switch req.kind {
	case metricBatcherRequestEnqueue:
		*pendingDataPoints += b.mergeQueuedRequests(pending, req, nextReq)
		*pendingBytes = sizer.MetricsSize(*pending)
		b.pendingDataPoints.Store(int64(*pendingDataPoints))
		b.pendingBytes.Store(int64(*pendingBytes))
		if *pendingDataPoints > 0 && *timerC == nil {
			timer.Reset(b.settings.flushInterval)
			*timerC = timer.C
		}
		if *pendingDataPoints >= b.settings.maxDataPoints || *pendingBytes >= b.settings.maxBytes {
			attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint)))
			b.telemetry.overflowTotal.Add(context.Background(), 1, attrs)
			if err := b.flush(context.Background(), pending, pendingDataPoints, pendingBytes, metricFlushReasonSize, timer, timerC); err != nil {
				b.logger.Warn("failed to flush metric batch", zap.String("reason", metricFlushReasonSize), zap.Error(err))
			}
		}
	case metricBatcherRequestFlushAndStop:
		err := b.flush(req.ctx, pending, pendingDataPoints, pendingBytes, req.reason, timer, timerC)
		req.done <- err
		return true
	}
	return false
}

func (b *backendMetricBatcher) mergeQueuedRequests(
	pending *pmetric.Metrics,
	first metricBatcherRequest,
	nextReq **metricBatcherRequest,
) int {
	dataPointCount := first.md.DataPointCount()
	expmetrics.Merge(*pending, first.md)

	for i := 0; i < cap(b.requests); i++ {
		select {
		case req := <-b.requests:
			if req.kind != metricBatcherRequestEnqueue {
				*nextReq = &req
				return dataPointCount
			}
			dataPointCount += req.md.DataPointCount()
			expmetrics.Merge(*pending, req.md)
		default:
			return dataPointCount
		}
	}

	return dataPointCount
}

func (b *backendMetricBatcher) flush(
	ctx context.Context,
	pending *pmetric.Metrics,
	pendingDataPoints *int,
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

	if pending.DataPointCount() == 0 {
		return nil
	}

	drained := *pending
	datapoints := *pendingDataPoints
	bytes := *pendingBytes
	*pending = pmetric.NewMetrics()
	*pendingDataPoints = 0
	*pendingBytes = 0
	b.pendingDataPoints.Store(0)
	b.pendingBytes.Store(0)

	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint)))
	b.telemetry.batchDataPoints.Record(ctx, int64(datapoints), attrs)
	b.telemetry.batchBytes.Record(ctx, int64(bytes), attrs)
	b.telemetry.flushTotal.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", b.endpoint), attribute.String("reason", reason))))

	err := b.send(ctx, b.exporter(), drained, reason)
	if err != nil {
		b.telemetry.flushErrors.Add(ctx, 1, attrs)
		if reason == metricFlushReasonSize || reason == metricFlushReasonTimeout {
			*pending = drained
			*pendingDataPoints = datapoints
			*pendingBytes = bytes
			b.pendingDataPoints.Store(int64(datapoints))
			b.pendingBytes.Store(int64(bytes))
			if *timerC == nil {
				timer.Reset(b.settings.flushInterval)
				*timerC = timer.C
			}
		} else {
			b.telemetry.droppedDataPoints.Add(ctx, int64(datapoints), attrs)
		}
	}

	return err
}

func newMetricBatcherTelemetry(settings component.TelemetrySettings) (*metricBatcherTelemetry, error) {
	meter := metadata.Meter(settings)
	var err, errs error

	t := &metricBatcherTelemetry{logger: settings.Logger, meter: meter}
	t.batchDataPoints, err = meter.Int64Histogram(
		"otelcol_loadbalancer_metric_batch_size",
		metric.WithDescription("Number of metric datapoints per flushed backend batch."),
		metric.WithUnit("{datapoints}"),
	)
	errs = errors.Join(errs, err)
	t.batchBytes, err = meter.Int64Histogram(
		"otelcol_loadbalancer_metric_batch_bytes",
		metric.WithDescription("Serialized OTLP bytes per flushed metric backend batch before compression."),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)
	t.flushTotal, err = meter.Int64Counter(
		"otelcol_loadbalancer_metric_batch_flush_total",
		metric.WithDescription("Number of metric batch flushes by endpoint and reason."),
		metric.WithUnit("{flushes}"),
	)
	errs = errors.Join(errs, err)
	t.flushErrors, err = meter.Int64Counter(
		"otelcol_loadbalancer_metric_batch_flush_errors",
		metric.WithDescription("Number of metric batch flush errors."),
		metric.WithUnit("{errors}"),
	)
	errs = errors.Join(errs, err)
	t.droppedDataPoints, err = meter.Int64Counter(
		"otelcol_loadbalancer_metric_batch_dropped_datapoints",
		metric.WithDescription("Number of dropped metric datapoints in the internal metric batcher."),
		metric.WithUnit("{datapoints}"),
	)
	errs = errors.Join(errs, err)
	t.overflowTotal, err = meter.Int64Counter(
		"otelcol_loadbalancer_metric_batch_overflow_total",
		metric.WithDescription("Number of times an internal metric batch hit a size bound and was force-flushed."),
		metric.WithUnit("{overflows}"),
	)
	errs = errors.Join(errs, err)
	t.pendingDataPoints, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_metric_batch_pending_datapoints",
		metric.WithDescription("Current number of pending metric datapoints per backend batch."),
		metric.WithUnit("{datapoints}"),
	)
	errs = errors.Join(errs, err)
	t.pendingBytes, err = meter.Int64ObservableGauge(
		"otelcol_loadbalancer_metric_batch_pending_bytes",
		metric.WithDescription("Current serialized OTLP bytes per pending metric backend batch before compression."),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)

	return t, errs
}

func (t *metricBatcherTelemetry) start(snapshot func() []metricBatcherPending) error {
	reg, err := t.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		for _, pending := range snapshot() {
			attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", pending.endpoint)))
			observer.ObserveInt64(t.pendingDataPoints, pending.datapoints, attrs)
			observer.ObserveInt64(t.pendingBytes, pending.bytes, attrs)
		}
		return nil
	}, t.pendingDataPoints, t.pendingBytes)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.registrations = append(t.registrations, reg)
	t.mu.Unlock()
	return nil
}

func (t *metricBatcherTelemetry) shutdown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, reg := range t.registrations {
		if err := reg.Unregister(); err != nil {
			t.logger.Warn("failed to unregister metric batcher metric callback", zap.Error(err))
		}
	}
	t.registrations = nil
}
