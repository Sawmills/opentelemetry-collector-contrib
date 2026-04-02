// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric"
	conventions "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
)

var _ exporter.Metrics = (*metricExporterImp)(nil)

const metricBatcherEnqueueBackoff = 2 * time.Millisecond

type metricExporterImp struct {
	loadBalancer *loadBalancer
	batcher      *metricBatcher
	routingKey   routingKey

	logger    *zap.Logger
	started   atomic.Bool
	telemetry *metadata.TelemetryBuilder
}

func newMetricsExporter(params exporter.Settings, cfg component.Config) (*metricExporterImp, error) {
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	exporterFactory := otlpexporter.NewFactory()
	cfFunc := func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := buildExporterConfig(cfg.(*Config), endpoint)
		oParams := buildExporterSettings(exporterFactory.Type(), params, endpoint)

		return exporterFactory.CreateMetrics(ctx, oParams, &oCfg)
	}

	lb, err := newLoadBalancer(params.Logger, cfg, cfFunc, telemetry)
	if err != nil {
		return nil, err
	}

	metricExporter := metricExporterImp{
		loadBalancer: lb,
		routingKey:   svcRouting,
		telemetry:    telemetry,
		logger:       params.Logger,
	}

	switch cfg.(*Config).RoutingKey {
	case svcRoutingStr, "":
		// default case for empty routing key
		metricExporter.routingKey = svcRouting
	case resourceRoutingStr:
		metricExporter.routingKey = resourceRouting
	case metricNameRoutingStr:
		metricExporter.routingKey = metricNameRouting
	case streamIDRoutingStr:
		metricExporter.routingKey = streamIDRouting
	default:
		return nil, fmt.Errorf("unsupported routing_key: %q", cfg.(*Config).RoutingKey)
	}

	if cfg.(*Config).MetricBatcher.Enabled {
		metricExporter.batcher, err = newMetricBatcher(
			params.Logger,
			params.TelemetrySettings,
			metricBatcherSettings{
				maxDataPoints: cfg.(*Config).MetricBatcher.MaxDataPoints,
				maxBytes:      cfg.(*Config).MetricBatcher.MaxBytes,
				flushInterval: cfg.(*Config).MetricBatcher.FlushInterval,
			},
			metricExporter.consumeBatch,
		)
		if err != nil {
			return nil, err
		}
		lb.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
			return metricExporter.batcher.Remove(ctx, endpoint, exp)
		}
	}

	return &metricExporter, nil
}

func (*metricExporterImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *metricExporterImp) Start(ctx context.Context, host component.Host) error {
	if err := e.loadBalancer.Start(ctx, host); err != nil {
		return err
	}
	e.started.Store(true)
	return nil
}

func (e *metricExporterImp) Shutdown(ctx context.Context) error {
	if !e.started.Swap(false) {
		return nil
	}
	var err error
	if e.batcher != nil {
		err = e.batcher.Shutdown(ctx)
	}
	err = errors.Join(err, e.loadBalancer.Shutdown(ctx))
	return err
}

func (e *metricExporterImp) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if !e.started.Load() {
		return errExporterIsStopping
	}

	batches, err := e.splitMetricsByRouting(md)
	if err != nil {
		return err
	}

	if e.batcher != nil {
		endpointBatches, err := e.groupRoutedMetricsByEndpoint(batches)
		if err != nil {
			return err
		}
		return e.enqueueEndpointBatches(ctx, endpointBatches, true)
	}

	return e.consumeMetricsByExporter(ctx, batches)
}

type endpointMetricsBatch struct {
	metrics pmetric.Metrics
	exp     *wrappedExporter
}

func (e *metricExporterImp) splitMetricsByRouting(md pmetric.Metrics) (map[string]pmetric.Metrics, error) {
	var batches map[string]pmetric.Metrics

	switch e.routingKey {
	case svcRouting:
		var errs []error
		batches, errs = splitMetricsByResourceServiceName(md)
		if len(errs) > 0 {
			for _, ee := range errs {
				e.logger.Error("failed to export metric", zap.Error(ee))
			}
			if len(batches) == 0 {
				return nil, consumererror.NewPermanent(errors.Join(errs...))
			}
		}
	case resourceRouting:
		batches = splitMetricsByResourceID(md)
	case metricNameRouting:
		batches = splitMetricsByMetricName(md)
	case streamIDRouting:
		batches = splitMetricsByStreamID(md)
	}

	return batches, nil
}

func (e *metricExporterImp) groupRoutedMetricsByEndpoint(
	batches map[string]pmetric.Metrics,
) (map[string]*endpointMetricsBatch, error) {
	endpointBatches := make(map[string]*endpointMetricsBatch)

	for routingID, mds := range batches {
		exp, endpoint, err := e.loadBalancer.exporterAndEndpoint([]byte(routingID))
		if err != nil {
			return nil, err
		}

		ep := endpointWithPort(endpoint)
		batch, ok := endpointBatches[ep]
		if !ok {
			batch = &endpointMetricsBatch{metrics: pmetric.NewMetrics(), exp: exp}
			endpointBatches[ep] = batch
		}
		batch.exp = exp
		metrics.Merge(batch.metrics, mds)
	}

	return endpointBatches, nil
}

func (e *metricExporterImp) enqueueEndpointBatches(
	ctx context.Context,
	batches map[string]*endpointMetricsBatch,
	retryOnStopping bool,
) error {
	var errs error
	failed := pmetric.NewMetrics()
	if len(batches) == 0 {
		return nil
	}

	pending := make(map[string]*endpointMetricsBatch, len(batches))
	for ep, batch := range batches {
		pending[ep] = batch
	}

	for len(pending) > 0 {
		madeProgress := false

		for ep, batch := range pending {
			// TryEnqueue is non-blocking; false,nil means queue is currently full.
			enqueued, err := e.batcher.TryEnqueue(ep, batch.exp, batch.metrics)

			if errors.Is(err, errMetricBatcherExporterStopping) && retryOnStopping {
				// Reroute once when endpoint exporter is stopping.
				rerouted, rerouteErr := e.splitMetricsByRouting(batch.metrics)
				errs = multierr.Append(errs, rerouteErr)
				if rerouteErr != nil {
					metrics.Merge(failed, batch.metrics)
				} else {
					reroutedBatches, groupErr := e.groupRoutedMetricsByEndpoint(rerouted)
					errs = multierr.Append(errs, groupErr)
					if groupErr != nil {
						metrics.Merge(failed, batch.metrics)
					} else {
						rerouteErr = e.enqueueEndpointBatches(ctx, reroutedBatches, false)
						if rerouteErr != nil {
							var mErr consumererror.Metrics
							if errors.As(rerouteErr, &mErr) {
								metrics.Merge(failed, mErr.Data())
								rerouteErr = errors.Unwrap(mErr)
							}
							errs = multierr.Append(errs, rerouteErr)
						}
					}
				}
				delete(pending, ep)
				continue
			}
			if err != nil {
				errs = multierr.Append(errs, err)
				metrics.Merge(failed, batch.metrics)
				delete(pending, ep)
				continue
			}
			if enqueued {
				madeProgress = true
				delete(pending, ep)
			}
		}

		if len(pending) == 0 {
			break
		}

		if madeProgress {
			continue
		}

		// Back off when all pending endpoint queues are currently full.
		timer := time.NewTimer(metricBatcherEnqueueBackoff)
		select {
		case <-ctx.Done():
			// If context was canceled or timed out we send out failed & pending metrics for a retry
			for _, batch := range pending {
				metrics.Merge(failed, batch.metrics)
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			errs = multierr.Append(errs, ctx.Err())
			if failed.DataPointCount() > 0 {
				return consumererror.NewMetrics(errs, failed)
			}
			return errs
		case <-timer.C:
		}
	}

	if failed.DataPointCount() > 0 {
		return consumererror.NewMetrics(errs, failed)
	}

	return errs
}

func (e *metricExporterImp) consumeMetricsByExporter(
	ctx context.Context,
	batches map[string]pmetric.Metrics,
) error {

	// Now assign each batch to an exporter, and merge as we go
	metricsByExporter := map[*wrappedExporter]pmetric.Metrics{}
	needsCleanup := true
	defer func() {
		if !needsCleanup {
			return
		}
		for exp := range metricsByExporter {
			exp.doneConsume()
		}
	}()

	for routingID, mds := range batches {
		exp, _, err := e.loadBalancer.exporterAndEndpoint([]byte(routingID))
		if err != nil {
			return err
		}

		expMetrics, ok := metricsByExporter[exp]
		if !ok {
			if !exp.tryStartConsume() {
				return errExporterIsStopping
			}
			expMetrics = pmetric.NewMetrics()
			metricsByExporter[exp] = expMetrics
		}

		metrics.Merge(expMetrics, mds)
	}

	needsCleanup = false
	var (
		errMu sync.Mutex
		errs  error
		wg    sync.WaitGroup
	)
	for exp, mds := range metricsByExporter {
		wg.Add(1)
		go func(exp *wrappedExporter, mds pmetric.Metrics) {
			defer wg.Done()
			start := time.Now()
			err := exp.ConsumeMetrics(ctx, mds)
			duration := time.Since(start)

			exp.doneConsume()
			errMu.Lock()
			errs = multierr.Append(errs, err)
			errMu.Unlock()
			e.recordBackendResult(ctx, exp, duration, err)
		}(exp, mds)
	}
	wg.Wait()

	return errs
}

func (e *metricExporterImp) consumeBatch(ctx context.Context, we *wrappedExporter, md pmetric.Metrics, reason string) error {
	if reason == metricFlushReasonResolverChange || reason == metricFlushReasonShutdown {
		we.forceStartConsume()
	} else if !we.tryStartConsume() {
		return errMetricBatcherExporterStopping
	}
	defer we.doneConsume()

	start := time.Now()
	err := we.ConsumeMetrics(ctx, md)
	duration := time.Since(start)
	e.recordBackendResult(ctx, we, duration, err)

	return err
}

func (e *metricExporterImp) recordBackendResult(ctx context.Context, we *wrappedExporter, duration time.Duration, err error) {
	e.telemetry.LoadbalancerBackendLatency.Record(ctx, duration.Milliseconds(), metric.WithAttributeSet(we.endpointAttr))
	if err == nil {
		e.telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(we.successAttr))
		return
	}

	e.telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(we.failureAttr))
	e.logger.Debug("failed to export metrics", zap.Error(err))
}

func splitMetricsByResourceServiceName(md pmetric.Metrics) (map[string]pmetric.Metrics, []error) {
	results := map[string]pmetric.Metrics{}
	var errs []error

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)

		svc, ok := rm.Resource().Attributes().Get(string(conventions.ServiceNameKey))
		if !ok {
			errs = append(errs, fmt.Errorf("unable to get service name from resource metric with attributes: %v", rm.Resource().Attributes().AsRaw()))
			continue
		}

		newMD := pmetric.NewMetrics()
		rmClone := newMD.ResourceMetrics().AppendEmpty()
		rm.CopyTo(rmClone)

		key := svc.Str()
		existing, ok := results[key]
		if ok {
			metrics.Merge(existing, newMD)
		} else {
			results[key] = newMD
		}
	}

	return results, errs
}

func splitMetricsByResourceID(md pmetric.Metrics) map[string]pmetric.Metrics {
	results := map[string]pmetric.Metrics{}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)

		newMD := pmetric.NewMetrics()
		rmClone := newMD.ResourceMetrics().AppendEmpty()
		rm.CopyTo(rmClone)

		key := identity.OfResource(rm.Resource()).String()
		existing, ok := results[key]
		if ok {
			metrics.Merge(existing, newMD)
		} else {
			results[key] = newMD
		}
	}

	return results
}

func splitMetricsByMetricName(md pmetric.Metrics) map[string]pmetric.Metrics {
	results := map[string]pmetric.Metrics{}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)

				newMD, mClone := cloneMetricWithoutType(rm, sm, m)
				m.CopyTo(mClone)

				key := m.Name()
				existing, ok := results[key]
				if ok {
					metrics.Merge(existing, newMD)
				} else {
					results[key] = newMD
				}
			}
		}
	}

	return results
}

func splitMetricsByStreamID(md pmetric.Metrics) map[string]pmetric.Metrics {
	results := map[string]pmetric.Metrics{}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		res := rm.Resource()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			scope := sm.Scope()

			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				metricID := identity.OfResourceMetric(res, scope, m)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					gauge := m.Gauge()

					for l := 0; l < gauge.DataPoints().Len(); l++ {
						dp := gauge.DataPoints().At(l)

						newMD, mClone := cloneMetricWithoutType(rm, sm, m)
						gaugeClone := mClone.SetEmptyGauge()

						dpClone := gaugeClone.DataPoints().AppendEmpty()
						dp.CopyTo(dpClone)

						key := identity.OfStream(metricID, dp).String()
						existing, ok := results[key]
						if ok {
							metrics.Merge(existing, newMD)
						} else {
							results[key] = newMD
						}
					}
				case pmetric.MetricTypeSum:
					sum := m.Sum()

					for l := 0; l < sum.DataPoints().Len(); l++ {
						dp := sum.DataPoints().At(l)

						newMD, mClone := cloneMetricWithoutType(rm, sm, m)
						sumClone := mClone.SetEmptySum()
						sumClone.SetIsMonotonic(sum.IsMonotonic())
						sumClone.SetAggregationTemporality(sum.AggregationTemporality())

						dpClone := sumClone.DataPoints().AppendEmpty()
						dp.CopyTo(dpClone)

						key := identity.OfStream(metricID, dp).String()
						existing, ok := results[key]
						if ok {
							metrics.Merge(existing, newMD)
						} else {
							results[key] = newMD
						}
					}
				case pmetric.MetricTypeHistogram:
					histogram := m.Histogram()

					for l := 0; l < histogram.DataPoints().Len(); l++ {
						dp := histogram.DataPoints().At(l)

						newMD, mClone := cloneMetricWithoutType(rm, sm, m)
						histogramClone := mClone.SetEmptyHistogram()
						histogramClone.SetAggregationTemporality(histogram.AggregationTemporality())

						dpClone := histogramClone.DataPoints().AppendEmpty()
						dp.CopyTo(dpClone)

						key := identity.OfStream(metricID, dp).String()
						existing, ok := results[key]
						if ok {
							metrics.Merge(existing, newMD)
						} else {
							results[key] = newMD
						}
					}
				case pmetric.MetricTypeExponentialHistogram:
					expHistogram := m.ExponentialHistogram()

					for l := 0; l < expHistogram.DataPoints().Len(); l++ {
						dp := expHistogram.DataPoints().At(l)

						newMD, mClone := cloneMetricWithoutType(rm, sm, m)
						expHistogramClone := mClone.SetEmptyExponentialHistogram()
						expHistogramClone.SetAggregationTemporality(expHistogram.AggregationTemporality())

						dpClone := expHistogramClone.DataPoints().AppendEmpty()
						dp.CopyTo(dpClone)

						key := identity.OfStream(metricID, dp).String()
						existing, ok := results[key]
						if ok {
							metrics.Merge(existing, newMD)
						} else {
							results[key] = newMD
						}
					}
				case pmetric.MetricTypeSummary:
					summary := m.Summary()

					for l := 0; l < summary.DataPoints().Len(); l++ {
						dp := summary.DataPoints().At(l)

						newMD, mClone := cloneMetricWithoutType(rm, sm, m)
						sumClone := mClone.SetEmptySummary()

						dpClone := sumClone.DataPoints().AppendEmpty()
						dp.CopyTo(dpClone)

						key := identity.OfStream(metricID, dp).String()
						existing, ok := results[key]
						if ok {
							metrics.Merge(existing, newMD)
						} else {
							results[key] = newMD
						}
					}
				}
			}
		}
	}

	return results
}

func cloneMetricWithoutType(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) (md pmetric.Metrics, mClone pmetric.Metric) {
	md = pmetric.NewMetrics()

	rmClone := md.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(rmClone.Resource())
	rmClone.SetSchemaUrl(rm.SchemaUrl())

	smClone := rmClone.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(smClone.Scope())
	smClone.SetSchemaUrl(sm.SchemaUrl())

	mClone = smClone.Metrics().AppendEmpty()
	mClone.SetName(m.Name())
	mClone.SetDescription(m.Description())
	mClone.SetUnit(m.Unit())

	return md, mClone
}
