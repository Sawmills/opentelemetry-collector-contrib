// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
)

var _ exporter.Logs = (*logExporterImp)(nil)

type logExporterImp struct {
	loadBalancer *loadBalancer
	batcher      *logBatcher

	logger    *zap.Logger
	started   bool
	telemetry *metadata.TelemetryBuilder
}

// Create new logs exporter
func newLogsExporter(params exporter.Settings, cfg component.Config) (*logExporterImp, error) {
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	exporterFactory := otlpexporter.NewFactory()
	cfFunc := func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := buildExporterConfig(cfg.(*Config), endpoint)
		oParams := buildExporterSettings(exporterFactory.Type(), params, endpoint)

		return exporterFactory.CreateLogs(ctx, oParams, &oCfg)
	}

	lb, err := newLoadBalancer(params.Logger, cfg, cfFunc, telemetry)
	if err != nil {
		return nil, err
	}

	logExporter := &logExporterImp{
		loadBalancer: lb,
		telemetry:    telemetry,
		logger:       params.Logger,
	}
	if cfg.(*Config).LogBatcher.Enabled {
		logExporter.batcher, err = newLogBatcher(
			params.Logger,
			params.TelemetrySettings,
			logBatcherSettings{
				maxRecords:    cfg.(*Config).LogBatcher.MaxRecords,
				maxBytes:      cfg.(*Config).LogBatcher.MaxBytes,
				flushInterval: cfg.(*Config).LogBatcher.FlushInterval,
			},
			logExporter.consumeBatch,
		)
		if err != nil {
			return nil, err
		}
		lb.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
			return logExporter.batcher.Remove(ctx, endpoint, exp)
		}
	}

	return logExporter, nil
}

func (*logExporterImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *logExporterImp) Start(ctx context.Context, host component.Host) error {
	e.started = true
	return e.loadBalancer.Start(ctx, host)
}

func (e *logExporterImp) Shutdown(ctx context.Context) error {
	if !e.started {
		return nil
	}
	var err error
	if e.batcher != nil {
		err = e.batcher.Shutdown(ctx)
	}
	err = errors.Join(err, e.loadBalancer.Shutdown(ctx))
	e.started = false
	return err
}

func (e *logExporterImp) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if e.batcher == nil {
		var errs error
		batches := batchpersignal.SplitLogs(ld)
		for _, batch := range batches {
			errs = multierr.Append(errs, e.consumeLog(ctx, batch))
		}
		return errs
	}

	return e.consumeLogsBatched(ctx, ld)
}

// endpointBatch holds logs grouped for a single backend endpoint,
// with resource/scope structures properly deduplicated.
type endpointBatch struct {
	logs plog.Logs
	exp  *wrappedExporter
}

// consumeLogsBatched routes each log record to its target endpoint via the
// load balancer, grouping records into per-endpoint plog.Logs that preserve
// the original resource/scope hierarchy. This avoids the N×resource/scope
// duplication that per-record wrapping would cause in the batcher.
func (e *logExporterImp) consumeLogsBatched(ctx context.Context, ld plog.Logs) error {
	batches := make(map[string]*endpointBatch)
	var errs error

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				rec := sl.LogRecords().At(k)

				traceID := rec.TraceID()
				balancingKey := traceID
				if traceID == pcommon.NewTraceIDEmpty() {
					balancingKey = random()
				}

				le, endpoint, err := e.loadBalancer.exporterAndEndpoint(balancingKey[:])
				if err != nil {
					errs = multierr.Append(errs, err)
					continue
				}
				ep := endpointWithPort(endpoint)
				batch, ok := batches[ep]
				if !ok {
					batch = &endpointBatch{logs: plog.NewLogs(), exp: le}
					batches[ep] = batch
				}
				batch.exp = le
				insertLogRecord(batch.logs, rl, sl, rec)
			}
		}
	}

	for ep, batch := range batches {
		var err error
		for range 2 {
			err = e.batcher.Enqueue(ctx, ep, batch.exp, batch.logs)
			if !errors.Is(err, errLogBatcherExporterStopping) {
				if err != nil {
					errs = multierr.Append(errs, err)
				}
				break
			}
		}
		if err != nil {
			errs = multierr.Append(errs, err)
		}
	}

	return errs
}

func (e *logExporterImp) consumeLog(ctx context.Context, ld plog.Logs) error {
	traceID := traceIDFromLogs(ld)
	balancingKey := traceID
	if traceID == pcommon.NewTraceIDEmpty() {
		balancingKey = random()
	}

	le, _, err := e.loadBalancer.exporterAndEndpoint(balancingKey[:])
	if err != nil {
		return err
	}

	return e.consumeBatch(ctx, le, ld, logFlushReasonDirect)
}

func (e *logExporterImp) consumeBatch(ctx context.Context, le *wrappedExporter, ld plog.Logs, reason string) error {
	if reason == logFlushReasonDirect || reason == logFlushReasonResolverChange || reason == logFlushReasonShutdown {
		le.forceStartConsume()
	} else if !le.tryStartConsume() {
		return errLogBatcherExporterStopping
	}
	defer le.doneConsume()

	start := time.Now()
	err := le.ConsumeLogs(ctx, ld)
	duration := time.Since(start)
	e.telemetry.LoadbalancerBackendLatency.Record(ctx, duration.Milliseconds(), metric.WithAttributeSet(le.endpointAttr))
	if err == nil {
		e.telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(le.successAttr))
	} else {
		e.telemetry.LoadbalancerBackendOutcome.Add(ctx, 1, metric.WithAttributeSet(le.failureAttr))
		e.logger.Debug("failed to export log", zap.Error(err))
	}

	return err
}

// insertLogRecord adds a log record into the destination plog.Logs, reusing
// existing ResourceLogs/ScopeLogs entries when their attributes match. This
// avoids creating duplicate resource/scope hierarchies for records that share
// the same origin.
func insertLogRecord(dest plog.Logs, srcRL plog.ResourceLogs, srcSL plog.ScopeLogs, rec plog.LogRecord) {
	targetRL := findOrCreateResourceLogs(dest, srcRL)
	targetSL := findOrCreateScopeLogs(targetRL, srcSL)
	rec.CopyTo(targetSL.LogRecords().AppendEmpty())
}

func findOrCreateResourceLogs(dest plog.Logs, src plog.ResourceLogs) plog.ResourceLogs {
	srcRes := src.Resource()
	srcSchema := src.SchemaUrl()
	for i := 0; i < dest.ResourceLogs().Len(); i++ {
		rl := dest.ResourceLogs().At(i)
		if rl.SchemaUrl() == srcSchema && rl.Resource().Attributes().Equal(srcRes.Attributes()) {
			return rl
		}
	}
	rl := dest.ResourceLogs().AppendEmpty()
	srcRes.CopyTo(rl.Resource())
	rl.SetSchemaUrl(srcSchema)
	return rl
}

func findOrCreateScopeLogs(rl plog.ResourceLogs, src plog.ScopeLogs) plog.ScopeLogs {
	srcScope := src.Scope()
	srcSchema := src.SchemaUrl()
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		sl := rl.ScopeLogs().At(i)
		if sl.SchemaUrl() == srcSchema &&
			sl.Scope().Name() == srcScope.Name() &&
			sl.Scope().Version() == srcScope.Version() &&
			sl.Scope().Attributes().Equal(srcScope.Attributes()) {
			return sl
		}
	}
	sl := rl.ScopeLogs().AppendEmpty()
	srcScope.CopyTo(sl.Scope())
	sl.SetSchemaUrl(srcSchema)
	return sl
}

func traceIDFromLogs(ld plog.Logs) pcommon.TraceID {
	rl := ld.ResourceLogs()
	if rl.Len() == 0 {
		return pcommon.NewTraceIDEmpty()
	}

	sl := rl.At(0).ScopeLogs()
	if sl.Len() == 0 {
		return pcommon.NewTraceIDEmpty()
	}

	logs := sl.At(0).LogRecords()
	if logs.Len() == 0 {
		return pcommon.NewTraceIDEmpty()
	}

	return logs.At(0).TraceID()
}

func random() pcommon.TraceID {
	v1 := uint8(rand.IntN(256))
	v2 := uint8(rand.IntN(256))
	v3 := uint8(rand.IntN(256))
	v4 := uint8(rand.IntN(256))
	return [16]byte{v1, v2, v3, v4}
}
