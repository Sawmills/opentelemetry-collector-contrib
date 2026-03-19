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

	var errs error
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				record := scopeLogs.LogRecords().At(k)
				errs = multierr.Append(errs, e.consumeLogRecord(resourceLogs, scopeLogs, record))
			}
		}
	}

	return errs
}

func (e *logExporterImp) consumeLogRecord(resourceLogs plog.ResourceLogs, scopeLogs plog.ScopeLogs, record plog.LogRecord) error {
	traceID := record.TraceID()
	balancingKey := traceID
	if traceID == pcommon.NewTraceIDEmpty() {
		balancingKey = random()
	}

	logs := singleLogRecord(resourceLogs, scopeLogs, record)
	for range 2 {
		le, endpoint, err := e.loadBalancer.exporterAndEndpoint(balancingKey[:])
		if err != nil {
			return err
		}
		err = e.batcher.Enqueue(endpointWithPort(endpoint), le, logs)
		if !errors.Is(err, errLogBatcherExporterStopping) {
			return err
		}
	}

	return errLogBatcherExporterStopping
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

	return e.consumeBatch(ctx, le, ld, logFlushReasonSize)
}

func (e *logExporterImp) consumeBatch(ctx context.Context, le *wrappedExporter, ld plog.Logs, _ string) error {
	le.consumeWG.Add(1)
	defer le.consumeWG.Done()

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

func singleLogRecord(resourceLogs plog.ResourceLogs, scopeLogs plog.ScopeLogs, record plog.LogRecord) plog.Logs {
	logs := plog.NewLogs()

	targetResourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().CopyTo(targetResourceLogs.Resource())
	targetResourceLogs.SetSchemaUrl(resourceLogs.SchemaUrl())

	targetScopeLogs := targetResourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().CopyTo(targetScopeLogs.Scope())
	targetScopeLogs.SetSchemaUrl(scopeLogs.SchemaUrl())

	record.CopyTo(targetScopeLogs.LogRecords().AppendEmpty())
	return logs
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
