// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/aggregator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/customottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/model"
)

type logsToMetricsProcessor struct {
	config           *config.Config
	nextLogsConsumer consumer.Logs
	logger           *zap.Logger
	telemetry        *logsToMetricsTelemetry
	collectorInfo    model.CollectorInstanceInfo
	logMetricDefs    []model.MetricDef[*ottllog.TransformContext]
	dropLogs         bool
}

func newProcessor(
	cfg *config.Config,
	nextLogsConsumer consumer.Logs,
	settings processor.Settings,
) (*logsToMetricsProcessor, error) {
	telemetry, err := newLogsToMetricsTelemetry(settings, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry: %w", err)
	}

	parser, err := ottllog.NewParser(customottl.LogFuncs(), settings.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL statement parser for logs: %w", err)
	}

	metricDefs := make([]model.MetricDef[*ottllog.TransformContext], 0, len(cfg.Logs))
	for i := range cfg.Logs {
		info := cfg.Logs[i]
		var md model.MetricDef[*ottllog.TransformContext]
		if err := md.FromMetricInfo(info, parser, settings.TelemetrySettings); err != nil {
			return nil, fmt.Errorf("failed to parse provided metric information: %w", err)
		}
		metricDefs = append(metricDefs, md)
	}

	return &logsToMetricsProcessor{
		config:    cfg,
		logger:    settings.Logger,
		telemetry: telemetry,
		collectorInfo: model.NewCollectorInstanceInfo(
			settings.TelemetrySettings,
		),
		nextLogsConsumer: nextLogsConsumer,
		logMetricDefs:    metricDefs,
		dropLogs:         cfg.DropLogs,
	}, nil
}

func (*logsToMetricsProcessor) Start(context.Context, component.Host) error {
	return nil
}

func (p *logsToMetricsProcessor) Shutdown(context.Context) error {
	if p.telemetry != nil {
		p.telemetry.shutdown()
	}
	return nil
}

func (*logsToMetricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// processLogs implements the ProcessLogsFunc type for processorhelper.NewLogs
func (p *logsToMetricsProcessor) processLogs(
	ctx context.Context,
	logs plog.Logs,
) (plog.Logs, error) {
	err := p.ConsumeLogs(ctx, logs)
	if err != nil {
		return logs, err
	}
	// If logs were dropped, return error to skip forwarding (similar to probabilistic sampler)
	// Otherwise return original logs for forwarding
	if p.dropLogs {
		return logs, processorhelper.ErrSkipProcessingData
	}
	return logs, nil
}

func (p *logsToMetricsProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	startTime := time.Now()
	logCount := logs.LogRecordCount()

	// Track per-metric statistics
	metricErrors := make(map[string]int64)
	metricExtracted := make(map[string]int64)
	var totalErrorCount int64
	var totalMetricsExtracted int64

	if len(p.logMetricDefs) == 0 {
		// No metric definitions, just record telemetry
		if p.dropLogs {
			p.telemetry.recordLogsDropped(ctx, int64(logCount))
		}
		return nil
	}

	processedMetrics := pmetric.NewMetrics()
	processedMetrics.ResourceMetrics().EnsureCapacity(logs.ResourceLogs().Len())
	agg := aggregator.NewAggregator[*ottllog.TransformContext](processedMetrics)

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLog := logs.ResourceLogs().At(i)
		resourceAttrs := resourceLog.Resource().Attributes()
		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLog := resourceLog.ScopeLogs().At(j)
			for k := 0; k < scopeLog.LogRecords().Len(); k++ {
				log := scopeLog.LogRecords().At(k)
				logAttrs := log.Attributes()
				for _, md := range p.logMetricDefs {
					metricName := md.Key.Name
					filteredLogAttrs, ok := md.FilterAttributes(logAttrs)
					if !ok {
						continue
					}

					// The transform context is created from original attributes so that the
					// OTTL expressions are also applied on the original attributes.
					tCtx := ottllog.NewTransformContextPtr(resourceLog, scopeLog, log)
					if md.Conditions != nil {
						match, err := md.Conditions.Eval(ctx, tCtx)
						if err != nil {
							tCtx.Close()
							p.logger.Debug(
								"condition evaluation error",
								zap.String("name", metricName),
								zap.Error(err),
							)
							metricErrors[metricName]++
							totalErrorCount++
							continue
						}
						if !match {
							tCtx.Close()
							p.logger.Debug(
								"condition not matched, skipping",
								zap.String("name", metricName),
							)
							continue
						}
					}
					filteredResAttrs := md.FilterResourceAttributes(resourceAttrs, p.collectorInfo)
					if err := agg.Aggregate(ctx, tCtx, md, filteredResAttrs, filteredLogAttrs, 1); err != nil {
						tCtx.Close()
						p.logger.Debug(
							"aggregation error",
							zap.String("name", metricName),
							zap.Error(err),
						)
						metricErrors[metricName]++
						totalErrorCount++
						continue
					}
					tCtx.Close()
					metricExtracted[metricName]++
					totalMetricsExtracted++
				}
			}
		}
	}
	agg.Finalize(p.logMetricDefs)

	if processedMetrics.ResourceMetrics().Len() > 0 {
		if err := routeMetrics(ctx, p.config.Route, processedMetrics); err != nil {
			p.logger.Error("Failed to send metrics to routereceiver", zap.Error(err))
			totalErrorCount++
			// Continue processing logs even if metrics send fails
		}
	}

	// Record processor-level telemetry
	p.telemetry.recordLogsProcessed(ctx, int64(logCount))

	// Record per-metric telemetry
	for metricName, count := range metricExtracted {
		if count > 0 {
			p.telemetry.recordMetricsExtracted(ctx, count, metricName)
		}
	}
	// Also record total without metric_name for backward compatibility
	if totalMetricsExtracted > 0 {
		p.telemetry.recordMetricsExtracted(ctx, totalMetricsExtracted, "")
	}

	// Record per-metric errors
	for metricName, count := range metricErrors {
		if count > 0 {
			p.telemetry.recordError(ctx, count, metricName)
		}
	}
	// Also record total errors without metric_name for backward compatibility
	if totalErrorCount > 0 {
		p.telemetry.recordError(ctx, totalErrorCount, "")
	}

	// Record processing duration (processor-level, not per-metric to avoid cardinality explosion)
	duration := time.Since(startTime)
	p.telemetry.recordProcessingDuration(ctx, duration.Milliseconds(), "")

	// Don't forward logs here - processLogs will handle forwarding/dropping
	// Just record telemetry for dropped logs
	if p.dropLogs {
		p.telemetry.recordLogsDropped(ctx, int64(logCount))
	}
	return nil
}
