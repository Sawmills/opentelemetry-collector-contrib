// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pipeline"
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
	metricsConsumer  consumer.Metrics
	logger           *zap.Logger
	telemetry        *logsToMetricsTelemetry
	collectorInfo    model.CollectorInstanceInfo
	logMetricDefs    []model.MetricDef[ottllog.TransformContext]
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

	metricDefs := make([]model.MetricDef[ottllog.TransformContext], 0, len(cfg.Logs))
	for i := range cfg.Logs {
		info := cfg.Logs[i]
		var md model.MetricDef[ottllog.TransformContext]
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

// DataType represents the type of telemetry data
type DataType int

const (
	// DataTypeMetrics represents metrics data type
	DataTypeMetrics DataType = iota
)

// connectorHost is an interface for accessing connectors from component.Host
type connectorHost interface {
	GetConnectors() map[component.ID]component.Component
}

func (p *logsToMetricsProcessor) Start(ctx context.Context, host component.Host) error {
	ch, ok := host.(connectorHost)
	if !ok {
		return fmt.Errorf("host does not support GetConnectors() method")
	}

	connectors := ch.GetConnectors()
	conn, ok := connectors[p.config.MetricsConnector]
	if !ok {
		return fmt.Errorf("metrics connector %s not found", p.config.MetricsConnector)
	}

	metricsConnector, ok := conn.(connector.Metrics)
	if !ok {
		return fmt.Errorf("connector %s is not a metrics connector", p.config.MetricsConnector)
	}

	// Get the router interface to access consumers for pipelines
	router, ok := metricsConnector.(connector.MetricsRouterAndConsumer)
	if !ok {
		return fmt.Errorf("connector %s does not support MetricsRouterAndConsumer interface", p.config.MetricsConnector)
	}

	// Get the consumer for the metrics pipeline
	pipelineID := pipeline.NewID(pipeline.SignalMetrics)
	metricsConsumer, err := router.Consumer(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get consumer for metrics pipeline from connector %s: %w", p.config.MetricsConnector, err)
	}

	p.metricsConsumer = metricsConsumer
	return nil
}

func (p *logsToMetricsProcessor) Shutdown(context.Context) error {
	if p.telemetry != nil {
		p.telemetry.shutdown()
	}
	return nil
}

func (p *logsToMetricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// processLogs implements the ProcessLogsFunc type for processorhelper.NewLogs
func (p *logsToMetricsProcessor) processLogs(ctx context.Context, logs plog.Logs) (plog.Logs, error) {
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
	agg := aggregator.NewAggregator[ottllog.TransformContext](processedMetrics)

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
					tCtx := ottllog.NewTransformContext(log, scopeLog.Scope(), resourceLog.Resource(), scopeLog, resourceLog)
					if md.Conditions != nil {
						match, err := md.Conditions.Eval(ctx, tCtx)
						if err != nil {
							p.logger.Debug("condition evaluation error", zap.String("name", metricName), zap.Error(err))
							metricErrors[metricName]++
							totalErrorCount++
							continue
						}
						if !match {
							p.logger.Debug("condition not matched, skipping", zap.String("name", metricName))
							continue
						}
					}
					filteredResAttrs := md.FilterResourceAttributes(resourceAttrs, p.collectorInfo)
					if err := agg.Aggregate(ctx, tCtx, md, filteredResAttrs, filteredLogAttrs, 1); err != nil {
						p.logger.Debug("aggregation error", zap.String("name", metricName), zap.Error(err))
						metricErrors[metricName]++
						totalErrorCount++
						continue
					}
					metricExtracted[metricName]++
					totalMetricsExtracted++
				}
			}
		}
	}
	agg.Finalize(p.logMetricDefs)

	// Send metrics to connector (which routes to metrics pipeline)
	if processedMetrics.ResourceMetrics().Len() > 0 {
		if err := p.metricsConsumer.ConsumeMetrics(ctx, processedMetrics); err != nil {
			p.logger.Error("Failed to send metrics to connector", zap.Error(err))
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
