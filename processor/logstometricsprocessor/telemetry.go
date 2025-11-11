// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/metadata"
)

// logsToMetricsTelemetry holds telemetry data and methods for recording metrics.
type logsToMetricsTelemetry struct {
	processorAttr    []attribute.KeyValue
	telemetryBuilder *metadata.TelemetryBuilder
}

// newLogsToMetricsTelemetry initializes a new logsToMetricsTelemetry instance.
func newLogsToMetricsTelemetry(set processor.Settings, cfg *config.Config) (*logsToMetricsTelemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	processorAttr := []attribute.KeyValue{
		attribute.String("processor", set.ID.String()),
	}

	// Add pipeline_id label if it's configured
	if cfg != nil && cfg.PipelineID != "" {
		processorAttr = append(processorAttr, attribute.String("pipeline_id", cfg.PipelineID))
	}

	// Add exporter_id label if it's configured
	if cfg != nil && cfg.ExporterID != "" {
		processorAttr = append(processorAttr, attribute.String("exporter_id", cfg.ExporterID))
	}

	return &logsToMetricsTelemetry{
		processorAttr:    processorAttr,
		telemetryBuilder: telemetryBuilder,
	}, nil
}

// recordLogsProcessed records the number of logs processed.
func (lmt *logsToMetricsTelemetry) recordLogsProcessed(ctx context.Context, count int64) {
	allLabels := make([]attribute.KeyValue, 0, len(lmt.processorAttr))
	allLabels = append(allLabels, lmt.processorAttr...)
	lmt.telemetryBuilder.ProcessorLogstometricsLogsProcessed.Add(
		ctx,
		count,
		metric.WithAttributes(allLabels...),
	)
}

// recordMetricsExtracted records the number of metrics extracted, optionally with metric name.
func (lmt *logsToMetricsTelemetry) recordMetricsExtracted(ctx context.Context, count int64, metricName string) {
	allLabels := make([]attribute.KeyValue, 0, len(lmt.processorAttr)+1)
	allLabels = append(allLabels, lmt.processorAttr...)
	if metricName != "" {
		allLabels = append(allLabels, attribute.String("metric_name", metricName))
	}
	lmt.telemetryBuilder.ProcessorLogstometricsMetricsExtracted.Add(
		ctx,
		count,
		metric.WithAttributes(allLabels...),
	)
}

// recordError records an error, optionally with metric name.
func (lmt *logsToMetricsTelemetry) recordError(ctx context.Context, count int64, metricName string) {
	allLabels := make([]attribute.KeyValue, 0, len(lmt.processorAttr)+1)
	allLabels = append(allLabels, lmt.processorAttr...)
	if metricName != "" {
		allLabels = append(allLabels, attribute.String("metric_name", metricName))
	}
	lmt.telemetryBuilder.ProcessorLogstometricsErrors.Add(
		ctx,
		count,
		metric.WithAttributes(allLabels...),
	)
}

// recordLogsDropped records the number of logs dropped.
func (lmt *logsToMetricsTelemetry) recordLogsDropped(ctx context.Context, count int64) {
	allLabels := make([]attribute.KeyValue, 0, len(lmt.processorAttr))
	allLabels = append(allLabels, lmt.processorAttr...)
	lmt.telemetryBuilder.ProcessorLogstometricsLogsDropped.Add(
		ctx,
		count,
		metric.WithAttributes(allLabels...),
	)
}

// recordProcessingDuration records the processing duration, optionally with metric name.
func (lmt *logsToMetricsTelemetry) recordProcessingDuration(ctx context.Context, durationMs int64, metricName string) {
	allLabels := make([]attribute.KeyValue, 0, len(lmt.processorAttr)+1)
	allLabels = append(allLabels, lmt.processorAttr...)
	if metricName != "" {
		allLabels = append(allLabels, attribute.String("metric_name", metricName))
	}
	lmt.telemetryBuilder.ProcessorLogstometricsProcessingDuration.Record(
		ctx,
		durationMs,
		metric.WithAttributes(allLabels...),
	)
}

// shutdown stops the telemetry builder.
func (lmt *logsToMetricsTelemetry) shutdown() {
	if lmt.telemetryBuilder != nil {
		lmt.telemetryBuilder.Shutdown()
	}
}
