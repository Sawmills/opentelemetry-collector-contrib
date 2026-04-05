// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
)

type flushMetadata struct {
	reason           string
	flushCompletedAt time.Time
}

type marshalerWithFlushMetadata interface {
	MarshalLogsWithFlushMetadata(plog.Logs) ([]byte, flushMetadata, error)
}

type exporterTelemetry struct {
	flushStart            metric.Int64Counter
	flushComplete         metric.Int64Counter
	uploadStart           metric.Int64Counter
	uploadComplete        metric.Int64Counter
	flushDuration         metric.Int64Histogram
	uploadDuration        metric.Int64Histogram
	flushToUploadDuration metric.Int64Histogram
}

const (
	flushStartMetricName            = "otelcol_exporter_awss3_flush_start_total"
	flushCompleteMetricName         = "otelcol_exporter_awss3_flush_complete_total"
	uploadStartMetricName           = "otelcol_exporter_awss3_upload_start_total"
	uploadCompleteMetricName        = "otelcol_exporter_awss3_upload_complete_total"
	flushDurationMetricName         = "otelcol_exporter_awss3_flush_duration"
	uploadDurationMetricName        = "otelcol_exporter_awss3_upload_duration"
	flushToUploadDurationMetricName = "otelcol_exporter_awss3_flush_to_upload_duration"
)

type s3Exporter struct {
	config     *Config
	signalType string
	uploader   upload.Manager
	logger     *zap.Logger
	marshaler  marshaler
	telemetry  *exporterTelemetry
}

func newS3Exporter(
	config *Config,
	signalType string,
	params exporter.Settings,
) *s3Exporter {
	telemetry := newExporterTelemetry(params.TelemetrySettings, params.Logger)

	s3Exporter := &s3Exporter{
		config:     config,
		signalType: signalType,
		logger:     params.Logger,
		telemetry:  telemetry,
	}
	return s3Exporter
}

func (e *s3Exporter) getUploadOpts(res pcommon.Resource) *upload.UploadOptions {
	s3Prefix := ""
	s3Bucket := ""
	if s3PrefixKey := e.config.ResourceAttrsToS3.S3Prefix; s3PrefixKey != "" {
		if value, ok := res.Attributes().Get(s3PrefixKey); ok {
			s3Prefix = value.AsString()
		}
	}
	if s3BucketKey := e.config.ResourceAttrsToS3.S3Bucket; s3BucketKey != "" {
		if value, ok := res.Attributes().Get(s3BucketKey); ok {
			s3Bucket = value.AsString()
		}
	}
	uploadOpts := &upload.UploadOptions{
		OverrideBucket: s3Bucket,
		OverridePrefix: s3Prefix,
	}
	return uploadOpts
}

func (e *s3Exporter) start(ctx context.Context, host component.Host) error {
	var m marshaler
	var err error
	if e.config.Encoding != nil {
		if m, err = newMarshalerFromEncoding(e.config.Encoding, e.config.EncodingFileExtension, host, e.logger); err != nil {
			return err
		}
	} else {
		if m, err = newMarshaler(e.config.MarshalerName, e.logger); err != nil {
			return fmt.Errorf("unknown marshaler %q", e.config.MarshalerName)
		}
	}

	e.marshaler = m

	up, err := newUploadManager(ctx, e.config, e.logger, e.signalType, m.format(), m.compressed())
	if err != nil {
		return err
	}
	e.uploader = up
	return nil
}

func (*s3Exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *s3Exporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	buf, err := e.marshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}

	uploadOpts := e.getUploadOpts(md.ResourceMetrics().At(0).Resource())
	return e.uploadBuffer(ctx, buf, uploadOpts, flushMetadata{})
}

func (e *s3Exporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	flushStartedAt := time.Now()
	e.telemetry.recordFlushStart(ctx, e.signalType)

	flushMeta := flushMetadata{}
	var (
		buf []byte
		err error
	)
	if metadataMarshaler, ok := e.marshaler.(marshalerWithFlushMetadata); ok {
		buf, flushMeta, err = metadataMarshaler.MarshalLogsWithFlushMetadata(logs)
	} else {
		buf, err = e.marshaler.MarshalLogs(logs)
	}
	e.telemetry.recordFlushComplete(ctx, e.signalType, flushMeta.reason, time.Since(flushStartedAt), err)
	if err != nil {
		return err
	}

	uploadOpts := e.getUploadOpts(logs.ResourceLogs().At(0).Resource())
	return e.uploadBuffer(ctx, buf, uploadOpts, flushMeta)
}

func (e *s3Exporter) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	buf, err := e.marshaler.MarshalTraces(traces)
	if err != nil {
		return err
	}
	uploadOpts := e.getUploadOpts(traces.ResourceSpans().At(0).Resource())
	return e.uploadBuffer(ctx, buf, uploadOpts, flushMetadata{})
}

func (e *s3Exporter) uploadBuffer(
	ctx context.Context,
	buf []byte,
	uploadOpts *upload.UploadOptions,
	flushMeta flushMetadata,
) error {
	if len(buf) == 0 {
		return nil
	}

	uploadStartedAt := time.Now()
	e.telemetry.recordUploadStart(ctx, e.signalType)
	err := e.uploader.Upload(ctx, buf, uploadOpts)
	e.telemetry.recordUploadComplete(
		ctx,
		e.signalType,
		uploadStartedAt,
		time.Since(uploadStartedAt),
		flushMeta,
		err,
	)
	return err
}

func newExporterTelemetry(settings component.TelemetrySettings, logger *zap.Logger) *exporterTelemetry {
	meterProvider := settings.MeterProvider
	if meterProvider == nil {
		meterProvider = noop.NewMeterProvider()
	}

	meter := meterProvider.Meter(metadata.ScopeName)
	tel := &exporterTelemetry{}
	tel.flushStart = mustCounter(meter, flushStartMetricName, logger)
	tel.flushComplete = mustCounter(meter, flushCompleteMetricName, logger)
	tel.uploadStart = mustCounter(meter, uploadStartMetricName, logger)
	tel.uploadComplete = mustCounter(meter, uploadCompleteMetricName, logger)
	tel.flushDuration = mustHistogram(meter, flushDurationMetricName, logger)
	tel.uploadDuration = mustHistogram(meter, uploadDurationMetricName, logger)
	tel.flushToUploadDuration = mustHistogram(meter, flushToUploadDurationMetricName, logger)
	return tel
}

func mustCounter(meter metric.Meter, name string, logger *zap.Logger) metric.Int64Counter {
	counter, err := meter.Int64Counter(name)
	if err != nil && logger != nil {
		logger.Warn("failed to create awss3 exporter counter", zap.String("name", name), zap.Error(err))
	}
	return counter
}

func mustHistogram(meter metric.Meter, name string, logger *zap.Logger) metric.Int64Histogram {
	histogram, err := meter.Int64Histogram(name, metric.WithUnit("ms"))
	if err != nil && logger != nil {
		logger.Warn("failed to create awss3 exporter histogram", zap.String("name", name), zap.Error(err))
	}
	return histogram
}

func (t *exporterTelemetry) recordFlushStart(ctx context.Context, signalType string) {
	if t == nil || t.flushStart == nil {
		return
	}
	t.flushStart.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", signalType)))
}

func (t *exporterTelemetry) recordFlushComplete(
	ctx context.Context,
	signalType string,
	reason string,
	duration time.Duration,
	err error,
) {
	if t == nil {
		return
	}

	attrs := flushOutcomeAttrs(signalType, reason, err)
	if t.flushComplete != nil {
		t.flushComplete.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if t.flushDuration != nil {
		t.flushDuration.Record(ctx, durationMillis(duration), metric.WithAttributes(attrs...))
	}
}

func (t *exporterTelemetry) recordUploadStart(ctx context.Context, signalType string) {
	if t == nil || t.uploadStart == nil {
		return
	}
	t.uploadStart.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", signalType)))
}

func (t *exporterTelemetry) recordUploadComplete(
	ctx context.Context,
	signalType string,
	uploadStartedAt time.Time,
	duration time.Duration,
	flushMeta flushMetadata,
	err error,
) {
	if t == nil {
		return
	}

	attrs := flushOutcomeAttrs(signalType, flushMeta.reason, err)
	if t.uploadComplete != nil {
		t.uploadComplete.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if t.uploadDuration != nil {
		t.uploadDuration.Record(ctx, durationMillis(duration), metric.WithAttributes(attrs...))
	}
	if t.flushToUploadDuration != nil && !flushMeta.flushCompletedAt.IsZero() {
		t.flushToUploadDuration.Record(
			ctx,
			durationMillis(uploadStartedAt.Sub(flushMeta.flushCompletedAt)),
			metric.WithAttributes(attrs...),
		)
	}
}

func flushOutcomeAttrs(signalType string, reason string, err error) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("signal", signalType),
		attribute.String("outcome", telemetryOutcome(err)),
	}
	if reason != "" {
		attrs = append(attrs, attribute.String("reason", reason))
	}
	return attrs
}

func telemetryOutcome(err error) string {
	if err != nil {
		return "failure"
	}
	return "success"
}

func durationMillis(duration time.Duration) int64 {
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}
