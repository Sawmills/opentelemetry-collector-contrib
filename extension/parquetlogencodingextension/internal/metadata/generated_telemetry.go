package metadata

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
)

func Meter(settings component.TelemetrySettings) metric.Meter {
	return settings.MeterProvider.Meter(ScopeName)
}

type TelemetryBuilder struct {
	meter                                            metric.Meter
	mu                                               sync.Mutex
	registrations                                    []metric.Registration
	ExtensionParquetLogEncodingBufferEstimatedBytes  metric.Int64Gauge
	ExtensionParquetLogEncodingBufferRecords         metric.Int64Gauge
	ExtensionParquetLogEncodingBufferOldestRecordAge metric.Int64ObservableGauge
	ExtensionParquetLogEncodingFlushAttemptTotal     metric.Int64Counter
	ExtensionParquetLogEncodingFlushTotal            metric.Int64Counter
	ExtensionParquetLogEncodingFlushEmptyTotal       metric.Int64Counter
	ExtensionParquetLogEncodingFlushFailedTotal      metric.Int64Counter
	ExtensionParquetLogEncodingFlushedRecords        metric.Int64Counter
	ExtensionParquetLogEncodingFlushedBytes          metric.Int64Counter
	ExtensionParquetLogEncodingTimeToFlush           metric.Int64Histogram
}

func (builder *TelemetryBuilder) Shutdown() {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	for _, reg := range builder.registrations {
		reg.Unregister()
	}
}

func NewTelemetryBuilder(settings component.TelemetrySettings) (*TelemetryBuilder, error) {
	builder := TelemetryBuilder{meter: Meter(settings)}
	var err, errs error
	builder.ExtensionParquetLogEncodingBufferEstimatedBytes, err = builder.meter.Int64Gauge(
		"otelcol_extension_parquet_log_encoding_buffer_estimated_bytes",
		metric.WithDescription("Estimated compressed bytes currently buffered in the parquet log encoder"),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingBufferRecords, err = builder.meter.Int64Gauge(
		"otelcol_extension_parquet_log_encoding_buffer_records",
		metric.WithDescription("Number of log records currently buffered in the parquet log encoder"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingBufferOldestRecordAge, err = builder.meter.Int64ObservableGauge(
		"otelcol_extension_parquet_log_encoding_buffer_oldest_record_age",
		metric.WithDescription("Age in seconds of the oldest buffered record in the parquet log encoder"),
		metric.WithUnit("s"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushAttemptTotal, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flush_attempt_total",
		metric.WithDescription("Number of flush attempts made by the parquet log encoder"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushTotal, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flush_total",
		metric.WithDescription("Number of non-empty parquet buffer flushes"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushEmptyTotal, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flush_empty_total",
		metric.WithDescription("Number of flush attempts that found no buffered parquet records"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushFailedTotal, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flush_failed_total",
		metric.WithDescription("Number of parquet flush attempts that failed after records were buffered"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushedRecords, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flushed_records",
		metric.WithDescription("Number of buffered records emitted by non-empty parquet flushes"),
		metric.WithUnit("1"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingFlushedBytes, err = builder.meter.Int64Counter(
		"otelcol_extension_parquet_log_encoding_flushed_bytes",
		metric.WithDescription("Bytes emitted by non-empty parquet flushes before S3-level compression"),
		metric.WithUnit("By"),
	)
	errs = errors.Join(errs, err)
	builder.ExtensionParquetLogEncodingTimeToFlush, err = builder.meter.Int64Histogram(
		"otelcol_extension_parquet_log_encoding_time_to_flush",
		metric.WithDescription("Time in milliseconds from first buffered parquet record to successful non-empty flush"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 250, 500, 1000, 5000, 10000, 30000, 60000, 300000, 900000),
	)
	errs = errors.Join(errs, err)
	return &builder, errs
}

func (builder *TelemetryBuilder) RegisterExtensionParquetLogEncodingBufferOldestRecordAgeCallback(
	callback metric.Callback,
) error {
	reg, err := builder.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			return callback(ctx, observer)
		},
		builder.ExtensionParquetLogEncodingBufferOldestRecordAge,
	)
	if err != nil {
		return err
	}

	builder.mu.Lock()
	defer builder.mu.Unlock()
	builder.registrations = append(builder.registrations, reg)
	return nil
}
