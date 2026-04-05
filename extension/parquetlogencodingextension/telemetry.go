package parquetlogencodingextension

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/internal/metadata"
)

type parquetTelemetry struct {
	exportCtx        context.Context
	extensionAttr    []attribute.KeyValue
	telemetryBuilder *metadata.TelemetryBuilder
}

func newParquetTelemetry(set extension.Settings) (*parquetTelemetry, error) {
	telemetryBuilder, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &parquetTelemetry{
		exportCtx:        context.Background(),
		extensionAttr:    []attribute.KeyValue{attribute.String("extension", set.ID.String())},
		telemetryBuilder: telemetryBuilder,
	}, nil
}

func (pt *parquetTelemetry) attrs(labels ...attribute.KeyValue) []attribute.KeyValue {
	allLabels := make([]attribute.KeyValue, 0, len(pt.extensionAttr)+len(labels))
	allLabels = append(allLabels, labels...)
	allLabels = append(allLabels, pt.extensionAttr...)
	return allLabels
}

func (pt *parquetTelemetry) shutdown() {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.Shutdown()
}

func (pt *parquetTelemetry) registerOldestRecordAgeCallback(observe func() int64) error {
	if pt == nil {
		return nil
	}

	return pt.telemetryBuilder.RegisterExtensionParquetLogEncodingBufferOldestRecordAgeCallback(
		func(_ context.Context, observer metric.Observer) error {
			observer.ObserveInt64(
				pt.telemetryBuilder.ExtensionParquetLogEncodingBufferOldestRecordAge,
				observe(),
				metric.WithAttributes(pt.attrs()...),
			)
			return nil
		},
	)
}

func (pt *parquetTelemetry) recordBufferState(records int, estimatedBytes int64) {
	if pt == nil {
		return
	}

	attrs := metric.WithAttributes(pt.attrs()...)
	pt.telemetryBuilder.ExtensionParquetLogEncodingBufferRecords.Record(
		pt.exportCtx,
		int64(records),
		attrs,
	)
	pt.telemetryBuilder.ExtensionParquetLogEncodingBufferEstimatedBytes.Record(
		pt.exportCtx,
		estimatedBytes,
		attrs,
	)
}

func (pt *parquetTelemetry) recordFlushAttempt(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushAttemptTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordEmptyFlush(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushEmptyTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordFailedFlush(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushFailedTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordFlush(
	reason string,
	flushedRecords, flushedBytes int64,
	timeToFlush time.Duration,
) {
	if pt == nil {
		return
	}

	attrs := metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...)
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushTotal.Add(pt.exportCtx, 1, attrs)
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushedRecords.Add(
		pt.exportCtx,
		flushedRecords,
		attrs,
	)
	pt.telemetryBuilder.ExtensionParquetLogEncodingFlushedBytes.Add(
		pt.exportCtx,
		flushedBytes,
		attrs,
	)
	pt.telemetryBuilder.ExtensionParquetLogEncodingTimeToFlush.Record(
		pt.exportCtx,
		durationMs(timeToFlush),
		attrs,
	)
}

func durationMs(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}
