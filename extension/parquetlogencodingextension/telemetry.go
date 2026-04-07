// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension"

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

func (pt *parquetTelemetry) recordBufferState(records int, estimatedBytes, oldestRecordAge int64) {
	if pt == nil {
		return
	}

	attrs := metric.WithAttributes(pt.attrs()...)
	pt.telemetryBuilder.ParquetLogEncodingBufferRecords.Record(
		pt.exportCtx,
		int64(records),
		attrs,
	)
	pt.telemetryBuilder.ParquetLogEncodingBufferEstimatedBytes.Record(
		pt.exportCtx,
		estimatedBytes,
		attrs,
	)
	pt.telemetryBuilder.ParquetLogEncodingBufferOldestRecordAge.Record(
		pt.exportCtx,
		oldestRecordAge,
		attrs,
	)
}

func (pt *parquetTelemetry) recordFlushAttempt(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ParquetLogEncodingFlushAttemptTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordEmptyFlush(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ParquetLogEncodingFlushEmptyTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordFailedFlush(reason string) {
	if pt == nil {
		return
	}
	pt.telemetryBuilder.ParquetLogEncodingFlushFailedTotal.Add(
		pt.exportCtx,
		1,
		metric.WithAttributes(pt.attrs(attribute.String("reason", reason))...),
	)
}

func (pt *parquetTelemetry) recordDroppedRecords(reason string, count int64) {
	if pt == nil || count <= 0 {
		return
	}
	pt.telemetryBuilder.ParquetLogEncodingDroppedRecordsTotal.Add(
		pt.exportCtx,
		count,
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
	pt.telemetryBuilder.ParquetLogEncodingFlushTotal.Add(pt.exportCtx, 1, attrs)
	pt.telemetryBuilder.ParquetLogEncodingFlushedRecords.Add(
		pt.exportCtx,
		flushedRecords,
		attrs,
	)
	pt.telemetryBuilder.ParquetLogEncodingFlushedBytes.Add(
		pt.exportCtx,
		flushedBytes,
		attrs,
	)
	pt.telemetryBuilder.ParquetLogEncodingTimeToFlush.Record(
		pt.exportCtx,
		durationMs(timeToFlush),
		attrs,
	)
}

func durationMs(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}
