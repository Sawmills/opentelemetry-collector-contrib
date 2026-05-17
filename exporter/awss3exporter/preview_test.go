// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Sawmills addition: preview marshaling tests for downstream live-tail support.

package awss3exporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMarshalLogsForPreviewUsesExporterMarshaler(t *testing.T) {
	logs := getTestLogs(t)
	logs.ResourceLogs().At(0).Resource().Attributes().PutStr("_sourceName", "testSourceName")

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf, SumoIC, Body} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := newMarshaler(marshalerName, zap.NewNop())
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalLogs(logs)
			require.NoError(t, err)

			result, err := MarshalLogsForPreview(logs, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.format(), result.FileFormat)
			require.Equal(t, marshaler.compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalLogsForPreviewUsesBatchingMarshaler(t *testing.T) {
	logs := getTestLogs(t)
	core, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	marshaler, err := newMarshalerWithConfig(OtlpJSON, 1<<20, zap.NewNop())
	require.NoError(t, err)

	payload, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)
	require.Empty(t, payload)

	flusher, ok := marshaler.(logFlusher)
	require.True(t, ok)
	expectedPayload, err := flusher.FlushLogs()
	require.NoError(t, err)

	result, err := MarshalLogsForPreview(
		logs,
		OtlpJSON,
		WithPreviewLogger(logger),
		WithPreviewMaxFileSizeBytes(1<<20),
	)
	require.NoError(t, err)

	require.Equal(t, marshaler.format(), result.FileFormat)
	require.Equal(t, marshaler.compressed(), result.IsCompressed)
	require.Equal(t, expectedPayload, result.Payload)
	require.Len(t, observedLogs.FilterMessage("Flushed uncompressed JSONL log batch").All(), 1)
}

func TestMarshalTracesForPreviewUsesExporterMarshaler(t *testing.T) {
	traces := getPreviewTestTraces()

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := newMarshaler(marshalerName, zap.NewNop())
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalTraces(traces)
			require.NoError(t, err)

			result, err := MarshalTracesForPreview(traces, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.format(), result.FileFormat)
			require.Equal(t, marshaler.compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalTracesForPreviewUsesBatchingMarshaler(t *testing.T) {
	traces := getPreviewTestTraces()
	core, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	marshaler, err := newMarshalerWithConfig(OtlpJSON, 1<<20, zap.NewNop())
	require.NoError(t, err)

	payload, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)
	require.Empty(t, payload)

	flusher, ok := marshaler.(traceFlusher)
	require.True(t, ok)
	expectedPayload, err := flusher.FlushTraces()
	require.NoError(t, err)

	result, err := MarshalTracesForPreview(
		traces,
		OtlpJSON,
		WithPreviewLogger(logger),
		WithPreviewMaxFileSizeBytes(1<<20),
	)
	require.NoError(t, err)

	require.Equal(t, marshaler.format(), result.FileFormat)
	require.Equal(t, marshaler.compressed(), result.IsCompressed)
	require.Equal(t, expectedPayload, result.Payload)
	require.Len(t, observedLogs.FilterMessage("Flushed uncompressed JSONL trace batch").All(), 1)
}

func TestMarshalMetricsForPreviewUsesExporterMarshaler(t *testing.T) {
	metrics := getPreviewTestMetrics()

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := newMarshaler(marshalerName, zap.NewNop())
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalMetrics(metrics)
			require.NoError(t, err)

			result, err := MarshalMetricsForPreview(metrics, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.format(), result.FileFormat)
			require.Equal(t, marshaler.compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalLogsForPreviewUnknownMarshaler(t *testing.T) {
	result, err := MarshalLogsForPreview(plog.NewLogs(), MarshalerType("unknown"))

	require.ErrorIs(t, err, ErrUnknownMarshaler)
	require.Equal(t, PreviewResult{}, result)
}

func getPreviewTestTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("test-span")
	return traces
}

func getPreviewTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	metric := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("test.metric")
	point := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	point.SetIntValue(1)
	return metrics
}
