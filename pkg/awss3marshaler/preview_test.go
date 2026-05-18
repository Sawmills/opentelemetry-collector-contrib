// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3marshaler

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMarshalLogsForPreviewUsesExporterMarshaler(t *testing.T) {
	logs := getPreviewTestLogs()

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf, SumoIC, Body} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := NewMarshaler(marshalerName)
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalLogs(logs)
			require.NoError(t, err)

			result, err := MarshalLogsForPreview(logs, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.Format(), result.FileFormat)
			require.Equal(t, marshaler.Compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalLogsForPreviewPreservesBatchingBehavior(t *testing.T) {
	logs := getPreviewTestLogs()

	result, err := MarshalLogsForPreview(
		logs,
		OtlpJSON,
		WithPreviewMaxFileSizeBytes(1<<20),
	)
	require.NoError(t, err)

	require.Equal(t, "json", result.FileFormat)
	require.False(t, result.IsCompressed)
	require.Empty(t, result.Payload)
}

func TestMarshalLogsForPreviewFlushesBatchingMarshalerWhenRequested(t *testing.T) {
	logs := getPreviewTestLogs()
	core, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	marshaler, err := NewMarshalerWithConfig(OtlpJSON, 1<<20, zap.NewNop())
	require.NoError(t, err)

	payload, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)
	require.Empty(t, payload)

	flusher, ok := marshaler.(LogFlusher)
	require.True(t, ok)
	expectedPayload, err := flusher.FlushLogs()
	require.NoError(t, err)

	result, err := MarshalLogsForPreview(
		logs,
		OtlpJSON,
		WithPreviewLogger(logger),
		WithPreviewMaxFileSizeBytes(1<<20),
		WithPreviewFlushBatches(),
	)
	require.NoError(t, err)

	require.Equal(t, marshaler.Format(), result.FileFormat)
	require.Equal(t, marshaler.Compressed(), result.IsCompressed)
	require.Equal(t, expectedPayload, result.Payload)
	require.Len(t, observedLogs.FilterMessage("Flushed uncompressed JSONL log batch").All(), 1)
}

func TestMarshalTracesForPreviewUsesExporterMarshaler(t *testing.T) {
	traces := getPreviewTestTraces()

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := NewMarshaler(marshalerName)
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalTraces(traces)
			require.NoError(t, err)

			result, err := MarshalTracesForPreview(traces, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.Format(), result.FileFormat)
			require.Equal(t, marshaler.Compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalTracesForPreviewPreservesBatchingBehavior(t *testing.T) {
	traces := getPreviewTestTraces()

	result, err := MarshalTracesForPreview(
		traces,
		OtlpJSON,
		WithPreviewMaxFileSizeBytes(1<<20),
	)
	require.NoError(t, err)

	require.Equal(t, "json", result.FileFormat)
	require.False(t, result.IsCompressed)
	require.Empty(t, result.Payload)
}

func TestMarshalTracesForPreviewFlushesBatchingMarshalerWhenRequested(t *testing.T) {
	traces := getPreviewTestTraces()
	core, observedLogs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	marshaler, err := NewMarshalerWithConfig(OtlpJSON, 1<<20, zap.NewNop())
	require.NoError(t, err)

	payload, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)
	require.Empty(t, payload)

	flusher, ok := marshaler.(TraceFlusher)
	require.True(t, ok)
	expectedPayload, err := flusher.FlushTraces()
	require.NoError(t, err)

	result, err := MarshalTracesForPreview(
		traces,
		OtlpJSON,
		WithPreviewLogger(logger),
		WithPreviewMaxFileSizeBytes(1<<20),
		WithPreviewFlushBatches(),
	)
	require.NoError(t, err)

	require.Equal(t, marshaler.Format(), result.FileFormat)
	require.Equal(t, marshaler.Compressed(), result.IsCompressed)
	require.Equal(t, expectedPayload, result.Payload)
	require.Len(t, observedLogs.FilterMessage("Flushed uncompressed JSONL trace batch").All(), 1)
}

func TestMarshalTracesForPreviewUnsupportedMarshaler(t *testing.T) {
	for _, marshalerName := range []MarshalerType{SumoIC, Body} {
		t.Run(string(marshalerName), func(t *testing.T) {
			var result PreviewResult
			var err error

			require.NotPanics(t, func() {
				result, err = MarshalTracesForPreview(getPreviewTestTraces(), marshalerName)
			})

			require.Error(t, err)
			require.Equal(t, PreviewResult{}, result)
		})
	}
}

func TestMarshalMetricsForPreviewUsesExporterMarshaler(t *testing.T) {
	metrics := getPreviewTestMetrics()

	for _, marshalerName := range []MarshalerType{OtlpJSON, OtlpProtobuf} {
		t.Run(string(marshalerName), func(t *testing.T) {
			marshaler, err := NewMarshaler(marshalerName)
			require.NoError(t, err)

			expectedPayload, err := marshaler.MarshalMetrics(metrics)
			require.NoError(t, err)

			result, err := MarshalMetricsForPreview(metrics, marshalerName)
			require.NoError(t, err)

			require.Equal(t, marshaler.Format(), result.FileFormat)
			require.Equal(t, marshaler.Compressed(), result.IsCompressed)
			require.Equal(t, expectedPayload, result.Payload)
		})
	}
}

func TestMarshalMetricsForPreviewUnsupportedMarshaler(t *testing.T) {
	for _, marshalerName := range []MarshalerType{SumoIC, Body} {
		t.Run(string(marshalerName), func(t *testing.T) {
			var result PreviewResult
			var err error

			require.NotPanics(t, func() {
				result, err = MarshalMetricsForPreview(getPreviewTestMetrics(), marshalerName)
			})

			require.Error(t, err)
			require.Equal(t, PreviewResult{}, result)
		})
	}
}

func TestMarshalLogsForPreviewReportsExporterCompression(t *testing.T) {
	result, err := MarshalLogsForPreview(
		getPreviewTestLogs(),
		OtlpJSON,
		WithPreviewCompression(configcompression.TypeGzip),
	)
	require.NoError(t, err)

	require.Equal(t, configcompression.TypeGzip, result.Compression)
	require.True(t, result.IsCompressed)
	require.NotEmpty(t, result.Payload)
}

func TestMarshalLogsForPreviewUnknownMarshaler(t *testing.T) {
	result, err := MarshalLogsForPreview(plog.NewLogs(), MarshalerType("unknown"))

	require.ErrorIs(t, err, ErrUnknownMarshaler)
	require.Equal(t, PreviewResult{}, result)
}

func getPreviewTestLogs() plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	attrs := resourceLogs.Resource().Attributes()
	attrs.PutStr(SourceCategoryKey, "logfile")
	attrs.PutStr(SourceHostKey, "host")
	attrs.PutStr(SourceNameKey, "source")

	record := resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr("log entry")
	record.Attributes().PutStr("log.file.path_resolved", "data.log")
	return logs
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
