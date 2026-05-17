// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Sawmills addition: preview marshaling helpers for downstream live-tail support.

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// PreviewResult contains bytes and output metadata produced by the S3
// exporter's built-in marshaler selection path.
type PreviewResult struct {
	FileFormat   string
	IsCompressed bool
	Payload      []byte
}

type previewOptions struct {
	logger           *zap.Logger
	maxFileSizeBytes int
}

// PreviewOption configures preview marshaling.
type PreviewOption func(*previewOptions)

// WithPreviewLogger routes marshaler logs through logger.
func WithPreviewLogger(logger *zap.Logger) PreviewOption {
	return func(options *previewOptions) {
		options.logger = logger
	}
}

// WithPreviewMaxFileSizeBytes enables the same OtlpJSON batching marshaler
// path used by the exporter when Config.MaxFileSizeBytes is greater than zero.
func WithPreviewMaxFileSizeBytes(maxFileSizeBytes int) PreviewOption {
	return func(options *previewOptions) {
		options.maxFileSizeBytes = maxFileSizeBytes
	}
}

// MarshalLogsForPreview marshals logs with the S3 exporter's built-in
// marshaler without starting an exporter or uploading to S3.
func MarshalLogsForPreview(logs plog.Logs, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	marshaler, err := newPreviewMarshaler(marshalerName, opts...)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalLogs(logs)
	if err != nil {
		return PreviewResult{}, err
	}

	if len(payload) == 0 {
		flushed, err := flushPreviewLogs(marshaler)
		if err != nil {
			return PreviewResult{}, err
		}
		if flushed != nil {
			payload = flushed
		}
	}

	return newPreviewResult(marshaler, payload), nil
}

// MarshalTracesForPreview marshals traces with the S3 exporter's built-in
// marshaler without starting an exporter or uploading to S3.
func MarshalTracesForPreview(traces ptrace.Traces, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	marshaler, err := newPreviewMarshaler(marshalerName, opts...)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalTraces(traces)
	if err != nil {
		return PreviewResult{}, err
	}

	if len(payload) == 0 {
		flushed, err := flushPreviewTraces(marshaler)
		if err != nil {
			return PreviewResult{}, err
		}
		if flushed != nil {
			payload = flushed
		}
	}

	return newPreviewResult(marshaler, payload), nil
}

// MarshalMetricsForPreview marshals metrics with the S3 exporter's built-in
// marshaler without starting an exporter or uploading to S3.
func MarshalMetricsForPreview(metrics pmetric.Metrics, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	marshaler, err := newPreviewMarshaler(marshalerName, opts...)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalMetrics(metrics)
	if err != nil {
		return PreviewResult{}, err
	}

	return newPreviewResult(marshaler, payload), nil
}

func newPreviewMarshaler(marshalerName MarshalerType, opts ...PreviewOption) (marshaler, error) {
	options := previewOptions{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.logger == nil {
		options.logger = zap.NewNop()
	}

	return newMarshalerWithConfig(marshalerName, options.maxFileSizeBytes, options.logger)
}

func newPreviewResult(marshaler marshaler, payload []byte) PreviewResult {
	return PreviewResult{
		FileFormat:   marshaler.format(),
		IsCompressed: marshaler.compressed(),
		Payload:      payload,
	}
}

func flushPreviewLogs(marshaler marshaler) ([]byte, error) {
	if flusher, ok := marshaler.(logFlusherWithReason); ok {
		return flusher.FlushLogsWithReason("preview")
	}
	if flusher, ok := marshaler.(logFlusher); ok {
		return flusher.FlushLogs()
	}
	return nil, nil
}

func flushPreviewTraces(marshaler marshaler) ([]byte, error) {
	if flusher, ok := marshaler.(traceFlusher); ok {
		return flusher.FlushTraces()
	}
	return nil, nil
}
