// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3marshaler // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"

import (
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// PreviewResult contains bytes and output metadata produced by the AWS S3
// exporter's built-in marshaler selection path. Payload contains marshaler
// output before exporter-level gzip/zstd compression.
type PreviewResult struct {
	FileFormat   string
	Compression  configcompression.Type
	IsCompressed bool
	Payload      []byte
}

type previewOptions struct {
	logger           *zap.Logger
	maxFileSizeBytes int
	compression      configcompression.Type
	flushBatches     bool
}

// PreviewOption configures preview marshaling.
type PreviewOption func(*previewOptions)

// WithPreviewLogger routes JSONL batching logs through logger.
func WithPreviewLogger(logger *zap.Logger) PreviewOption {
	return func(options *previewOptions) {
		options.logger = logger
	}
}

// WithPreviewMaxFileSizeBytes enables OtlpJSON JSONL batching when
// maxFileSizeBytes is greater than zero.
func WithPreviewMaxFileSizeBytes(maxFileSizeBytes int) PreviewOption {
	return func(options *previewOptions) {
		options.maxFileSizeBytes = maxFileSizeBytes
	}
}

// WithPreviewCompression reports exporter-level compression metadata.
func WithPreviewCompression(compression configcompression.Type) PreviewOption {
	return func(options *previewOptions) {
		options.compression = compression
	}
}

// WithPreviewFlushBatches flushes buffered JSONL batches during preview.
func WithPreviewFlushBatches() PreviewOption {
	return func(options *previewOptions) {
		options.flushBatches = true
	}
}

// MarshalLogsForPreview marshals logs with a built-in AWS S3 marshaler without
// starting an exporter or uploading to S3. Encoding-extension marshalers are
// intentionally not supported.
func MarshalLogsForPreview(logs plog.Logs, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	options := newPreviewOptions(opts...)
	marshaler, err := newPreviewMarshaler(marshalerName, options)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalLogs(logs)
	if err != nil {
		return PreviewResult{}, err
	}

	if len(payload) == 0 && options.flushBatches {
		flushed, err := flushPreviewLogs(marshaler)
		if err != nil {
			return PreviewResult{}, err
		}
		if flushed != nil {
			payload = flushed
		}
	}

	return newPreviewResult(marshaler, options, payload), nil
}

// MarshalTracesForPreview marshals traces with a built-in AWS S3 marshaler
// without starting an exporter or uploading to S3. Encoding-extension marshalers
// are intentionally not supported.
func MarshalTracesForPreview(traces ptrace.Traces, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	options := newPreviewOptions(opts...)
	marshaler, err := newPreviewMarshaler(marshalerName, options)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalTraces(traces)
	if err != nil {
		return PreviewResult{}, err
	}

	if len(payload) == 0 && options.flushBatches {
		flushed, err := flushPreviewTraces(marshaler)
		if err != nil {
			return PreviewResult{}, err
		}
		if flushed != nil {
			payload = flushed
		}
	}

	return newPreviewResult(marshaler, options, payload), nil
}

// MarshalMetricsForPreview marshals metrics with a built-in AWS S3 marshaler
// without starting an exporter or uploading to S3. Encoding-extension marshalers
// are intentionally not supported.
func MarshalMetricsForPreview(metrics pmetric.Metrics, marshalerName MarshalerType, opts ...PreviewOption) (PreviewResult, error) {
	options := newPreviewOptions(opts...)
	marshaler, err := newPreviewMarshaler(marshalerName, options)
	if err != nil {
		return PreviewResult{}, err
	}

	payload, err := marshaler.MarshalMetrics(metrics)
	if err != nil {
		return PreviewResult{}, err
	}

	return newPreviewResult(marshaler, options, payload), nil
}

func newPreviewOptions(opts ...PreviewOption) previewOptions {
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
	return options
}

func newPreviewMarshaler(marshalerName MarshalerType, options previewOptions) (Marshaler, error) {
	return NewMarshalerWithConfig(marshalerName, options.maxFileSizeBytes, options.logger)
}

func newPreviewResult(marshaler Marshaler, options previewOptions, payload []byte) PreviewResult {
	return PreviewResult{
		FileFormat:   marshaler.Format(),
		Compression:  options.compression,
		IsCompressed: marshaler.Compressed() || options.compression.IsCompressed(),
		Payload:      payload,
	}
}

func flushPreviewLogs(marshaler Marshaler) ([]byte, error) {
	if flusher, ok := marshaler.(LogFlusherWithReason); ok {
		return flusher.FlushLogsWithReason("preview")
	}
	if flusher, ok := marshaler.(LogFlusher); ok {
		return flusher.FlushLogs()
	}
	return nil, nil
}

func flushPreviewTraces(marshaler Marshaler) ([]byte, error) {
	if flusher, ok := marshaler.(TraceFlusher); ok {
		return flusher.FlushTraces()
	}
	return nil, nil
}
