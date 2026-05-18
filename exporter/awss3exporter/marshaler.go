// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"
)

type (
	marshaler            = awss3marshaler.Marshaler
	logFlusher           = awss3marshaler.LogFlusher
	logFlusherWithReason = awss3marshaler.LogFlusherWithReason
	traceFlusher         = awss3marshaler.TraceFlusher
)

// LogMarshalerWithFlushMetadata is an optional encoding extension interface
// for passing flush timing metadata to S3 exporter telemetry.
type LogMarshalerWithFlushMetadata interface {
	MarshalLogsWithFlushMetadata(plog.Logs) ([]byte, string, time.Time, error)
}

var ErrUnknownMarshaler = awss3marshaler.ErrUnknownMarshaler

type encodingMarshaler struct {
	logsMarshaler           plog.Marshaler
	tracesMarshaler         ptrace.Marshaler
	metricsMarshaler        pmetric.Marshaler
	fileFormat              string
	isCompressed            bool
	extLogFlusher           logFlusher
	extLogFlusherWithReason logFlusherWithReason
	extTraceFlusher         traceFlusher
}

func newMarshalerFromEncoding(encoding *component.ID, fileFormat string, host component.Host) (marshaler, error) {
	marshaler := &encodingMarshaler{fileFormat: fileFormat}
	e, ok := host.GetExtensions()[*encoding]
	if !ok {
		return nil, fmt.Errorf("unknown encoding %q", encoding)
	}
	marshaler.logsMarshaler, _ = e.(plog.Marshaler)
	marshaler.metricsMarshaler, _ = e.(pmetric.Marshaler)
	marshaler.tracesMarshaler, _ = e.(ptrace.Marshaler)
	if flusher, ok := e.(logFlusher); ok {
		marshaler.extLogFlusher = flusher
	}
	if flusher, ok := e.(logFlusherWithReason); ok {
		marshaler.extLogFlusherWithReason = flusher
	}
	if flusher, ok := e.(traceFlusher); ok {
		marshaler.extTraceFlusher = flusher
	}
	return marshaler, nil
}

func (marshaler *encodingMarshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	if marshaler.tracesMarshaler == nil {
		return nil, errors.New("configured encoding does not support traces marshaling")
	}
	return marshaler.tracesMarshaler.MarshalTraces(td)
}

func (marshaler *encodingMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	if marshaler.logsMarshaler == nil {
		return nil, errors.New("configured encoding does not support logs marshaling")
	}
	buf, _, err := marshaler.MarshalLogsWithFlushMetadata(ld)
	return buf, err
}

func (marshaler *encodingMarshaler) MarshalLogsWithFlushMetadata(
	ld plog.Logs,
) ([]byte, flushMetadata, error) {
	if marshaler.logsMarshaler == nil {
		return nil, flushMetadata{}, errors.New("configured encoding does not support logs marshaling")
	}
	if metadataMarshaler, ok := marshaler.logsMarshaler.(LogMarshalerWithFlushMetadata); ok {
		buf, reason, completedAt, err := metadataMarshaler.MarshalLogsWithFlushMetadata(ld)
		if err != nil {
			return nil, flushMetadata{}, err
		}
		return buf, flushMetadata{
			reason:           reason,
			flushCompletedAt: completedAt,
		}, nil
	}

	buf, err := marshaler.logsMarshaler.MarshalLogs(ld)
	return buf, flushMetadata{}, err
}

func (marshaler *encodingMarshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	if marshaler.metricsMarshaler == nil {
		return nil, errors.New("configured encoding does not support metrics marshaling")
	}
	return marshaler.metricsMarshaler.MarshalMetrics(md)
}

func (marshaler *encodingMarshaler) Format() string {
	return marshaler.fileFormat
}

func (marshaler *encodingMarshaler) Compressed() bool {
	return marshaler.isCompressed
}

func (marshaler *encodingMarshaler) FlushLogs() ([]byte, error) {
	if marshaler.extLogFlusher != nil {
		return marshaler.extLogFlusher.FlushLogs()
	}
	return nil, nil
}

func (marshaler *encodingMarshaler) FlushLogsWithReason(reason string) ([]byte, error) {
	if marshaler.extLogFlusherWithReason != nil {
		return marshaler.extLogFlusherWithReason.FlushLogsWithReason(reason)
	}
	return marshaler.FlushLogs()
}

func (marshaler *encodingMarshaler) FlushTraces() ([]byte, error) {
	if marshaler.extTraceFlusher != nil {
		return marshaler.extTraceFlusher.FlushTraces()
	}
	return nil, nil
}
