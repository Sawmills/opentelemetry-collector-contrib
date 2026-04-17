// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type LogMarshalerWithFlushMetadata interface {
	MarshalLogsWithFlushMetadata(plog.Logs) ([]byte, string, time.Time, error)
}

type s3Marshaler struct {
	logsMarshaler           plog.Marshaler
	tracesMarshaler         ptrace.Marshaler
	metricsMarshaler        pmetric.Marshaler
	logger                  *zap.Logger
	fileFormat              string
	IsCompressed            bool
	extLogFlusher           logFlusher
	extLogFlusherWithReason logFlusherWithReason
	extTraceFlusher         traceFlusher
}

func (marshaler *s3Marshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return marshaler.tracesMarshaler.MarshalTraces(td)
}

func (marshaler *s3Marshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	buf, _, err := marshaler.MarshalLogsWithFlushMetadata(ld)
	return buf, err
}

func (marshaler *s3Marshaler) MarshalLogsWithFlushMetadata(
	ld plog.Logs,
) ([]byte, flushMetadata, error) {
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

func (marshaler *s3Marshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return marshaler.metricsMarshaler.MarshalMetrics(md)
}

func (marshaler *s3Marshaler) format() string {
	return marshaler.fileFormat
}

func (marshaler *s3Marshaler) compressed() bool {
	return marshaler.IsCompressed
}

func (marshaler *s3Marshaler) FlushLogs() ([]byte, error) {
	if marshaler.extLogFlusher != nil {
		return marshaler.extLogFlusher.FlushLogs()
	}
	return nil, nil
}

func (marshaler *s3Marshaler) FlushLogsWithReason(reason string) ([]byte, error) {
	if marshaler.extLogFlusherWithReason != nil {
		return marshaler.extLogFlusherWithReason.FlushLogsWithReason(reason)
	}
	return marshaler.FlushLogs()
}

func (marshaler *s3Marshaler) FlushTraces() ([]byte, error) {
	if marshaler.extTraceFlusher != nil {
		return marshaler.extTraceFlusher.FlushTraces()
	}
	return nil, nil
}
