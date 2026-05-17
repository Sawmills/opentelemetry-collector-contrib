// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3marshaler // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type MarshalerType string

const (
	OtlpProtobuf MarshalerType = "otlp_proto"
	OtlpJSON     MarshalerType = "otlp_json"
	SumoIC       MarshalerType = "sumo_ic"
	Body         MarshalerType = "body"
)

// Marshaler marshals OpenTelemetry data into an AWS S3 exporter payload.
type Marshaler interface {
	MarshalTraces(td ptrace.Traces) ([]byte, error)
	MarshalLogs(ld plog.Logs) ([]byte, error)
	MarshalMetrics(md pmetric.Metrics) ([]byte, error)
	Format() string
	Compressed() bool
}

// LogFlusher flushes buffered log payloads.
type LogFlusher interface {
	FlushLogs() ([]byte, error)
}

// LogFlusherWithReason flushes buffered log payloads with a caller-provided reason.
type LogFlusherWithReason interface {
	FlushLogsWithReason(reason string) ([]byte, error)
}

// TraceFlusher flushes buffered trace payloads.
type TraceFlusher interface {
	FlushTraces() ([]byte, error)
}

var ErrUnknownMarshaler = errors.New("unknown marshaler")

// NewMarshalerWithConfig creates a built-in AWS S3 marshaler. OtlpJSON uses
// JSONL batching when maxBatchSize is greater than zero.
func NewMarshalerWithConfig(mType MarshalerType, maxBatchSize int, logger *zap.Logger) (Marshaler, error) {
	if mType == OtlpJSON && maxBatchSize > 0 {
		return newJSONBatchingMarshaler(maxBatchSize, logger), nil
	}
	return NewMarshaler(mType)
}

// NewMarshaler creates a built-in AWS S3 marshaler.
func NewMarshaler(mType MarshalerType) (Marshaler, error) {
	marshaler := &s3Marshaler{}
	switch mType {
	case OtlpProtobuf:
		marshaler.logsMarshaler = &plog.ProtoMarshaler{}
		marshaler.tracesMarshaler = &ptrace.ProtoMarshaler{}
		marshaler.metricsMarshaler = &pmetric.ProtoMarshaler{}
		marshaler.fileFormat = "binpb"
		marshaler.isCompressed = false
	case OtlpJSON:
		marshaler.logsMarshaler = &plog.JSONMarshaler{}
		marshaler.tracesMarshaler = &ptrace.JSONMarshaler{}
		marshaler.metricsMarshaler = &pmetric.JSONMarshaler{}
		marshaler.fileFormat = "json"
		marshaler.isCompressed = false
	case SumoIC:
		sumomarshaler := newSumoICMarshaler()
		marshaler.logsMarshaler = &sumomarshaler
		marshaler.tracesMarshaler = &sumomarshaler
		marshaler.metricsMarshaler = &sumomarshaler
		marshaler.fileFormat = "json"
		marshaler.isCompressed = true
	case Body:
		exportbodyMarshaler := newbodyMarshaler()
		marshaler.logsMarshaler = &exportbodyMarshaler
		marshaler.tracesMarshaler = &exportbodyMarshaler
		marshaler.metricsMarshaler = &exportbodyMarshaler
		marshaler.fileFormat = exportbodyMarshaler.format()
		marshaler.isCompressed = false
	default:
		return nil, ErrUnknownMarshaler
	}
	return marshaler, nil
}

type s3Marshaler struct {
	logsMarshaler    plog.Marshaler
	tracesMarshaler  ptrace.Marshaler
	metricsMarshaler pmetric.Marshaler
	fileFormat       string
	isCompressed     bool
}

func (marshaler *s3Marshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	return marshaler.tracesMarshaler.MarshalTraces(td)
}

func (marshaler *s3Marshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	return marshaler.logsMarshaler.MarshalLogs(ld)
}

func (marshaler *s3Marshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return marshaler.metricsMarshaler.MarshalMetrics(md)
}

func (marshaler *s3Marshaler) Format() string {
	return marshaler.fileFormat
}

func (marshaler *s3Marshaler) Compressed() bool {
	return marshaler.isCompressed
}

func (*s3Marshaler) FlushLogs() ([]byte, error) {
	return nil, nil
}

func (marshaler *s3Marshaler) FlushLogsWithReason(string) ([]byte, error) {
	return marshaler.FlushLogs()
}

func (*s3Marshaler) FlushTraces() ([]byte, error) {
	return nil, nil
}
