// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3marshaler // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"

import (
	"bytes"
	"encoding/json"
	"sync"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// jsonBatchingMarshaler handles JSON marshaling with proper batching.
type jsonBatchingMarshaler struct {
	mutex sync.Mutex

	logBatches       []json.RawMessage
	logsBatchCount   int
	tracesBatchCount int

	uncompressedSize int
	maxBatchSize     int

	logsMarshaler    plog.JSONMarshaler
	tracesMarshaler  ptrace.JSONMarshaler
	metricsMarshaler pmetric.JSONMarshaler

	traceBatches []json.RawMessage

	logger *zap.Logger
}

func newJSONBatchingMarshaler(maxBatchSize int, logger *zap.Logger) *jsonBatchingMarshaler {
	return &jsonBatchingMarshaler{
		maxBatchSize:     maxBatchSize,
		logBatches:       make([]json.RawMessage, 0),
		uncompressedSize: 0,
		logsMarshaler:    plog.JSONMarshaler{},
		tracesMarshaler:  ptrace.JSONMarshaler{},
		metricsMarshaler: pmetric.JSONMarshaler{},
		traceBatches:     make([]json.RawMessage, 0),
		logger:           logger,
	}
}

func (m *jsonBatchingMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data, err := m.logsMarshaler.MarshalLogs(ld)
	if err != nil {
		return nil, err
	}

	m.logsBatchCount++
	m.logBatches = append(m.logBatches, json.RawMessage(data))
	m.uncompressedSize += len(data)

	if m.uncompressedSize >= m.maxBatchSize {
		return m.flushLogBatch(), nil
	}

	return []byte{}, nil
}

func createJSONL(batches []json.RawMessage) []byte {
	if len(batches) == 0 {
		return []byte("")
	}

	var result bytes.Buffer
	for i, batch := range batches {
		if i > 0 {
			result.WriteByte('\n')
		}
		result.Write(batch)
	}
	result.WriteByte('\n')
	return result.Bytes()
}

func (m *jsonBatchingMarshaler) flushLogBatch() []byte {
	if m.logsBatchCount == 0 {
		return nil
	}

	jsonlData := createJSONL(m.logBatches)

	if m.logger != nil {
		m.logger.Debug("Flushed uncompressed JSONL log batch",
			zap.Int("batch_count", m.logsBatchCount),
			zap.Int("uncompressed_size", len(jsonlData)))
	}

	m.logBatches = m.logBatches[:0]
	m.logsBatchCount = 0
	m.uncompressedSize = 0

	return jsonlData
}

func (m *jsonBatchingMarshaler) FlushLogs() ([]byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.flushLogBatch(), nil
}

func (m *jsonBatchingMarshaler) MarshalTraces(td ptrace.Traces) ([]byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	buf, err := m.tracesMarshaler.MarshalTraces(td)
	if err != nil {
		return nil, err
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(buf, &parsed); err != nil {
		return nil, err
	}

	resourceSpans, ok := parsed["resourceSpans"]
	if !ok {
		return nil, nil
	}

	var spans []json.RawMessage
	if err := json.Unmarshal(resourceSpans, &spans); err != nil {
		return nil, err
	}

	for _, span := range spans {
		m.traceBatches = append(m.traceBatches, span)
		m.tracesBatchCount++
		m.uncompressedSize += len(span)
	}

	if m.maxBatchSize > 0 && m.uncompressedSize >= m.maxBatchSize {
		return m.flushTraceBatch()
	}

	return []byte{}, nil
}

func (m *jsonBatchingMarshaler) flushTraceBatch() ([]byte, error) {
	if m.tracesBatchCount == 0 {
		return nil, nil
	}

	wrapped := make([]json.RawMessage, 0, len(m.traceBatches))
	for _, span := range m.traceBatches {
		envelope, err := json.Marshal(map[string][]json.RawMessage{
			"resourceSpans": {span},
		})
		if err != nil {
			return nil, err
		}
		wrapped = append(wrapped, envelope)
	}

	jsonlData := createJSONL(wrapped)

	if m.logger != nil {
		m.logger.Debug("Flushed uncompressed JSONL trace batch",
			zap.Int("batch_count", m.tracesBatchCount),
			zap.Int("uncompressed_size", len(jsonlData)))
	}

	m.traceBatches = m.traceBatches[:0]
	m.tracesBatchCount = 0
	m.uncompressedSize = 0

	return jsonlData, nil
}

func (m *jsonBatchingMarshaler) FlushTraces() ([]byte, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.flushTraceBatch()
}

func (m *jsonBatchingMarshaler) MarshalMetrics(md pmetric.Metrics) ([]byte, error) {
	return m.metricsMarshaler.MarshalMetrics(md)
}

func (*jsonBatchingMarshaler) Format() string {
	return "json"
}

func (*jsonBatchingMarshaler) Compressed() bool {
	return false
}
