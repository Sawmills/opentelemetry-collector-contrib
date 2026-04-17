// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func newTestLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host", "myhost")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("hello world")
	return ld
}

func newTestTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	return td
}

func TestMarshalLogsAccumulatesAndFlushReturnsBatchedJSONL(t *testing.T) {
	m := newJSONBatchingMarshaler(1<<20, zap.NewNop()) // large batch size, won't auto-flush

	// First call — accumulates, returns empty.
	buf, err := m.MarshalLogs(newTestLogs())
	require.NoError(t, err)
	assert.Empty(t, buf)

	// Second call — still accumulates.
	buf, err = m.MarshalLogs(newTestLogs())
	require.NoError(t, err)
	assert.Empty(t, buf)

	// FlushLogs returns the accumulated JSONL.
	flushed, err := m.FlushLogs()
	require.NoError(t, err)
	require.NotNil(t, flushed)

	lines := strings.Split(strings.TrimSuffix(string(flushed), "\n"), "\n")
	assert.Len(t, lines, 2, "expected 2 JSONL lines for 2 accumulated log records")

	// Each line should be valid JSON with the resourceLogs envelope.
	for _, line := range lines {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(line), &doc))
		assert.Contains(t, doc, "resourceLogs", "each JSONL line should have resourceLogs envelope")
	}
}

func TestMarshalTracesAccumulatesAndFlushReturnsBatchedJSONL(t *testing.T) {
	m := newJSONBatchingMarshaler(1<<20, zap.NewNop())

	buf, err := m.MarshalTraces(newTestTraces())
	require.NoError(t, err)
	assert.Empty(t, buf)

	buf, err = m.MarshalTraces(newTestTraces())
	require.NoError(t, err)
	assert.Empty(t, buf)

	flushed, err := m.FlushTraces()
	require.NoError(t, err)
	require.NotNil(t, flushed)

	lines := strings.Split(strings.TrimSuffix(string(flushed), "\n"), "\n")
	assert.Len(t, lines, 2, "expected 2 JSONL lines for 2 accumulated traces")

	for _, line := range lines {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(line), &doc))
		assert.Contains(t, doc, "resourceSpans", "each JSONL line should have resourceSpans envelope")
	}
}

func TestFlushLogsReturnsNilWhenNoData(t *testing.T) {
	m := newJSONBatchingMarshaler(1<<20, zap.NewNop())

	flushed, err := m.FlushLogs()
	require.NoError(t, err)
	assert.Nil(t, flushed)
}

func TestFlushTracesReturnsNilWhenNoData(t *testing.T) {
	m := newJSONBatchingMarshaler(1<<20, zap.NewNop())

	flushed, err := m.FlushTraces()
	require.NoError(t, err)
	assert.Nil(t, flushed)
}

func TestMaxBatchSizeTriggersAutomaticFlush(t *testing.T) {
	// Use a tiny max batch size so that a single MarshalLogs call triggers
	// an automatic flush.
	m := newJSONBatchingMarshaler(1, zap.NewNop())

	buf, err := m.MarshalLogs(newTestLogs())
	require.NoError(t, err)
	require.NotEmpty(t, buf, "should auto-flush when maxBatchSize exceeded")

	lines := strings.Split(strings.TrimSuffix(string(buf), "\n"), "\n")
	assert.Len(t, lines, 1)

	// After auto-flush, FlushLogs should return nil (buffer drained).
	flushed, err := m.FlushLogs()
	require.NoError(t, err)
	assert.Nil(t, flushed)
}

func TestMaxBatchSizeTriggersAutomaticFlushTraces(t *testing.T) {
	m := newJSONBatchingMarshaler(1, zap.NewNop())

	buf, err := m.MarshalTraces(newTestTraces())
	require.NoError(t, err)
	require.NotEmpty(t, buf, "should auto-flush when maxBatchSize exceeded")

	lines := strings.Split(strings.TrimSuffix(string(buf), "\n"), "\n")
	assert.Len(t, lines, 1)

	flushed, err := m.FlushTraces()
	require.NoError(t, err)
	assert.Nil(t, flushed)
}
