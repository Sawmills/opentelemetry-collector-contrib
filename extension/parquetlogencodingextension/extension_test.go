// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters/datadog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters/snowflake"
)

var testExtensionType = component.MustNewType("parquet_log_encoding")

func TestFlushLogsWithReasonAndMetadata(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 3)

	buf, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata("manual")
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, "manual", reason)
	assert.False(t, completedAt.IsZero())
}

func TestFlushRespectsEmptyBuffer(t *testing.T) {
	ext := newTestParquetExtension(t)

	buf, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata("shutdown")
	require.NoError(t, err)
	assert.Nil(t, buf)
	assert.Empty(t, reason)
	assert.True(t, completedAt.IsZero())
}

func TestShutdownWithBufferedDataReturnsErrorAndPreservesBuffer(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 1)

	err := ext.Shutdown(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffered")
	assert.Len(t, ext.writer.Objs, 1)
}

func TestShutdownWithPendingRecordsReturnsErrorAndMovesSpillIntoBuffer(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.config.CompressionCodec = "uncompressed"
	ext.maxFileSizeBytes = 1
	largeMessage := strings.Repeat("x", 4096)

	buf, reason, _, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: largeMessage + "1"},
		datadog.ParquetLog{Message: largeMessage + "2"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonSize, reason)
	assert.Len(t, ext.pendingRecords, 1)

	err = ext.Shutdown(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffered parquet records remain at shutdown")
	assert.Empty(t, ext.pendingRecords)
	assert.Len(t, ext.writer.Objs, 1)
}

func TestShutdownWithPendingRecordsWriteFailureRequeuesOnlyUnwrittenTail(t *testing.T) {
	ext := newTestParquetExtension(t)
	now := time.Unix(100, 0)
	ext.nowFn = func() time.Time { return now }
	ext.pendingRecords = []any{
		datadog.ParquetLog{Message: "first"},
		datadog.ParquetLog{Message: "second"},
		datadog.ParquetLog{Message: "third"},
	}
	ext.oldestPendingRecord = now.Add(-time.Minute)

	writes := 0
	ext.writeRecordFn = func(record any) error {
		writes++
		if writes == 3 {
			return errors.New("boom")
		}
		return ext.Write(record)
	}

	err := ext.Shutdown(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write pending record")
	require.Len(t, ext.writer.Objs, 2)
	assert.Equal(t, datadog.ParquetLog{Message: "first"}, ext.writer.Objs[0])
	assert.Equal(t, datadog.ParquetLog{Message: "second"}, ext.writer.Objs[1])
	require.Len(t, ext.pendingRecords, 1)
	assert.Equal(t, datadog.ParquetLog{Message: "third"}, ext.pendingRecords[0])
	assert.Equal(t, now.Add(-time.Minute), ext.oldestPendingRecord)
}

func TestShutdownPendingWriteFailureStopsBufferStateLoop(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.bufferStateInterval = 5 * time.Millisecond
	require.NoError(t, ext.Start(t.Context(), nil))

	now := time.Unix(100, 0)
	ext.nowFn = func() time.Time { return now }
	ext.pendingRecords = []any{
		datadog.ParquetLog{Message: "first"},
		datadog.ParquetLog{Message: "second"},
	}

	writes := 0
	ext.writeRecordFn = func(record any) error {
		writes++
		if writes == 2 {
			return errors.New("boom")
		}
		return ext.Write(record)
	}

	err := ext.Shutdown(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "move pending parquet spill payloads into buffer")
	assert.Nil(t, ext.bufferStateTickerStop)
	assert.Nil(t, ext.bufferStateTickerDone)
}

func TestAddLogRecordWriteFailureRequeuesFailedRecordAndTail(t *testing.T) {
	ext := newTestParquetExtension(t)
	now := time.Unix(100, 0)
	ext.nowFn = func() time.Time { return now }

	writes := 0
	ext.writeRecordFn = func(record any) error {
		writes++
		if writes == 2 {
			return errors.New("boom")
		}
		return ext.Write(record)
	}

	_, _, _, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: "first"},
		datadog.ParquetLog{Message: "second"},
		datadog.ParquetLog{Message: "third"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write record")
	require.Len(t, ext.writer.Objs, 1)
	assert.Equal(t, datadog.ParquetLog{Message: "first"}, ext.writer.Objs[0])
	require.Len(t, ext.pendingRecords, 2)
	assert.Equal(t, datadog.ParquetLog{Message: "second"}, ext.pendingRecords[0])
	assert.Equal(t, datadog.ParquetLog{Message: "third"}, ext.pendingRecords[1])
	assert.Equal(t, now, ext.oldestPendingRecord)
	assert.NotZero(t, ext.pendingEstimatedBytes)
}

func TestAddLogRecordWriteFailureRefreshesBufferStateAfterRequeue(t *testing.T) {
	ext := newTestParquetExtension(t)
	now := time.Unix(100, 0)
	ext.nowFn = func() time.Time { return now }

	var gotRecords int
	var gotEstimatedBytes int64
	ext.recordBufferStateFn = func(records int, estimatedBytes, oldestRecordAge int64) {
		_ = oldestRecordAge
		gotRecords = records
		gotEstimatedBytes = estimatedBytes
	}

	writes := 0
	ext.writeRecordFn = func(record any) error {
		writes++
		if writes == 2 {
			return errors.New("boom")
		}
		return ext.Write(record)
	}

	_, _, _, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: "first"},
		datadog.ParquetLog{Message: "second"},
		datadog.ParquetLog{Message: "third"},
	})
	require.Error(t, err)
	assert.Equal(t, 3, gotRecords)
	assert.NotZero(t, gotEstimatedBytes)
}

func TestShutdownErrorLeavesBufferStateLoopRunning(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.bufferStateInterval = 5 * time.Millisecond

	current := time.Unix(100, 0)
	var currentUnix atomic.Int64
	currentUnix.Store(current.UnixNano())
	ext.nowFn = func() time.Time { return time.Unix(0, currentUnix.Load()) }
	writeDatadogLogs(t, ext, 1)

	recordAges := make(chan int64, 8)
	ext.recordBufferStateFn = func(records int, estimatedBytes, oldestRecordAge int64) {
		_ = records
		_ = estimatedBytes
		recordAges <- oldestRecordAge
	}

	require.NoError(t, ext.Start(t.Context(), nil))
	t.Cleanup(ext.stopBufferStateLoop)

	err := ext.Shutdown(t.Context())
	require.Error(t, err)
	require.NotNil(t, ext.bufferStateTickerStop)

	current = current.Add(7 * time.Second)
	currentUnix.Store(current.UnixNano())
	require.Eventually(t, func() bool {
		select {
		case age := <-recordAges:
			return age >= 7
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestFlushWriteStopFailurePreservesBufferedStateForRetry(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 2)

	originalWriteStop := ext.writeStopFn
	ext.writeStopFn = func() error {
		_ = originalWriteStop()
		return errors.New("write stop failed")
	}

	buf, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata("manual")
	require.Error(t, err)
	assert.Nil(t, buf)
	assert.Empty(t, reason)
	assert.True(t, completedAt.IsZero())
	assert.Len(t, ext.writer.Objs, 2)

	ext.writeStopFn = ext.defaultWriteStop
	retriedBuf, retriedReason, retriedCompletedAt, retriedErr := ext.FlushLogsWithReasonAndMetadata(
		"manual",
	)
	require.NoError(t, retriedErr)
	require.NotEmpty(t, retriedBuf)
	assert.Equal(t, "manual", retriedReason)
	assert.False(t, retriedCompletedAt.IsZero())
}

func TestFlushReadFailurePreservesBufferedStateForRetry(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 2)

	ext.readWriterBytes = func() ([]byte, error) {
		return nil, errors.New("read bytes failed")
	}

	buf, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata("manual")
	require.Error(t, err)
	assert.Nil(t, buf)
	assert.Empty(t, reason)
	assert.True(t, completedAt.IsZero())
	assert.Len(t, ext.writer.Objs, 2)

	ext.readWriterBytes = ext.defaultReadWriterBytes
	retriedBuf, retriedReason, retriedCompletedAt, retriedErr := ext.FlushLogsWithReasonAndMetadata(
		"manual",
	)
	require.NoError(t, retriedErr)
	require.NotEmpty(t, retriedBuf)
	assert.Equal(t, "manual", retriedReason)
	assert.False(t, retriedCompletedAt.IsZero())
}

func TestMarshalLogsUsesFlushTimeNotBufferAgeForFlushDuration(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 1)

	ext.oldestBufferedRecord = time.Unix(100, 0)
	now := time.Unix(200, 0)
	ext.nowFn = func() time.Time { return now }

	var recordedDuration time.Duration
	ext.recordFlushFn = func(_ string, _, _ int64, d time.Duration) {
		recordedDuration = d
	}

	_, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	assert.Equal(t, flushReasonManual, reason)
	assert.Equal(t, now, completedAt)
	assert.Zero(t, recordedDuration)
}

func TestStartRefreshesBufferedRecordAgeWhileIdle(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.bufferStateInterval = 5 * time.Millisecond

	current := time.Unix(100, 0)
	var currentUnix atomic.Int64
	currentUnix.Store(current.UnixNano())
	ext.nowFn = func() time.Time { return time.Unix(0, currentUnix.Load()) }
	writeDatadogLogs(t, ext, 1)

	recordAges := make(chan int64, 8)
	ext.recordBufferStateFn = func(records int, estimatedBytes, oldestRecordAge int64) {
		_ = records
		_ = estimatedBytes
		recordAges <- oldestRecordAge
	}

	require.NoError(t, ext.Start(t.Context(), nil))
	defer ext.stopBufferStateLoop()

	current = current.Add(7 * time.Second)
	currentUnix.Store(current.UnixNano())
	require.Eventually(t, func() bool {
		select {
		case age := <-recordAges:
			return age >= 7
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestRecordBufferStateIncludesPendingRecords(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.config.CompressionCodec = "uncompressed"
	ext.maxFileSizeBytes = 1
	largeMessage := strings.Repeat("x", 4096)

	var gotRecords int
	var gotEstimatedBytes int64
	ext.recordBufferStateFn = func(records int, estimatedBytes, oldestRecordAge int64) {
		gotRecords = records
		gotEstimatedBytes = estimatedBytes
		_ = oldestRecordAge
	}

	_, _, _, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: largeMessage + "1"},
		datadog.ParquetLog{Message: largeMessage + "2"},
		datadog.ParquetLog{Message: largeMessage + "3"},
	})
	require.NoError(t, err)

	require.Len(t, ext.pendingRecords, 2)
	require.Equal(t, 2, gotRecords)
	require.NotZero(t, gotEstimatedBytes)
}

func TestRecordBufferStateUsesPendingOldestRecordAge(t *testing.T) {
	ext := newTestParquetExtension(t)
	now := time.Unix(200, 0)
	ext.nowFn = func() time.Time { return now }
	ext.pendingRecords = []any{datadog.ParquetLog{Message: "queued"}}
	ext.oldestPendingRecord = time.Unix(150, 0)
	ext.pendingEstimatedBytes = 42

	var gotRecords int
	var gotEstimatedBytes int64
	var gotOldestRecordAge int64
	ext.recordBufferStateFn = func(records int, estimatedBytes, oldestRecordAge int64) {
		gotRecords = records
		gotEstimatedBytes = estimatedBytes
		gotOldestRecordAge = oldestRecordAge
	}

	ext.mutex.Lock()
	ext.recordBufferStateLocked()
	ext.mutex.Unlock()

	require.Equal(t, 1, gotRecords)
	require.Equal(t, int64(42), gotEstimatedBytes)
	require.Equal(t, int64(50), gotOldestRecordAge)
}

func TestMarshalLogsFlushesImmediatelyWhenSizeThresholdIsExceeded(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.maxFileSizeBytes = 1
	now := time.Unix(200, 0)
	ext.nowFn = func() time.Time {
		now = now.Add(time.Second)
		return now
	}

	buf, reason, completedAt, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: "large enough"},
		datadog.ParquetLog{Message: "buffer after flush"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonSize, reason)
	assert.False(t, completedAt.IsZero())
	assert.Empty(t, ext.writer.Objs)
	assert.Len(t, ext.pendingRecords, 1)
	assert.True(t, ext.oldestBufferedRecord.IsZero())
}

func TestMarshalLogsQueuesRemainingRecordsAfterSizeFlush(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.config.CompressionCodec = "uncompressed"
	ext.maxFileSizeBytes = 1
	largeMessage := strings.Repeat("x", 4096)

	buf, reason, completedAt, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: largeMessage + "1"},
		datadog.ParquetLog{Message: largeMessage + "2"},
		datadog.ParquetLog{Message: largeMessage + "3"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonSize, reason)
	assert.False(t, completedAt.IsZero())
	assert.Empty(t, ext.writer.Objs)
	assert.Len(t, ext.pendingRecords, 2)

	secondBuf, secondReason, _, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, secondBuf)
	assert.Equal(t, flushReasonManual, secondReason)
	assert.Empty(t, ext.writer.Objs)
	assert.Len(t, ext.pendingRecords, 1)

	thirdBuf, thirdReason, _, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, thirdBuf)
	assert.Equal(t, flushReasonManual, thirdReason)
	assert.Empty(t, ext.writer.Objs)
	assert.Empty(t, ext.pendingRecords)
}

func TestFlushLogsDrainsPendingRecords(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.config.CompressionCodec = "uncompressed"
	ext.maxFileSizeBytes = 1
	largeMessage := strings.Repeat("x", 4096)

	buf, reason, _, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: largeMessage + "1"},
		datadog.ParquetLog{Message: largeMessage + "2"},
		datadog.ParquetLog{Message: largeMessage + "3"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonSize, reason)
	assert.Len(t, ext.pendingRecords, 2)

	secondBuf, err := ext.FlushLogs()
	require.NoError(t, err)
	require.NotEmpty(t, secondBuf)
	assert.Len(t, ext.pendingRecords, 1)

	thirdBuf, err := ext.FlushLogs()
	require.NoError(t, err)
	require.NotEmpty(t, thirdBuf)
	assert.Empty(t, ext.pendingRecords)
}

func TestSplitFlushFailurePreservesPendingTailForRetry(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.config.CompressionCodec = "uncompressed"
	ext.maxFileSizeBytes = 1
	largeMessage := strings.Repeat("x", 4096)

	originalWriteStop := ext.writeStopFn
	ext.writeStopFn = func() error {
		_ = originalWriteStop()
		return errors.New("split flush failed")
	}

	buf, reason, completedAt, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: largeMessage + "1"},
		datadog.ParquetLog{Message: largeMessage + "2"},
		datadog.ParquetLog{Message: largeMessage + "3"},
	})
	require.Error(t, err)
	assert.Nil(t, buf)
	assert.Empty(t, reason)
	assert.True(t, completedAt.IsZero())
	assert.Len(t, ext.writer.Objs, 1)
	assert.Len(t, ext.pendingRecords, 2)

	ext.writeStopFn = ext.defaultWriteStop
	retriedBuf, retriedReason, _, retriedErr := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, retriedErr)
	require.NotEmpty(t, retriedBuf)
	assert.Equal(t, flushReasonManual, retriedReason)
	assert.Len(t, ext.pendingRecords, 1)
}

func TestFlushReinitializeFailureReturnsPayloadAndRepairsOnNextWrite(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 2)

	ext.reinitializeWriterFn = func() error {
		return errors.New("reinitialize failed")
	}

	buf, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonManual, reason)
	assert.False(t, completedAt.IsZero())
	assert.Nil(t, ext.writer)

	_, _, _, err = ext.addLogRecordWithFlushMetadata([]any{datadog.ParquetLog{Message: "needs repair"}})
	require.Error(t, err)

	ext.reinitializeWriterFn = ext.initializeWriter
	_, _, _, err = ext.addLogRecordWithFlushMetadata([]any{datadog.ParquetLog{Message: "recovers"}})
	require.NoError(t, err)
	require.NotNil(t, ext.writer)
	assert.Len(t, ext.writer.Objs, 1)
}

func TestMarshalLogsIgnoresCanceledFactoryContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ext, err := NewParquetLogExtension(
		ctx,
		extensiontest.NewNopSettings(testExtensionType),
		&Config{
			MaxFileSizeBytes:   defaultMaxFileSizeBytes,
			NumberOfGoRoutines: defaultNumberOfGoRoutines,
		},
	)
	require.NoError(t, err)

	_, err = ext.(*parquetLogExtension).MarshalLogs(newDatadogLogs(1))
	require.NoError(t, err)
}

func TestNewParquetAdapterDefaultsToDatadog(t *testing.T) {
	adapter, err := newParquetAdapter(
		&Config{},
		extensiontest.NewNopSettings(testExtensionType),
		nil,
	)
	require.NoError(t, err)

	require.IsType(t, &datadog.ParquetLog{}, adapter.Schema())
}

func TestNewParquetAdapterSnowflakeRoutesToSnowflakeAdapter(t *testing.T) {
	adapter, err := newParquetAdapter(
		&Config{Schema: "snowflake"},
		extensiontest.NewNopSettings(testExtensionType),
		nil,
	)
	require.NoError(t, err)

	require.IsType(t, &snowflake.ParquetLog{}, adapter.Schema())
}

func TestNewParquetAdapterRejectsUnknownSchema(t *testing.T) {
	adapter, err := newParquetAdapter(
		&Config{Schema: "custom"},
		extensiontest.NewNopSettings(testExtensionType),
		nil,
	)
	require.Error(t, err)
	require.Nil(t, adapter)
}

func TestMarshalLogsSnowflakeSchemaFlushReadback(t *testing.T) {
	ext := newSnowflakeTestParquetExtension(t)

	buf, err := ext.MarshalLogs(snowflakeJSONBodyLogs())
	require.NoError(t, err)
	require.Nil(t, buf)

	pqBytes, reason, completedAt, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, pqBytes)
	require.Equal(t, flushReasonManual, reason)
	require.False(t, completedAt.IsZero())

	pqFile, err := buffer.NewBufferFile(pqBytes)
	require.NoError(t, err)

	pr, err := reader.NewParquetReader(pqFile, new(snowflake.ParquetLog), 1)
	require.NoError(t, err)
	defer func() {
		pr.ReadStop()
	}()

	require.Equal(t, int64(1), pr.GetNumRows())
	var schemaNames []string
	for _, info := range pr.SchemaHandler.Infos {
		schemaNames = append(schemaNames, info.ExName)
	}
	require.Equal(t, []string{
		"parquet_go_root",
		"ts",
		"service",
		"status",
		"message_text",
		"host",
		"source",
		"trace_id",
		"span_id",
		"schema_version",
		"body_json_text",
		"attributes_hot_text",
		"attributes_cold_text",
		"tags_hot_text",
		"tags_cold_text",
	}, schemaNames)

	rows := make([]snowflake.ParquetLog, 1)
	require.NoError(t, pr.Read(&rows))

	row := rows[0]
	require.Equal(t, int64(1700000000000), row.TS)
	require.Equal(t, "checkout", row.Service)
	require.Equal(t, "error", row.Status)
	require.Equal(t, "hello", row.MessageText)
	require.Equal(t, "host-1", row.Host)
	require.Equal(t, "nodejs", row.Source)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", row.TraceID)
	require.Equal(t, "0102030405060708", row.SpanID)
	require.Equal(t, "snowflake_v1", row.SchemaVersion)
	require.NotNil(t, row.BodyJSONText)
	require.JSONEq(t, `{"a":1,"b":2,"message":"hello"}`, *row.BodyJSONText)
	require.JSONEq(t, `{}`, row.AttributesHotText)
	require.JSONEq(t, `{"customer.id":"12345","host.name":"host-1","hostname":"host-1","request.id":"req-123","service":"checkout","service.name":"checkout","source":"nodejs","status":"error","transaction.id":"tx-789"}`, row.AttributesColdText)
	require.JSONEq(t, `{"env":"prod","version":"1.2.3"}`, row.TagsHotText)
	require.JSONEq(t, `{"service":"checkout"}`, row.TagsColdText)
}

func newTestParquetExtension(t *testing.T) *parquetLogExtension {
	t.Helper()

	ext, err := NewParquetLogExtension(
		t.Context(),
		extensiontest.NewNopSettings(testExtensionType),
		&Config{
			MaxFileSizeBytes:   defaultMaxFileSizeBytes,
			NumberOfGoRoutines: defaultNumberOfGoRoutines,
		},
	)
	require.NoError(t, err)

	return ext.(*parquetLogExtension)
}

func newSnowflakeTestParquetExtension(t *testing.T) *parquetLogExtension {
	t.Helper()

	ext, err := NewParquetLogExtension(
		t.Context(),
		extensiontest.NewNopSettings(testExtensionType),
		&Config{
			Schema:             "snowflake",
			MaxFileSizeBytes:   defaultMaxFileSizeBytes,
			NumberOfGoRoutines: defaultNumberOfGoRoutines,
			CompressionCodec:   defaultCompressionCodec,
			PageSizeBytes:      defaultPageSizeBytes,
			RowGroupSizeBytes:  defaultRowGroupSizeBytes,
		},
	)
	require.NoError(t, err)

	return ext.(*parquetLogExtension)
}

func writeDatadogLogs(t *testing.T, ext *parquetLogExtension, count int) {
	t.Helper()

	_, err := ext.MarshalLogs(newDatadogLogs(count))
	require.NoError(t, err)
}

func newDatadogLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "checkout")
	resourceLogs.Resource().Attributes().PutStr("host.name", "host-1")
	ddtags := resourceLogs.Resource().Attributes().PutEmptyMap("ddtags")
	ddtags.PutStr("env", "prod")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	for i := range count {
		record := scopeLogs.LogRecords().AppendEmpty()
		record.Body().SetStr("checkout log")
		record.SetSeverityText("ERROR")
		record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000+int64(i), 0)))
		record.Attributes().PutStr("ddsource", "nodejs")
		record.Attributes().PutStr("request.id", "req-123")
	}

	return logs
}

func snowflakeJSONBodyLogs() plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "checkout")
	resourceLogs.Resource().Attributes().PutStr("host.name", "host-1")
	ddtags := resourceLogs.Resource().Attributes().PutEmptyMap("ddtags")
	ddtags.PutStr("env", "prod")
	ddtags.PutStr("version", "1.2.3")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	record := scopeLogs.LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000, 0)))
	record.SetSeverityText("ERROR")
	record.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	record.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	record.Body().SetStr(`{"b":2,"message":"hello","a":1}`)
	record.Attributes().PutStr("ddsource", "nodejs")
	record.Attributes().PutStr("customer.id", "12345")
	record.Attributes().PutStr("transaction.id", "tx-789")
	record.Attributes().PutStr("request.id", "req-123")

	return logs
}
