// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension

import (
	"context"
	"errors"
	"strings"
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

func TestShutdownWithQueuedFlushesReturnsError(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.queuedFlushes = append(ext.queuedFlushes, flushResult{buf: []byte("queued")})

	err := ext.Shutdown(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "queued")
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
	assert.Len(t, ext.writer.Objs, 0)
	assert.Len(t, ext.queuedFlushes, 1)
	assert.True(t, ext.oldestBufferedRecord.IsZero())
	assert.Zero(t, ext.bufferOldestRecordAgeSeconds())
}

func TestMarshalLogsQueuesAdditionalSizeFlushesFromSameBatch(t *testing.T) {
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
	assert.Len(t, ext.queuedFlushes, 2)
	assert.Len(t, ext.writer.Objs, 0)

	secondBuf, secondReason, _, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, secondBuf)
	assert.Equal(t, flushReasonSize, secondReason)
	assert.Len(t, ext.queuedFlushes, 1)

	thirdBuf, thirdReason, _, err := ext.FlushLogsWithReasonAndMetadata(flushReasonManual)
	require.NoError(t, err)
	require.NotEmpty(t, thirdBuf)
	assert.Equal(t, flushReasonSize, thirdReason)
	assert.Empty(t, ext.queuedFlushes)
}

func TestPopQueuedFlushClearsRetainedBufferReference(t *testing.T) {
	ext := newTestParquetExtension(t)
	ext.queuedFlushes = []flushResult{
		{buf: []byte("first")},
		{buf: []byte("second")},
	}

	_, ok := ext.popQueuedFlushLocked()
	require.True(t, ok)
	require.Len(t, ext.queuedFlushes, 1)
	assert.Equal(t, "second", string(ext.queuedFlushes[0].buf))
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
		"",
		extensiontest.NewNopSettings(testExtensionType),
	)
	require.NoError(t, err)

	require.IsType(t, &datadog.ParquetLog{}, adapter.Schema())
}

func TestNewParquetAdapterSnowflakeRoutesToSnowflakeAdapter(t *testing.T) {
	adapter, err := newParquetAdapter(
		"snowflake",
		extensiontest.NewNopSettings(testExtensionType),
	)
	require.NoError(t, err)

	require.IsType(t, &snowflake.ParquetLog{}, adapter.Schema())
}

func TestNewParquetAdapterRejectsUnknownSchema(t *testing.T) {
	adapter, err := newParquetAdapter(
		"custom",
		extensiontest.NewNopSettings(testExtensionType),
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
