// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters/datadog"
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

	buf, reason, completedAt, err := ext.addLogRecordWithFlushMetadata([]any{
		datadog.ParquetLog{Message: "large enough"},
		datadog.ParquetLog{Message: "buffer after flush"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	assert.Equal(t, flushReasonSize, reason)
	assert.False(t, completedAt.IsZero())
	assert.Len(t, ext.writer.Objs, 1)
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
