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
	assert.Equal(t, "", reason)
	assert.True(t, completedAt.IsZero())
}

func TestShutdownWithBufferedDataReturnsErrorAndPreservesBuffer(t *testing.T) {
	ext := newTestParquetExtension(t)
	writeDatadogLogs(t, ext, 1)

	err := ext.Shutdown(context.Background())
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
	assert.Equal(t, "", reason)
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
	assert.Equal(t, "", reason)
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

func newTestParquetExtension(t *testing.T) *parquetLogExtension {
	t.Helper()

	ext, err := NewParquetLogExtension(
		context.Background(),
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

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "checkout")
	resourceLogs.Resource().Attributes().PutStr("host.name", "host-1")
	ddtags := resourceLogs.Resource().Attributes().PutEmptyMap("ddtags")
	ddtags.PutStr("env", "prod")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	for i := 0; i < count; i++ {
		record := scopeLogs.LogRecords().AppendEmpty()
		record.Body().SetStr("checkout log")
		record.SetSeverityText("ERROR")
		record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000000+int64(i), 0)))
		record.Attributes().PutStr("ddsource", "nodejs")
		record.Attributes().PutStr("request.id", "req-123")
	}

	_, err := ext.MarshalLogs(logs)
	require.NoError(t, err)
}
