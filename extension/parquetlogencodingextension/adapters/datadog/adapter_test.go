package datadog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestConvertToParquetScopesRecordFallbacksPerSibling(t *testing.T) {
	adapter, err := NewDatadogParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
	)
	require.NoError(t, err)

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	first := scopeLogs.LogRecords().AppendEmpty()
	first.Body().SetStr("first")
	first.Attributes().PutStr("host.name", "host-a")
	first.Attributes().PutStr("service.name", "svc-a")

	second := scopeLogs.LogRecords().AppendEmpty()
	second.Body().SetStr("second")
	second.Attributes().PutStr("host.name", "host-b")
	second.Attributes().PutStr("service.name", "svc-b")

	records, err := adapter.ConvertToParquet(context.Background(), logs)
	require.NoError(t, err)
	require.Len(t, records, 2)

	firstRow, ok := records[0].(ParquetLog)
	require.True(t, ok)
	secondRow, ok := records[1].(ParquetLog)
	require.True(t, ok)

	require.Equal(t, "host-a", firstRow.Host)
	require.Equal(t, "svc-a", firstRow.Service)
	require.Equal(t, "host-b", secondRow.Host)
	require.Equal(t, "svc-b", secondRow.Service)
}
