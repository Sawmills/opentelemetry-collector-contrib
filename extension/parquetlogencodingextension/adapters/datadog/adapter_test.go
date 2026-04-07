// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datadog

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
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

	records, err := adapter.ConvertToParquet(t.Context(), logs)
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

func TestConvertToParquetResourceAttributesDoNotOverrideRecordAttributes(t *testing.T) {
	adapter, err := NewDatadogParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
	)
	require.NoError(t, err)

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "resource-service")
	resourceLogs.Resource().Attributes().PutStr("request.id", "resource-request")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	record := scopeLogs.LogRecords().AppendEmpty()
	record.Body().SetStr("fallback path")
	record.Attributes().PutStr("service.name", "record-service")
	record.Attributes().PutStr("request.id", "record-request")

	records, err := adapter.ConvertToParquet(t.Context(), logs)
	require.NoError(t, err)
	require.Len(t, records, 1)

	row, ok := records[0].(ParquetLog)
	require.True(t, ok)

	var attrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.Attributes), &attrs))
	assert.Equal(t, "record-service", attrs["service.name"])
	assert.Equal(t, "record-request", attrs["request.id"])
}

func TestConvertToParquetDdtagsResourceAttributesDoNotOverrideFlattenedRecordAttributes(t *testing.T) {
	adapter, err := NewDatadogParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
	)
	require.NoError(t, err)

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	ddtags := resourceLogs.Resource().Attributes().PutEmptyMap("ddtags")
	ddtags.PutStr("env", "prod")
	resourceLogs.Resource().Attributes().PutStr("request.id", "resource-request")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	record := scopeLogs.LogRecords().AppendEmpty()
	record.Body().SetStr("ddtags path")
	record.Attributes().PutEmptyMap("request").PutStr("id", "record-request")

	records, err := adapter.ConvertToParquet(t.Context(), logs)
	require.NoError(t, err)
	require.Len(t, records, 1)

	row, ok := records[0].(ParquetLog)
	require.True(t, ok)

	var attrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.Attributes), &attrs))
	assert.Equal(t, "record-request", attrs["request.id"])
}

func TestTagsToMapPreservesWhitespaceForBackwardCompatibility(t *testing.T) {
	got := tagsToMap([]string{" env : value "})

	require.Len(t, got, 1)
	assert.Equal(t, " value ", got[" env "])
}
