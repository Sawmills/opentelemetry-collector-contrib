// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflake

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestConvertToParquetJSONStringBodyProducesSnowflakeRow(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newJSONBodyLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row, ok := rows[0].(ParquetLog)
	require.True(t, ok)

	require.Equal(t, int64(1700000000000), row.TS)
	require.Equal(t, "checkout", row.Service)
	require.Equal(t, "error", row.Status)
	require.Equal(t, "hello", row.MessageText)
	require.Equal(t, "host-1", row.Host)
	require.Equal(t, "nodejs", row.Source)
	require.Equal(t, "snowflake_v1", row.SchemaVersion)
	require.Equal(t, "0102030405060708090a0b0c0d0e0f10", row.TraceID)
	require.Equal(t, "0102030405060708", row.SpanID)

	require.NotNil(t, row.BodyJSONText)
	require.JSONEq(t, `{"a":1,"b":2,"message":"hello"}`, *row.BodyJSONText)

	var hotAttrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.AttributesHotText), &hotAttrs))
	require.Empty(t, hotAttrs)

	var coldAttrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.AttributesColdText), &coldAttrs))
	require.Equal(t, "12345", coldAttrs["customer.id"])
	require.Equal(t, "tx-789", coldAttrs["transaction.id"])
	require.Equal(t, "checkout", coldAttrs["service"])
	require.Equal(t, "host-1", coldAttrs["hostname"])
	require.Equal(t, "nodejs", coldAttrs["source"])
	require.Equal(t, "error", coldAttrs["status"])
	require.Equal(t, "req-123", coldAttrs["request.id"])

	var hotTags map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.TagsHotText), &hotTags))
	require.Equal(t, "prod", hotTags["env"])
	require.Equal(t, "1.2.3", hotTags["version"])

	var coldTags map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.TagsColdText), &coldTags))
	require.Equal(t, "checkout", coldTags["service"])
}

func TestConvertToParquetPlainStringBodyLeavesBodyJSONNil(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newPlainStringLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row, ok := rows[0].(ParquetLog)
	require.True(t, ok)

	require.Nil(t, row.BodyJSONText)
	require.Equal(t, "plain body", row.MessageText)
	require.Equal(t, "warn", row.Status)
	require.JSONEq(t, `{}`, row.AttributesHotText)
	require.JSONEq(t, `{}`, row.TagsHotText)
}

func TestConvertToParquetMapBodyCanonicalizesBodyJSON(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newMapBodyLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row, ok := rows[0].(ParquetLog)
	require.True(t, ok)

	require.NotNil(t, row.BodyJSONText)
	require.JSONEq(t, `{"error":{"message":"nested hello"},"z":1}`, *row.BodyJSONText)
	require.Equal(t, "nested hello", row.MessageText)
}

func TestConvertToParquetWarnsOnceForZeroTimestampDrops(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	settings := extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding"))
	settings.Logger = zap.New(core)

	var dropCount int64
	adapter, err := NewSnowflakeParquetAdapter(settings, Config{
		RecordDropFn: func(reason string, count int64) {
			require.Equal(t, "row_drop_invalid_ts", reason)
			dropCount += count
		},
	})
	require.NoError(t, err)

	logs := newPlainStringLogs()
	resourceLogs := logs.ResourceLogs().At(0)
	scopeLogs := resourceLogs.ScopeLogs().At(0)

	firstDropped := scopeLogs.LogRecords().AppendEmpty()
	firstDropped.Body().SetStr("dropped-1")

	secondDropped := scopeLogs.LogRecords().AppendEmpty()
	secondDropped.Body().SetStr("dropped-2")

	rows, err := adapter.ConvertToParquet(t.Context(), logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	entries := observed.All()
	require.Len(t, entries, 1)
	require.Equal(t, "dropped log records with zero timestamp", entries[0].Message)
	require.Equal(t, int64(2), entries[0].ContextMap()["count"])
	require.Equal(t, int64(2), dropCount)
}

func TestConvertToParquetSanitizesColdAttributes(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newLargeAttributeLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	var coldAttrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.AttributesColdText), &coldAttrs))

	require.NotContains(t, coldAttrs, "k8s.pod.uid")
	require.NotContains(t, coldAttrs, "k8s.pod.ip")
	require.NotContains(t, coldAttrs, "k8s.node.uid")
	require.Len(t, coldAttrs, maxArchivedColdAttributes)
	require.Len(t, coldAttrs["000payload"].(string), maxArchivedStringValueSize)
}

func TestConvertToParquetSanitizesColdTags(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newLargeTagLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	var coldTags map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.TagsColdText), &coldTags))

	require.NotContains(t, coldTags, "k8s.pod.uid")
	require.NotContains(t, coldTags, "k8s.pod.ip")
	require.NotContains(t, coldTags, "k8s.node.uid")
	require.Len(t, coldTags, maxArchivedColdAttributes)
	require.Len(t, coldTags["000payload"].(string), maxArchivedStringValueSize)
}

func TestTagsToMapRejectsNonFiniteNumbers(t *testing.T) {
	require.Equal(t, map[string]any{
		"env":   "prod",
		"value": "NaN",
		"peak":  "+Inf",
	}, tagsToMap([]string{"env:prod", "value:NaN", "peak:+Inf"}))
}

func TestTagsToMapPreservesLargeIntegerPrecision(t *testing.T) {
	require.Equal(t, map[string]any{
		"id": json.Number("9007199254740993"),
	}, tagsToMap([]string{"id:9007199254740993"}))
}

func TestNormalizeJSONNumberPreservesRepresentableFloat(t *testing.T) {
	value, err := normalizeJSONNumber(json.Number("1.5"))
	require.NoError(t, err)
	require.Equal(t, 1.5, value)
}

func TestNormalizeJSONNumberFallsBackForPrecisionSensitiveFloat(t *testing.T) {
	value, err := normalizeJSONNumber(json.Number("9007199254740993.0"))
	require.NoError(t, err)
	require.Equal(t, "9007199254740993.0", value)
}

func TestResourceFlattenedAttributesDoNotOverrideRecordAttributes(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newAttributePrecedenceLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	var coldAttrs map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.AttributesColdText), &coldAttrs))
	require.Equal(t, "record-value", coldAttrs["cloud.region"])
}

func TestConvertToParquetUsesConfiguredHotKeys(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{
			AttributesHotKeys: []string{"customer.id"},
			TagsHotKeys:       []string{"service"},
		},
	)
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(t.Context(), newJSONBodyLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	require.JSONEq(t, `{"customer.id":"12345"}`, row.AttributesHotText)
	require.JSONEq(t, `{"host.name":"host-1","hostname":"host-1","request.id":"req-123","service":"checkout","service.name":"checkout","source":"nodejs","status":"error","transaction.id":"tx-789"}`, row.AttributesColdText)
	require.JSONEq(t, `{"service":"checkout"}`, row.TagsHotText)
	require.JSONEq(t, `{"env":"prod","version":"1.2.3"}`, row.TagsColdText)
}

func TestConvertToParquetStringifiesNonFiniteOTLPDoubles(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(
		extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")),
		Config{},
	)
	require.NoError(t, err)

	logs := newPlainStringLogs()
	record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	nanValue, err := strconv.ParseFloat("NaN", 64)
	require.NoError(t, err)
	infValue, err := strconv.ParseFloat("+Inf", 64)
	require.NoError(t, err)
	record.Attributes().PutDouble("float.nan", nanValue)
	logs.ResourceLogs().At(0).Resource().Attributes().PutDouble("float.inf", infValue)

	rows, err := adapter.ConvertToParquet(t.Context(), logs)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	require.JSONEq(t, `{"float.inf":"+Inf","float.nan":"NaN","status":"warn"}`, row.AttributesColdText)
}

func TestSanitizeArchivedValuePreservesValidUTF8(t *testing.T) {
	value := strings.Repeat("é", (maxArchivedStringValueSize/2)+2)
	truncated := sanitizeArchivedValue(value).(string)
	require.True(t, utf8.ValidString(truncated))
	require.LessOrEqual(t, len(truncated), maxArchivedStringValueSize)
}

func newJSONBodyLogs() plog.Logs {
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

func newPlainStringLogs() plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	record := scopeLogs.LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000100, 0)))
	record.SetSeverityText("WARN")
	record.Body().SetStr("plain body")

	return logs
}

func newMapBodyLogs() plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()

	record := scopeLogs.LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1700000200, 0)))
	record.SetSeverityText("INFO")
	body := record.Body().SetEmptyMap()
	body.PutInt("z", 1)
	errMap := body.PutEmptyMap("error")
	errMap.PutStr("message", "nested hello")

	return logs
}

func newLargeAttributeLogs() plog.Logs {
	logs := newPlainStringLogs()
	record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	record.Attributes().PutStr("k8s.pod.uid", "pod-uid")
	record.Attributes().PutStr("k8s.pod.ip", "10.0.0.1")
	record.Attributes().PutStr("k8s.node.uid", "node-uid")
	record.Attributes().PutStr("000payload", strings.Repeat("x", maxArchivedStringValueSize+32))
	for i := range maxArchivedColdAttributes + 5 {
		record.Attributes().PutStr("attr."+strconv.Itoa(i), "value")
	}
	return logs
}

func newAttributePrecedenceLogs() plog.Logs {
	logs := newPlainStringLogs()
	resource := logs.ResourceLogs().At(0).Resource()
	cloud := resource.Attributes().PutEmptyMap("cloud")
	cloud.PutStr("region", "resource-value")

	record := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	record.Attributes().PutStr("cloud.region", "record-value")
	return logs
}

func newLargeTagLogs() plog.Logs {
	logs := newPlainStringLogs()
	resource := logs.ResourceLogs().At(0).Resource()
	ddtags := resource.Attributes().PutEmptyMap("ddtags")
	ddtags.PutStr("k8s.pod.uid", "pod-uid")
	ddtags.PutStr("k8s.pod.ip", "10.0.0.1")
	ddtags.PutStr("k8s.node.uid", "node-uid")
	ddtags.PutStr("000payload", strings.Repeat("x", maxArchivedStringValueSize+32))
	for i := range maxArchivedColdAttributes + 5 {
		ddtags.PutStr("tag."+strconv.Itoa(i), "value")
	}
	return logs
}
