// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestMergeBodyMapIntoLogAttributes(t *testing.T) {
	record := plog.NewLogRecord()
	record.Attributes().PutStr("exception.message", "attr-wins")
	body := record.Body().SetEmptyMap()
	body.PutStr("event.name", "body")
	body.PutStr("exception.message", "body-loses")
	body.PutStr("userinfo.username", "blocked")

	mergeBodyMapIntoLogAttributes(record)

	value, ok := record.Attributes().Get("event.name")
	require.True(t, ok)
	assert.Equal(t, "body", value.Str())

	value, ok = record.Attributes().Get("exception.message")
	require.True(t, ok)
	assert.Equal(t, "attr-wins", value.Str())

	_, ok = record.Attributes().Get("userinfo.username")
	assert.False(t, ok)

	body.PutEmptyMap("nested").PutStr("key", "value")
	mergeBodyMapIntoLogAttributes(record)
	_, ok = record.Attributes().Get("nested")
	assert.False(t, ok)
}

func TestPreprocessLogsForSawmills_RemovesSawmillsService(t *testing.T) {
	logs := plog.NewLogs()
	resource := logs.ResourceLogs().AppendEmpty().Resource().Attributes().PutEmptyMap("sawmills")
	resource.PutStr("service", "resource-service")
	resource.PutStr("keep", "resource-keep")

	record := logs.ResourceLogs().At(0).ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	recordSawmills := record.Attributes().PutEmptyMap("sawmills")
	recordSawmills.PutStr("service", "record-service")
	recordSawmills.PutStr("keep", "record-keep")

	processed := preprocessLogsForSawmills(logs)
	gotResource := processed.ResourceLogs().At(0).Resource().Attributes()
	gotRecord := processed.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()

	resourceMap, ok := gotResource.Get("sawmills")
	require.True(t, ok)
	_, exists := resourceMap.Map().Get("service")
	assert.False(t, exists)
	value, exists := resourceMap.Map().Get("keep")
	require.True(t, exists)
	assert.Equal(t, "resource-keep", value.Str())

	recordMap, ok := gotRecord.Get("sawmills")
	require.True(t, ok)
	_, exists = recordMap.Map().Get("service")
	assert.False(t, exists)
	value, exists = recordMap.Map().Get("keep")
	require.True(t, exists)
	assert.Equal(t, "record-keep", value.Str())

	originalRecordMap, ok := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("sawmills")
	require.True(t, ok)
	value, exists = originalRecordMap.Map().Get("service")
	require.True(t, exists)
	assert.Equal(t, "record-service", value.Str())
}

func TestPreprocessLogsForSawmills_RemovesEmptySawmillsMap(t *testing.T) {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Attributes().PutEmptyMap("sawmills").PutStr("service", "record-service")

	processed := preprocessLogsForSawmills(logs)
	_, ok := processed.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("sawmills")
	assert.False(t, ok)
}

func TestPreprocessLogsForSawmills_SkipsCopyWhenNoServiceKey(t *testing.T) {
	logs := plog.NewLogs()
	logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes().PutStr("plain", "value")

	processed := preprocessLogsForSawmills(logs)
	processed.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().PutStr("shared", "yes")

	value, ok := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("shared")
	require.True(t, ok)
	assert.Equal(t, "yes", value.Str())
}

func TestMergeMapsByPriority_HigherPriorityWins(t *testing.T) {
	first := pcommon.NewMap()
	first.PutStr("exception.message", "from-first")
	first.PutStr("event.name", "first")
	first.PutEmptyMap("nested_from_first").PutStr("kept", "value")
	first.PutEmpty("drop_lower")

	second := pcommon.NewMap()
	second.PutStr("exception.message", "from-second")
	second.PutStr("event.name", "second")
	second.PutStr("userinfo.username", "blocked")
	second.PutEmptyMap("nested_from_second").PutStr("dropped", "value")
	second.PutStr("drop_lower", "second")

	merged := mergeMapsByPriority(second, first)

	value, ok := merged.Get("exception.message")
	require.True(t, ok)
	assert.Equal(t, "from-first", value.Str())

	value, ok = merged.Get("event.name")
	require.True(t, ok)
	assert.Equal(t, "first", value.Str())

	value, ok = merged.Get("nested_from_first")
	require.True(t, ok)
	nestedValue, exists := value.Map().Get("kept")
	require.True(t, exists)
	assert.Equal(t, "value", nestedValue.Str())

	_, ok = merged.Get("drop_lower")
	assert.False(t, ok)

	_, ok = merged.Get("userinfo.username")
	assert.False(t, ok)

	_, ok = merged.Get("nested_from_second")
	assert.False(t, ok)
}
