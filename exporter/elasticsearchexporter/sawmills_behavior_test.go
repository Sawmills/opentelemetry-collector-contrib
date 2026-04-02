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
	record.Attributes().PutStr("from_attr", "attr")
	record.Attributes().PutStr("overlap", "attr-wins")
	body := record.Body().SetEmptyMap()
	body.PutStr("from_body", "body")
	body.PutStr("overlap", "body-loses")

	mergeBodyMapIntoLogAttributes(record)

	value, ok := record.Attributes().Get("from_body")
	require.True(t, ok)
	assert.Equal(t, "body", value.Str())

	value, ok = record.Attributes().Get("from_attr")
	require.True(t, ok)
	assert.Equal(t, "attr", value.Str())

	value, ok = record.Attributes().Get("overlap")
	require.True(t, ok)
	assert.Equal(t, "attr-wins", value.Str())
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
}

func TestMergeMapsByPriority_FirstMapWins(t *testing.T) {
	first := pcommon.NewMap()
	first.PutStr("overlap", "from-first")
	first.PutStr("only_first", "first")

	second := pcommon.NewMap()
	second.PutStr("overlap", "from-second")
	second.PutStr("only_second", "second")

	merged := mergeMapsByPriority(second, first)

	value, ok := merged.Get("overlap")
	require.True(t, ok)
	assert.Equal(t, "from-first", value.Str())

	value, ok = merged.Get("only_first")
	require.True(t, ok)
	assert.Equal(t, "first", value.Str())

	value, ok = merged.Get("only_second")
	require.True(t, ok)
	assert.Equal(t, "second", value.Str())
}
