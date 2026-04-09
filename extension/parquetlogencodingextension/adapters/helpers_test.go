// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapters

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestStatusFromSeverityNumber(t *testing.T) {
	require.Empty(t, StatusFromSeverityNumber(plog.SeverityNumber(0)))
	require.Equal(t, "trace", StatusFromSeverityNumber(plog.SeverityNumberTrace4))
	require.Equal(t, "debug", StatusFromSeverityNumber(plog.SeverityNumberDebug4))
	require.Equal(t, "info", StatusFromSeverityNumber(plog.SeverityNumberInfo4))
	require.Equal(t, "warn", StatusFromSeverityNumber(plog.SeverityNumberWarn4))
	require.Equal(t, "error", StatusFromSeverityNumber(plog.SeverityNumberError4))
	require.Equal(t, "fatal", StatusFromSeverityNumber(plog.SeverityNumberFatal4))
	require.Equal(t, "error", StatusFromSeverityNumber(plog.SeverityNumber(99)))
}

func TestTagsToMap(t *testing.T) {
	tagMap := TagsToMap([]string{" enabled:true ", "count:123", "label:value"}, true, func(value string) (any, bool) {
		if value == "123" {
			return json.Number(value), true
		}
		return nil, false
	})

	require.Equal(t, true, tagMap["enabled"])
	require.Equal(t, json.Number("123"), tagMap["count"])
	require.Equal(t, "value", tagMap["label"])
}
