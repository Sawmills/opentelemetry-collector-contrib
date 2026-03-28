// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type keyType int

const (
	keyTypeInvalid keyType = iota
	keyTypeFlat
	keyTypeHier
)

// splitFieldName splits dot-separated keys while preserving escaped dots.
func splitFieldName(name string) []string {
	temp := strings.ReplaceAll(name, `\\.`, "\x00")
	parts := strings.Split(temp, ".")
	for i := range parts {
		parts[i] = strings.ReplaceAll(parts[i], "\x00", ".")
	}
	return parts
}

func getKeyValue(valueMap pcommon.Map, keyParts []string) (pcommon.Value, bool) {
	if len(keyParts) == 0 {
		return pcommon.NewValueEmpty(), false
	}

	nextKeyPart, remainingParts := keyParts[0], keyParts[1:]
	value, ok := valueMap.Get(nextKeyPart)
	if !ok {
		return pcommon.NewValueEmpty(), false
	}

	if len(remainingParts) == 0 {
		return valueMap.Get(nextKeyPart)
	}

	if value.Type() == pcommon.ValueTypeMap {
		return getKeyValue(value.Map(), remainingParts)
	}

	return pcommon.NewValueEmpty(), false
}

func getFieldValue2Keys(valueMap pcommon.Map, keyParts []string, flatKey string) (pcommon.Value, keyType, bool) {
	if value, ok := valueMap.Get(flatKey); ok {
		return value, keyTypeFlat, true
	}

	if value, ok := getKeyValue(valueMap, keyParts); ok {
		return value, keyTypeHier, true
	}

	return pcommon.NewValueEmpty(), keyTypeInvalid, false
}
