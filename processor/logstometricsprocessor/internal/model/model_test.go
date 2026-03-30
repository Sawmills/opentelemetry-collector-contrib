// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type StrMap map[string]any

// buildMap creates a pcommon.Map from a StrMap.
// Values can be strings, numbers, or nested maps (StrMap).
func buildMap(data StrMap) pcommon.Map {
	m := pcommon.NewMap()
	populateMap(m, data)
	return m
}

// populateMap recursively populates a pcommon.Map from StrMap.
func populateMap(m pcommon.Map, data StrMap) {
	for k, v := range data {
		switch val := v.(type) {
		case string:
			m.PutStr(k, val)
		case int:
			m.PutInt(k, int64(val))
		case int64:
			m.PutInt(k, val)
		case float64:
			m.PutDouble(k, val)
		case bool:
			m.PutBool(k, val)
		case StrMap:
			nested := m.PutEmptyMap(k)
			populateMap(nested, val)
		}
	}
}

func TestFilterAttributes_FlatKeys(t *testing.T) {
	tests := []struct {
		name         string
		attrs        StrMap
		filters      []AttributeKeyValue
		wantResult   bool
		wantFiltered StrMap
		description  string
	}{
		{
			name: "single flat key found",
			attrs: StrMap{
				"service": "api",
			},
			filters: []AttributeKeyValue{
				{Key: "service", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"service": "api",
			},
			description: "Backward compatibility: flat key lookup",
		},
		{
			name: "multiple flat keys all found",
			attrs: StrMap{
				"service": "api",
				"env":     "prod",
				"version": "1.0",
			},
			filters: []AttributeKeyValue{
				{Key: "service", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "env", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "version", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"service": "api",
				"env":     "prod",
				"version": "1.0",
			},
			description: "Multiple flat keys",
		},
		{
			name: "flat key not found - required",
			attrs: StrMap{
				"service": "api",
			},
			filters: []AttributeKeyValue{
				{Key: "missing", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   false,
			wantFiltered: nil,
			description:  "Missing required flat key should return false",
		},
		{
			name: "flat key not found - optional",
			attrs: StrMap{
				"service": "api",
			},
			filters: []AttributeKeyValue{
				{Key: "missing", Optional: true, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   true,
			wantFiltered: StrMap{},
			description:  "Missing optional flat key should return true with empty map",
		},
		{
			name: "flat key with default value",
			attrs: StrMap{
				"service": "api",
			},
			filters: []AttributeKeyValue{
				{
					Key:          "missing",
					Optional:     false,
					DefaultValue: pcommon.NewValueStr("default"),
				},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"missing": "default",
			},
			description: "Missing key with default value should use default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := buildMap(tt.attrs)
			md := &MetricDef[any]{
				Attributes: tt.filters,
			}

			result, ok := md.FilterAttributes(attrs)

			assert.Equal(t, tt.wantResult, ok, tt.description)
			if tt.wantResult {
				assert.Equal(t, len(tt.wantFiltered), result.Len())
				for k, v := range tt.wantFiltered {
					actualVal, found := result.Get(k)
					assert.True(t, found, "key %s should be present", k)
					switch expectedVal := v.(type) {
					case string:
						assert.Equal(t, expectedVal, actualVal.Str())
					case int:
						assert.Equal(t, int64(expectedVal), actualVal.Int())
					case int64:
						assert.Equal(t, expectedVal, actualVal.Int())
					case float64:
						assert.Equal(t, expectedVal, actualVal.Double())
					case bool:
						assert.Equal(t, expectedVal, actualVal.Bool())
					}
				}
			} else {
				// When ok is false, result is an empty/invalid map, don't call methods on it
				assert.False(t, ok, "should return false when required key is missing")
			}
		})
	}
}

func TestFilterAttributes_NestedKeys(t *testing.T) {
	tests := []struct {
		name         string
		attrs        StrMap
		filters      []AttributeKeyValue
		wantResult   bool
		wantFiltered StrMap
		description  string
	}{
		{
			name: "nested key k1.k2 found",
			attrs: StrMap{
				"k1": StrMap{
					"k2": "value",
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.k2": "value",
			},
			description: "Nested key with dot notation",
		},
		{
			name: "multi-level nested key k1.k2.k3 found",
			attrs: StrMap{
				"k1": StrMap{
					"k2": StrMap{
						"k3": "deep_value",
					},
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2.k3", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.k2.k3": "deep_value",
			},
			description: "Multi-level nested key",
		},
		{
			name: "mixed flat and nested keys",
			attrs: StrMap{
				"service": "api",
				"k1": StrMap{
					"k2": "nested_value",
				},
			},
			filters: []AttributeKeyValue{
				{Key: "service", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k1.k2", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"service": "api",
				"k1.k2":   "nested_value",
			},
			description: "Mixed flat and nested keys",
		},
		{
			name: "nested key not found - required",
			attrs: StrMap{
				"k1": StrMap{
					"k2": "value",
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k3", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   false,
			wantFiltered: nil,
			description:  "Missing nested key should return false",
		},
		{
			name: "nested key not found - optional",
			attrs: StrMap{
				"k1": StrMap{
					"k2": "value",
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k3", Optional: true, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   true,
			wantFiltered: StrMap{},
			description:  "Missing optional nested key should return true",
		},
		{
			name: "nested key with default value",
			attrs: StrMap{
				"k1": StrMap{
					"k2": "value",
				},
			},
			filters: []AttributeKeyValue{
				{
					Key:          "k1.k3",
					Optional:     false,
					DefaultValue: pcommon.NewValueStr("default_nested"),
				},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.k3": "default_nested",
			},
			description: "Missing nested key with default value",
		},
		{
			name: "flat key preferred over nested when both exist",
			attrs: StrMap{
				"k1.k2": "flat_value",
				"k1": StrMap{
					"k2": "nested_value",
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.k2": "flat_value",
			},
			description: "Flat key should be preferred over nested when both exist (backward compatibility)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := buildMap(tt.attrs)
			md := &MetricDef[any]{
				Attributes: tt.filters,
			}

			result, ok := md.FilterAttributes(attrs)

			assert.Equal(t, tt.wantResult, ok, tt.description)
			if tt.wantResult {
				assert.Equal(t, len(tt.wantFiltered), result.Len())
				for k, v := range tt.wantFiltered {
					actualVal, found := result.Get(k)
					assert.True(t, found, "key %s should be present", k)
					switch expectedVal := v.(type) {
					case string:
						assert.Equal(t, expectedVal, actualVal.Str())
					case int:
						assert.Equal(t, int64(expectedVal), actualVal.Int())
					case int64:
						assert.Equal(t, expectedVal, actualVal.Int())
					case float64:
						assert.Equal(t, expectedVal, actualVal.Double())
					case bool:
						assert.Equal(t, expectedVal, actualVal.Bool())
					}
				}
			} else {
				// When ok is false, result is an empty/invalid map, don't call methods on it
				assert.False(t, ok, "should return false when required key is missing")
			}
		})
	}
}

func TestFilterAttributes_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		attrs        StrMap
		filters      []AttributeKeyValue
		wantResult   bool
		wantFiltered StrMap
		description  string
	}{
		{
			name: "escaped dots in key",
			attrs: StrMap{
				"k1.k2": "flat_value_with_dot",
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.k2": "flat_value_with_dot",
			},
			description: "Flat key with dot should be found (backward compatibility - flat lookup first)",
		},
		{
			name: "key points to non-map value in nested path",
			attrs: StrMap{
				"k1": "not_a_map",
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   false,
			wantFiltered: nil,
			description:  "Key pointing to non-map value in nested path should return false",
		},
		{
			name: "empty attributes list",
			attrs: StrMap{
				"service": "api",
			},
			filters:      []AttributeKeyValue{},
			wantResult:   true,
			wantFiltered: StrMap{},
			description:  "Empty filters should return empty map",
		},
		{
			name: "nested key with different value types",
			attrs: StrMap{
				"k1": StrMap{
					"str":   "string_value",
					"int":   42,
					"float": 3.14,
					"bool":  true,
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.str", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k1.int", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k1.float", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k1.bool", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"k1.str":   "string_value",
				"k1.int":   42,
				"k1.float": 3.14,
				"k1.bool":  true,
			},
			description: "Nested keys with different value types",
		},
		{
			name: "partial nested path exists but final key missing",
			attrs: StrMap{
				"k1": StrMap{
					"k2": StrMap{
						"k3": "value",
					},
				},
			},
			filters: []AttributeKeyValue{
				{Key: "k1.k2.k4", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult:   false,
			wantFiltered: nil,
			description:  "Partial nested path exists but final key is missing",
		},
		{
			name: "multiple nested keys at different levels",
			attrs: StrMap{
				"level1": StrMap{
					"level2a": "value2a",
					"level2b": StrMap{
						"level3": "value3",
					},
				},
			},
			filters: []AttributeKeyValue{
				{Key: "level1.level2a", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{
					Key:          "level1.level2b.level3",
					Optional:     false,
					DefaultValue: pcommon.NewValueEmpty(),
				},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"level1.level2a":        "value2a",
				"level1.level2b.level3": "value3",
			},
			description: "Multiple nested keys at different levels",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := buildMap(tt.attrs)
			md := &MetricDef[any]{
				Attributes: tt.filters,
			}

			result, ok := md.FilterAttributes(attrs)

			assert.Equal(t, tt.wantResult, ok, tt.description)
			if tt.wantResult {
				assert.Equal(t, len(tt.wantFiltered), result.Len())
				for k, v := range tt.wantFiltered {
					actualVal, found := result.Get(k)
					require.True(t, found, "key %s should be present", k)
					switch expectedVal := v.(type) {
					case string:
						assert.Equal(t, expectedVal, actualVal.Str())
					case int:
						assert.Equal(t, int64(expectedVal), actualVal.Int())
					case int64:
						assert.Equal(t, expectedVal, actualVal.Int())
					case float64:
						assert.InDelta(t, expectedVal, actualVal.Double(), 0.001)
					case bool:
						assert.Equal(t, expectedVal, actualVal.Bool())
					}
				}
			} else {
				// When ok is false, result is an empty/invalid map, don't call methods on it
				assert.False(t, ok, "should return false when required key is missing")
			}
		})
	}
}

func TestFilterAttributes_RealWorldScenarios(t *testing.T) {
	tests := []struct {
		name         string
		attrs        StrMap
		filters      []AttributeKeyValue
		wantResult   bool
		wantFiltered StrMap
		description  string
	}{
		{
			name: "kubernetes pod metadata",
			attrs: StrMap{
				"k8s": StrMap{
					"pod": StrMap{
						"name":      "my-pod",
						"namespace": "default",
					},
				},
				"service": "api",
			},
			filters: []AttributeKeyValue{
				{Key: "service", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k8s.pod.name", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{Key: "k8s.pod.namespace", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"service":           "api",
				"k8s.pod.name":      "my-pod",
				"k8s.pod.namespace": "default",
			},
			description: "Real-world scenario: Kubernetes pod metadata",
		},
		{
			name: "http request metadata",
			attrs: StrMap{
				"http": StrMap{
					"request": StrMap{
						"method": "GET",
						"path":   "/api/v1/users",
					},
					"response": StrMap{
						"status_code": 200,
					},
				},
			},
			filters: []AttributeKeyValue{
				{
					Key:          "http.request.method",
					Optional:     false,
					DefaultValue: pcommon.NewValueEmpty(),
				},
				{Key: "http.request.path", Optional: false, DefaultValue: pcommon.NewValueEmpty()},
				{
					Key:          "http.response.status_code",
					Optional:     false,
					DefaultValue: pcommon.NewValueEmpty(),
				},
			},
			wantResult: true,
			wantFiltered: StrMap{
				"http.request.method":       "GET",
				"http.request.path":         "/api/v1/users",
				"http.response.status_code": 200,
			},
			description: "Real-world scenario: HTTP request metadata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := buildMap(tt.attrs)
			md := &MetricDef[any]{
				Attributes: tt.filters,
			}

			result, ok := md.FilterAttributes(attrs)

			assert.Equal(t, tt.wantResult, ok, tt.description)
			if tt.wantResult {
				assert.Equal(t, len(tt.wantFiltered), result.Len())
				for k, v := range tt.wantFiltered {
					actualVal, found := result.Get(k)
					require.True(t, found, "key %s should be present", k)
					switch expectedVal := v.(type) {
					case string:
						assert.Equal(t, expectedVal, actualVal.Str())
					case int:
						assert.Equal(t, int64(expectedVal), actualVal.Int())
					case int64:
						assert.Equal(t, expectedVal, actualVal.Int())
					case float64:
						assert.InDelta(t, expectedVal, actualVal.Double(), 0.001)
					case bool:
						assert.Equal(t, expectedVal, actualVal.Bool())
					}
				}
			} else {
				// When ok is false, result is an empty/invalid map, don't call methods on it
				assert.False(t, ok, "should return false when required key is missing")
			}
		})
	}
}
