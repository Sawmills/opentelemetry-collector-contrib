// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package rejectionrepair preserves a rejected field in a flat JSON-string field.
package rejectionrepair

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"
)

// Relocate keeps the full original JSON value before removing its original key.
// The caller must first prove that Elasticsearch rejected this field on this item.
// Ambiguous input or a destination collision returns the original document.
func Relocate(document []byte, field string) ([]byte, bool) {
	if !utf8.Valid(document) || !rootField(field) {
		return document, false
	}
	fields, ok := uniqueObject(document)
	if !ok {
		return document, false
	}
	value, exists := fields[field]
	if !exists {
		return document, false
	}
	backup := "sawmills_original_" + field + "_json"
	for key := range fields {
		if key == backup || strings.HasPrefix(key, backup+".") || strings.HasPrefix(key, field+".") {
			return document, false
		}
	}
	preserved, err := json.Marshal(struct {
		Key       string `json:"key"`
		ValueType string `json:"value_type"`
		ValueJSON string `json:"value_json"`
	}{field, valueType(value), string(value)})
	if err != nil {
		return document, false
	}
	fields[backup], err = json.Marshal(string(preserved))
	if err != nil {
		return document, false
	}
	delete(fields, field)
	repaired, err := json.Marshal(fields)
	if err != nil {
		return document, false
	}
	return repaired, true
}

func uniqueObject(document []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := token.(string)
		if !ok || strings.ContainsRune(key, utf8.RuneError) {
			return nil, false
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, false
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF {
		return nil, false
	}
	return fields, true
}

func rootField(field string) bool {
	if len(field) == 0 || len(field) > 128 || strings.HasPrefix(field, "sawmills_original_") {
		return false
	}
	for _, c := range field {
		if c != '_' && c != '-' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func valueType(value json.RawMessage) string {
	switch value[0] {
	case '"':
		return "string"
	case '{':
		return "object"
	case '[':
		return "array"
	case 't', 'f':
		return "boolean"
	case 'n':
		return "null"
	default:
		return "number"
	}
}
