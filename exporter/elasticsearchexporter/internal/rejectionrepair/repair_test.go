// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rejectionrepair

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRelocatePreservesRejectedStringAndUnrelatedLargeInteger(t *testing.T) {
	original := []byte(`{"level":"WARN","other":9007199254740993}`)
	want := `{"other":9007199254740993,"sawmills_original_level_json":"{\"key\":\"level\",\"value_type\":\"string\",\"value_json\":\"\\\"WARN\\\"\"}"}`

	got, changed := Relocate(original, "level")

	if !changed || string(got) != want {
		t.Fatalf("lossless relocation: changed=%v, got=%s, want=%s", changed, got, want)
	}
	if string(original) != `{"level":"WARN","other":9007199254740993}` {
		t.Fatal("input was mutated")
	}
}

func TestRelocatePreservesNumberLexemeAndType(t *testing.T) {
	original := []byte(`{"timestamp":-0.000e+100,"untouched":"text"}`)

	got, changed := Relocate(original, "timestamp")

	var fields map[string]json.RawMessage
	if !changed || json.Unmarshal(got, &fields) != nil {
		t.Fatal("expected repaired document")
	}
	var backup string
	if json.Unmarshal(fields["sawmills_original_timestamp_json"], &backup) != nil || backup != `{"key":"timestamp","value_type":"number","value_json":"-0.000e+100"}` {
		t.Fatalf("original JSON number was not preserved: %s", backup)
	}
}

func TestRelocateRefusesDuplicateKeys(t *testing.T) {
	original := []byte(`{"level":"WARN","level":"ERROR"}`)

	got, changed := Relocate(original, "level")

	if changed || !bytes.Equal(got, original) {
		t.Fatal("ambiguous duplicate keys must remain untouched")
	}
}

func assertUnchanged(t *testing.T, original []byte, field string) {
	t.Helper()
	got, changed := Relocate(original, field)
	if changed || !bytes.Equal(got, original) {
		t.Fatal("unsafe relocation must return the original bytes")
	}
}

func TestRelocateRefusesExistingBackup(t *testing.T) {
	assertUnchanged(t, []byte(`{"level":"WARN","sawmills_original_level_json":null}`), "level")
}

func TestRelocateRefusesDottedBackupCollision(t *testing.T) {
	assertUnchanged(t, []byte(`{"level":"WARN","sawmills_original_level_json.child":"keep"}`), "level")
}

func TestRelocateRefusesAmbiguousDottedSource(t *testing.T) {
	assertUnchanged(t, []byte(`{"level":"WARN","level.child":"keep"}`), "level")
}

func TestRelocateRefusesUnboundNestedPath(t *testing.T) {
	assertUnchanged(t, []byte(`{"level.child":"WARN"}`), "level.child")
}

func TestRelocateRefusesGeneratedTimestamp(t *testing.T) {
	assertUnchanged(t, []byte(`{"@timestamp":"bad"}`), "@timestamp")
}

func TestRelocateRefusesMalformedUTF8(t *testing.T) {
	assertUnchanged(t, append([]byte(`{"level":"`), 0xff, '"', '}'), "level")
}

func TestRelocateRefusesNormalizedKey(t *testing.T) {
	assertUnchanged(t, []byte(`{"level":"WARN","\ud800":"keep"}`), "level")
}

func TestRelocateRefusesTrailingDocument(t *testing.T) {
	assertUnchanged(t, []byte(`{"level":"WARN"}{"keep":true}`), "level")
}

func TestRelocateRefusesMissingField(t *testing.T) {
	assertUnchanged(t, []byte(`{"other":"keep"}`), "level")
}

func TestRelocatePreservesCompositeRawValue(t *testing.T) {
	original := []byte(`{"error":{"a":[true,null,9007199254740993],"b":"\\\"\ud800<>&"}}`)
	want := `{"a":[true,null,9007199254740993],"b":"\\\"\ud800<>&"}`

	got, changed := Relocate(original, "error")

	var fields map[string]json.RawMessage
	var backup string
	if !changed || json.Unmarshal(got, &fields) != nil || json.Unmarshal(fields["sawmills_original_error_json"], &backup) != nil {
		t.Fatal("expected lossless backup")
	}
	var entry map[string]string
	if json.Unmarshal([]byte(backup), &entry) != nil || entry["key"] != "error" || entry["value_type"] != "object" || entry["value_json"] != want {
		t.Fatal("composite raw JSON changed")
	}
}
