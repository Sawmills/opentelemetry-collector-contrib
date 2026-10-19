// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rejectionrepair

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func proofEnvelope(t *testing.T, envelope string) []byte {
	t.Helper()
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal("synthetic envelope encoding failed")
	}
	return []byte(`{"sawmills_original_error_json":` + string(encoded) + `}`)
}

func TestProofRejectsAmbiguousEvidence(t *testing.T) {
	original := []byte(`{"error":-0.000e+100}`)
	for _, test := range []struct {
		name, envelope string
	}{
		{"null_key", `{"key":null,"value_type":"number","value_json":"-0.000e+100"}`},
		{"null_type", `{"key":"error","value_type":null,"value_json":"-0.000e+100"}`},
		{"null_raw", `{"key":"error","value_type":"number","value_json":null}`},
		{"duplicate", `{"key":"error","key":"other","value_type":"number","value_json":"-0.000e+100"}`},
		{"missing", `{"key":"error","value_type":"number"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var proof Proof
			if got := proof.Observe(original, proofEnvelope(t, test.envelope), "error"); got != Unverified {
				t.Fatalf("ambiguous evidence must be unverified, got outcome %d", got)
			}
		})
	}
}

func TestProofMatchesExactRawValueAndType(t *testing.T) {
	for _, test := range []struct {
		name, value, kind string
	}{
		{"number", `-0.000e+100`, "number"},
		{"large_integer", `9007199254740993`, "number"},
		{"object", `{ "nested": [1, 2] }`, "object"},
		{"array", `[true, null]`, "array"},
		{"string", `"synthetic"`, "string"},
		{"boolean", `true`, "boolean"},
		{"null", `null`, "null"},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := json.Marshal(map[string]string{"key": "error", "value_type": test.kind, "value_json": test.value})
			if err != nil {
				t.Fatal("synthetic envelope encoding failed")
			}
			var proof Proof
			if got := proof.Observe([]byte(`{"error":`+test.value+`}`), proofEnvelope(t, string(envelope)), "error"); got != Equal {
				t.Fatalf("exact original must be equal, got outcome %d", got)
			}
		})
	}
}

func TestProofCountsMismatchWithoutReturningValues(t *testing.T) {
	original := []byte(`{"error":9007199254740993}`)
	for _, envelope := range []string{
		`{"key":"wrong","value_type":"number","value_json":"9007199254740993"}`,
		`{"key":"error","value_type":"string","value_json":"9007199254740993"}`,
		`{"key":"error","value_type":"number","value_json":"9007199254740992"}`,
	} {
		var proof Proof
		if got := proof.Observe(original, proofEnvelope(t, envelope), "error"); got != Unequal {
			t.Fatalf("changed backup must be unequal, got outcome %d", got)
		}
	}
}

func TestProofBoundsOversizedEvidenceAndConcurrentBudget(t *testing.T) {
	var proof Proof
	var unverified, exhausted atomic.Int64
	original := []byte(`{"error":"` + strings.Repeat("x", 1<<20) + `"}`)
	var workers sync.WaitGroup
	for range 100 {
		workers.Go(func() {
			switch proof.Observe(original, nil, "error") {
			case Unverified:
				unverified.Add(1)
			case NotSampled:
				exhausted.Add(1)
			}
		})
	}
	workers.Wait()
	if unverified.Load() != 20 || exhausted.Load() != 80 {
		t.Fatalf("budget results: unverified=%d exhausted=%d", unverified.Load(), exhausted.Load())
	}
}

func TestProofExhaustedBudgetDoesNotAllocate(t *testing.T) {
	var proof Proof
	for range 20 {
		proof.Observe(nil, nil, "error")
	}
	if allocations := testing.AllocsPerRun(100, func() { proof.Observe(nil, nil, "error") }); allocations != 0 {
		t.Fatalf("exhausted probe allocated: %f", allocations)
	}
}
