// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package rejectionrepair // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/rejectionrepair"

import (
	"encoding/json"
	"sync/atomic"
	"unicode/utf8"
)

// ProofResult contains no document data. NotSampled means the lifetime budget
// was exhausted; Unverified means a selected sample could not be checked.
type ProofResult uint8

const (
	NotSampled ProofResult = iota
	Equal
	Unequal
	Unverified
)

// Proof samples the first 20 repair candidates. The exporter shares one Proof
// across the process, including concurrent sessions and exporters. It retains only
// a saturating count; callers must not copy it after first use.
type Proof struct {
	selected atomic.Uint32
}

// Observe compares the actual outgoing backup with the original rejected value.
// It never changes delivery or retains payloads. Each selected document is
// limited to 1 MiB; oversized or ambiguous evidence is Unverified.
func (p *Proof) Observe(original, outgoing []byte, field string) ProofResult {
	for {
		selected := p.selected.Load()
		if selected >= 20 {
			return NotSampled
		}
		if p.selected.CompareAndSwap(selected, selected+1) {
			break
		}
	}
	if len(original) > 1<<20 || len(outgoing) > 1<<20 || !rootField(field) ||
		!utf8.Valid(original) || !utf8.Valid(outgoing) {
		return Unverified
	}
	before, ok := uniqueObject(original)
	if !ok {
		return Unverified
	}
	value, exists := before[field]
	if !exists {
		return Unverified
	}
	after, ok := uniqueObject(outgoing)
	if !ok {
		return Unverified
	}
	var envelope string
	if json.Unmarshal(after["sawmills_original_"+field+"_json"], &envelope) != nil {
		return Unverified
	}
	fields, ok := uniqueObject([]byte(envelope))
	if !ok || len(fields) != 3 {
		return Unverified
	}
	for _, name := range []string{"key", "value_type", "value_json"} {
		if member := fields[name]; len(member) == 0 || member[0] != '"' {
			return Unverified
		}
	}
	var key, kind, raw string
	if json.Unmarshal(fields["key"], &key) != nil ||
		json.Unmarshal(fields["value_type"], &kind) != nil ||
		json.Unmarshal(fields["value_json"], &raw) != nil {
		return Unverified
	}
	if _, remains := after[field]; remains || key != field || kind != valueType(value) || raw != string(value) {
		return Unequal
	}
	return Equal
}
