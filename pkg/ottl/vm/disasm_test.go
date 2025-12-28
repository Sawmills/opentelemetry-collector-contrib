// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"strings"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

func TestDisassemble(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpNot, 0),
			ir.Encode(ir.OpJumpIfTrue, 4),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.BoolValue(true),
			ir.BoolValue(false),
		},
	}

	got := Disassemble(program)
	lines := strings.Split(got, "\n")
	if len(lines) != len(program.Code) {
		t.Fatalf("expected %d lines, got %d", len(program.Code), len(lines))
	}
	wantOps := []string{"LOAD_CONST", "NOT", "JUMP_IF_TRUE", "LOAD_CONST", "EQ"}
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			t.Fatalf("expected fields in line %q", line)
		}
		if fields[1] != wantOps[i] {
			t.Fatalf("expected opcode %q at %d, got %q", wantOps[i], i, fields[1])
		}
	}
	if !strings.Contains(lines[0], "; true") {
		t.Fatalf("expected const comment for true, got %q", lines[0])
	}
	if !strings.Contains(lines[3], "; false") {
		t.Fatalf("expected const comment for false, got %q", lines[3])
	}
}
