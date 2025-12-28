// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestCompileBoolExpressionConstFold(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 + 2 == 3")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true constant, got %v", val)
	}
}

func TestCompileComparisonConstFold(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 + 2 == 4")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if expr.Left == nil || expr.Left.Left == nil || expr.Left.Left.Comparison == nil {
		t.Fatalf("expected comparison expression")
	}
	cmp := expr.Left.Left.Comparison
	program, err := parser.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || got {
		t.Fatalf("expected false constant, got %v", val)
	}
}
