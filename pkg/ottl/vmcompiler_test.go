// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

func boolp(v bool) *boolean {
	b := boolean(v)
	return &b
}

func stringp(v string) *string {
	return &v
}

func newTestParser(t *testing.T, withVM bool) Parser[any] {
	opts := []Option[any]{}
	if withVM {
		opts = append(opts, WithVMEnabled[any]())
	}
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return nil, errors.New("path parsing not supported in test")
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		opts...,
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}
	return p
}

func TestCompileMicroMathExpression_Ints(t *testing.T) {
	expr := &mathExpression{
		Left: &addSubTerm{
			Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(1)}},
			Right: []*opMultDivValue{
				{
					Operator: mult,
					Value:    &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}},
				},
			},
		},
		Right: []*opAddSubTerm{
			{
				Operator: add,
				Term: &addSubTerm{
					Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(3)}},
				},
			},
		},
	}

	program, err := compileMicroMathExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok {
		t.Fatalf("expected int result")
	}
	if got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestCompileMicroMathExpression_Floats(t *testing.T) {
	expr := &mathExpression{
		Left: &addSubTerm{
			Left: &mathValue{Literal: &mathExprLiteral{Float: float64p(1.5)}},
			Right: []*opMultDivValue{
				{
					Operator: mult,
					Value:    &mathValue{Literal: &mathExprLiteral{Float: float64p(2.0)}},
				},
			},
		},
	}

	program, err := compileMicroMathExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Float64()
	if !ok {
		t.Fatalf("expected float result")
	}
	if got != 3.0 {
		t.Fatalf("expected 3.0, got %v", got)
	}
}

func TestCompileMicroComparison_Eq(t *testing.T) {
	p := newTestParser(t, false)
	expr := &mathExpression{
		Left: &addSubTerm{
			Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(1)}},
		},
		Right: []*opAddSubTerm{
			{
				Operator: add,
				Term: &addSubTerm{
					Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}},
				},
			},
		},
	}

	cmp := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(3)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroComparison_StringEq(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{String: stringp("a")},
		Op:    eq,
		Right: value{String: stringp("a")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroComparison_BoolNe(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Bool: boolp(true)},
		Op:    ne,
		Right: value{Bool: boolp(false)},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroComparison_MixedNumeric(t *testing.T) {
	p := newTestParser(t, false)
	expr := &mathExpression{
		Left: &addSubTerm{
			Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(3)}},
		},
	}

	cmp := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Float: float64p(3.0)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroComparison_Unsupported(t *testing.T) {
	p := newTestParser(t, false)
	n := isNil(true)
	cmp := &comparison{
		Left:  value{IsNil: &n},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	_, err := p.compileMicroComparisonVM(cmp)
	if err == nil {
		t.Fatalf("expected error for unsupported value")
	}
}

func TestCompileMicroComparison_StringLtUnsupported(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{String: stringp("a")},
		Op:    lt,
		Right: value{String: stringp("b")},
	}

	_, err := p.compileMicroComparisonVM(cmp)
	if err == nil {
		t.Fatalf("expected error for unsupported comparison")
	}
}

func TestCompileMicroComparison_PathGetter(t *testing.T) {
	getterParser, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(7), nil
				},
				Setter: func(context.Context, any, any) error {
					return nil
				},
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	evaluator, err := getterParser.newComparisonEvaluator(cmp)
	if err != nil {
		t.Fatalf("build evaluator failed: %v", err)
	}

	got, err := evaluator.Eval(context.Background(), nil)
	if err != nil {
		t.Fatalf("eval failed: %v", err)
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroBooleanExpression_AndOr(t *testing.T) {
	p := newTestParser(t, false)
	cmp1 := &comparison{
		Left:  value{Literal: &mathExprLiteral{Int: int64p(1)}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}
	cmp2 := &comparison{
		Left:  value{Literal: &mathExprLiteral{Int: int64p(2)}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmp1},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: cmp2}},
			},
		},
		Right: []*opOrTerm{
			{
				Operator: "or",
				Term: &term{
					Left: &booleanValue{ConstExpr: &constExpr{Boolean: boolp(false)}},
				},
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(8)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroBooleanExpression_Not(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Int: int64p(1)}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{
				Negation:   stringp("not"),
				Comparison: cmp,
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	machine := vm.NewMicroVM(4)
	val, err := machine.Run(program.program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}
