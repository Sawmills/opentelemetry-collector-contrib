// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
)

func stringpTest(v string) *string {
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

// --- Constant Folding Tests ---

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

// --- Runtime Tests (not constant-foldable) ---

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
		Left:  value{String: stringpTest("a")},
		Op:    lt,
		Right: value{String: stringpTest("b")},
	}

	_, err := p.compileMicroComparisonVM(cmp)
	if err == nil {
		t.Fatalf("expected error for unsupported comparison")
	}
}

func TestCompileMicroBooleanExpression_ShortCircuitOr(t *testing.T) {
	var boomCalls int
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			keys := path.Keys()
			var key string
			if len(keys) > 0 {
				s, err := keys[0].String(context.Background(), nil)
				if err != nil {
					return nil, err
				}
				if s != nil {
					key = *s
				}
			}
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					if key == "boom" {
						boomCalls++
						return nil, errors.New("boom")
					}
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmpOK := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("ok")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}
	cmpBoom := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("boom")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpOK},
		},
		Right: []*opOrTerm{
			{
				Operator: "or",
				Term: &term{
					Left: &booleanValue{Comparison: cmpBoom},
				},
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	var stack [defaultMicroVMStackSize]ir.Value
	// Use context-aware runner since we now emit OpLoadAttrCached
	val, err := vm.RunWithStackAndContext(stack[:], program.program, context.Background(), nil)
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
	if boomCalls != 0 {
		t.Fatalf("expected boom getter to be skipped, got %d calls", boomCalls)
	}
}

func TestCompileMicroBooleanExpression_ShortCircuitAnd(t *testing.T) {
	var boomCalls int
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			keys := path.Keys()
			var key string
			if len(keys) > 0 {
				s, err := keys[0].String(context.Background(), nil)
				if err != nil {
					return nil, err
				}
				if s != nil {
					key = *s
				}
			}
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					if key == "boom" {
						boomCalls++
						return nil, errors.New("boom")
					}
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmpFalse := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("ok")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	cmpBoom := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("boom")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpFalse},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: cmpBoom}},
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	var stack [defaultMicroVMStackSize]ir.Value
	// Use context-aware runner since we now emit OpLoadAttrCached
	val, err := vm.RunWithStackAndContext(stack[:], program.program, context.Background(), nil)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if got {
		t.Fatalf("expected false, got true")
	}
	if boomCalls != 0 {
		t.Fatalf("expected boom getter to be skipped, got %d calls", boomCalls)
	}
}

func TestCompileMicroComparison_GasLimitOption(t *testing.T) {
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
		WithVMGasLimit[any](123),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	// Use a path so constant folding doesn't apply
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program.program.GasLimit != 123 {
		t.Fatalf("expected gas limit 123, got %d", program.program.GasLimit)
	}
}
