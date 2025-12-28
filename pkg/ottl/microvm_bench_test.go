// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

var benchBoolSink bool

func int64p(v int64) *int64 {
	return &v
}

func float64p(v float64) *float64 {
	return &v
}

func newBenchParser(withVM bool) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return nil, errors.New("path parsing not supported in benchmark")
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

func newBenchParserWithPath(withVM bool, getter func() any) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			// Ensure keys are marked as consumed when present.
			_ = path.Keys()
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return getter(), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

func BenchmarkOTTLInterpreterAddEq(b *testing.B) {
	p, err := newBenchParser(false)
	if err != nil {
		b.Fatal(err)
	}

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

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLComparisonAddEq_VM(b *testing.B) {
	p, err := newBenchParser(true)
	if err != nil {
		b.Fatal(err)
	}

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

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLMicroVMAddEq(b *testing.B) {
	program := &vm.Program{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAdd, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
			ir.Int64Value(3),
		},
	}

	machine := vm.NewMicroVM(8)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := machine.Run(program)
		if err != nil {
			b.Fatal(err)
		}
		result, ok := val.Bool()
		if !ok {
			b.Fatal("result is not bool")
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLInterpreterFloatMulEq(b *testing.B) {
	p, err := newBenchParser(false)
	if err != nil {
		b.Fatal(err)
	}

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

	cmp := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Float: float64p(3.0)}},
	}

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLComparisonFloatMulEq_VM(b *testing.B) {
	p, err := newBenchParser(true)
	if err != nil {
		b.Fatal(err)
	}

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

	cmp := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Float: float64p(3.0)}},
	}

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLMicroVMFloatMulEq(b *testing.B) {
	program := &vm.Program{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpMul, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Float64Value(1.5),
			ir.Float64Value(2.0),
			ir.Float64Value(3.0),
		},
	}

	machine := vm.NewMicroVM(8)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val, err := machine.Run(program)
		if err != nil {
			b.Fatal(err)
		}
		result, ok := val.Bool()
		if !ok {
			b.Fatal("result is not bool")
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLInterpreterPathEq(b *testing.B) {
	p, err := newBenchParserWithPath(false, func() any { return int64(7) })
	if err != nil {
		b.Fatal(err)
	}

	pathExpr := &mathExprLiteral{
		Path: &path{
			Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("foo")}}}},
		},
	}
	cmp := &comparison{
		Left:  value{Literal: pathExpr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}

func BenchmarkOTTLComparisonPathEq_VM(b *testing.B) {
	p, err := newBenchParserWithPath(true, func() any { return int64(7) })
	if err != nil {
		b.Fatal(err)
	}

	pathExpr := &mathExprLiteral{
		Path: &path{
			Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("foo")}}}},
		},
	}
	cmp := &comparison{
		Left:  value{Literal: pathExpr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	evaluator, err := p.newComparisonEvaluator(cmp)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = result
	}
}
