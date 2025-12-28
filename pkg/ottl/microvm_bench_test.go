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

func stringp(v string) *string {
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

func newBenchParserWithPaths(withVM bool, getter func(pathName, key string) any) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
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
					return getter(path.Name(), key), nil
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
	program := &vm.ProgramAny{
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
	program := &vm.ProgramAny{
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

func BenchmarkOTTLInterpreterComplexWhere(b *testing.B) {
	p, err := newBenchParserWithPaths(false, func(_ string, key string) any {
		switch key {
		case "foo":
			return int64(7)
		case "bar":
			return int64(9)
		default:
			return int64(0)
		}
	})
	if err != nil {
		b.Fatal(err)
	}

	cmpFoo := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("foo")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}
	cmpBar := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("bar")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(9)}},
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
	cmpMath := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(3)}},
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpFoo},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: cmpBar}},
			},
		},
		Right: []*opOrTerm{
			{
				Operator: "or",
				Term: &term{
					Left: &booleanValue{Comparison: cmpMath},
				},
			},
		},
	}

	evaluator, err := p.newBoolExpr(boolExpr)
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

func BenchmarkOTTLComparisonComplexWhere_VM(b *testing.B) {
	p, err := newBenchParserWithPaths(true, func(_ string, key string) any {
		switch key {
		case "foo":
			return int64(7)
		case "bar":
			return int64(9)
		default:
			return int64(0)
		}
	})
	if err != nil {
		b.Fatal(err)
	}

	cmpFoo := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("foo")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}
	cmpBar := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("bar")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(9)}},
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
	cmpMath := &comparison{
		Left:  value{MathExpression: expr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(3)}},
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpFoo},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: cmpBar}},
			},
		},
		Right: []*opOrTerm{
			{
				Operator: "or",
				Term: &term{
					Left: &booleanValue{Comparison: cmpMath},
				},
			},
		},
	}

	evaluator, err := p.newBoolExpr(boolExpr)
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

func BenchmarkOTTLInterpreterDeepArithmetic(b *testing.B) {
	p, err := newBenchParser(false)
	if err != nil {
		b.Fatal(err)
	}

	innerAdd := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(1)}}},
		Right: []*opAddSubTerm{{
			Operator: add,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}}},
		}},
	}
	mulBy3 := &mathExpression{
		Left: &addSubTerm{
			Left:  &mathValue{SubExpression: innerAdd},
			Right: []*opMultDivValue{{Operator: mult, Value: &mathValue{Literal: &mathExprLiteral{Int: int64p(3)}}}},
		},
	}
	sub4 := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{SubExpression: mulBy3}},
		Right: []*opAddSubTerm{{
			Operator: sub,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(4)}}},
		}},
	}
	div2 := &mathExpression{
		Left: &addSubTerm{
			Left:  &mathValue{SubExpression: sub4},
			Right: []*opMultDivValue{{Operator: div, Value: &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}}}},
		},
	}
	add5 := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{SubExpression: div2}},
		Right: []*opAddSubTerm{{
			Operator: add,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(5)}}},
		}},
	}

	cmp := &comparison{
		Left:  value{MathExpression: add5},
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

func BenchmarkOTTLComparisonDeepArithmetic_VM(b *testing.B) {
	p, err := newBenchParser(true)
	if err != nil {
		b.Fatal(err)
	}

	innerAdd := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(1)}}},
		Right: []*opAddSubTerm{{
			Operator: add,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}}},
		}},
	}
	mulBy3 := &mathExpression{
		Left: &addSubTerm{
			Left:  &mathValue{SubExpression: innerAdd},
			Right: []*opMultDivValue{{Operator: mult, Value: &mathValue{Literal: &mathExprLiteral{Int: int64p(3)}}}},
		},
	}
	sub4 := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{SubExpression: mulBy3}},
		Right: []*opAddSubTerm{{
			Operator: sub,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(4)}}},
		}},
	}
	div2 := &mathExpression{
		Left: &addSubTerm{
			Left:  &mathValue{SubExpression: sub4},
			Right: []*opMultDivValue{{Operator: div, Value: &mathValue{Literal: &mathExprLiteral{Int: int64p(2)}}}},
		},
	}
	add5 := &mathExpression{
		Left: &addSubTerm{Left: &mathValue{SubExpression: div2}},
		Right: []*opAddSubTerm{{
			Operator: add,
			Term:     &addSubTerm{Left: &mathValue{Literal: &mathExprLiteral{Int: int64p(5)}}},
		}},
	}

	cmp := &comparison{
		Left:  value{MathExpression: add5},
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

func BenchmarkOTTLMicroVMDeepArithmetic(b *testing.B) {
	program := &vm.ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAdd, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpMul, 0),
			ir.Encode(ir.OpLoadConst, 3),
			ir.Encode(ir.OpSub, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpDiv, 0),
			ir.Encode(ir.OpLoadConst, 4),
			ir.Encode(ir.OpAdd, 0),
			ir.Encode(ir.OpLoadConst, 5),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
			ir.Int64Value(3),
			ir.Int64Value(4),
			ir.Int64Value(5),
			ir.Int64Value(7),
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

func BenchmarkOTTLInterpreterManyComparisons(b *testing.B) {
	p, err := newBenchParser(false)
	if err != nil {
		b.Fatal(err)
	}

	buildCmp := func(left, right int64) *comparison {
		return &comparison{
			Left:  value{Literal: &mathExprLiteral{Int: int64p(left)}},
			Op:    eq,
			Right: value{Literal: &mathExprLiteral{Int: int64p(right)}},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: buildCmp(1, 1)},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(2, 2)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(3, 3)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(4, 4)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(5, 5)}},
			},
		},
	}

	evaluator, err := p.newBoolExpr(boolExpr)
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

func BenchmarkOTTLComparisonManyComparisons_VM(b *testing.B) {
	p, err := newBenchParser(true)
	if err != nil {
		b.Fatal(err)
	}

	buildCmp := func(left, right int64) *comparison {
		return &comparison{
			Left:  value{Literal: &mathExprLiteral{Int: int64p(left)}},
			Op:    eq,
			Right: value{Literal: &mathExprLiteral{Int: int64p(right)}},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: buildCmp(1, 1)},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(2, 2)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(3, 3)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(4, 4)}},
				{Operator: "and", Value: &booleanValue{Comparison: buildCmp(5, 5)}},
			},
		},
	}

	evaluator, err := p.newBoolExpr(boolExpr)
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

func BenchmarkOTTLMicroVMManyComparisons(b *testing.B) {
	program := &vm.ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpEq, 0),
			ir.Encode(ir.OpJumpIfFalse, 20),
			ir.Encode(ir.OpPop, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpEq, 0),
			ir.Encode(ir.OpJumpIfFalse, 20),
			ir.Encode(ir.OpPop, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpEq, 0),
			ir.Encode(ir.OpJumpIfFalse, 20),
			ir.Encode(ir.OpPop, 0),
			ir.Encode(ir.OpLoadConst, 3),
			ir.Encode(ir.OpLoadConst, 3),
			ir.Encode(ir.OpEq, 0),
			ir.Encode(ir.OpJumpIfFalse, 20),
			ir.Encode(ir.OpPop, 0),
			ir.Encode(ir.OpLoadConst, 4),
			ir.Encode(ir.OpLoadConst, 4),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
			ir.Int64Value(3),
			ir.Int64Value(4),
			ir.Int64Value(5),
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

func BenchmarkOTTLMicroVMDeepArithmeticSpecialized(b *testing.B) {
	program := &vm.ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAddInt, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpMulInt, 0),
			ir.Encode(ir.OpLoadConst, 3),
			ir.Encode(ir.OpSubInt, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpDivInt, 0),
			ir.Encode(ir.OpLoadConst, 4),
			ir.Encode(ir.OpAddInt, 0),
			ir.Encode(ir.OpLoadConst, 5),
			ir.Encode(ir.OpEqInt, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
			ir.Int64Value(3),
			ir.Int64Value(4),
			ir.Int64Value(5),
			ir.Int64Value(7),
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

func BenchmarkOTTLMicroVMAddEqSpecialized(b *testing.B) {
	program := &vm.ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAddInt, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpEqInt, 0),
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
