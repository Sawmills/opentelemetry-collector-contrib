// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

var benchBoolSink bool

type benchIsMatchArguments[K any] struct {
	Target  StringLikeGetter[K]
	Pattern StringGetter[K]
}

func newBenchIsMatchFactory[K any]() Factory[K] {
	return NewFactory("IsMatch", &benchIsMatchArguments[K]{}, createBenchIsMatchFunction[K])
}

func createBenchIsMatchFunction[K any](_ FunctionContext, oArgs Arguments) (ExprFunc[K], error) {
	args, ok := oArgs.(*benchIsMatchArguments[K])
	if !ok {
		return nil, errors.New("IsMatchFactory args must be of type *benchIsMatchArguments[K]")
	}
	var cached *regexp.Regexp
	if pattern, isLiteral := GetLiteralValue(args.Pattern); isLiteral {
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		cached = compiled
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		target, err := args.Target.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return false, nil
		}
		re := cached
		if re == nil {
			pattern, err := args.Pattern.Get(ctx, tCtx)
			if err != nil {
				return nil, err
			}
			re, err = regexp.Compile(pattern)
			if err != nil {
				return nil, err
			}
		}
		return re.MatchString(*target), nil
	}, nil
}

type benchIsIntArguments[K any] struct {
	Target IntGetter[K]
}

func newBenchIsIntFactory[K any]() Factory[K] {
	return NewFactory("IsInt", &benchIsIntArguments[K]{}, createBenchIsIntFunction[K])
}

func createBenchIsIntFunction[K any](_ FunctionContext, oArgs Arguments) (ExprFunc[K], error) {
	args, ok := oArgs.(*benchIsIntArguments[K])
	if !ok {
		return nil, errors.New("IsIntFactory args must be of type *benchIsIntArguments[K]")
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		_, err := args.Target.Get(ctx, tCtx)
		switch err.(type) {
		case TypeError:
			return false, nil
		case nil:
			return true, nil
		default:
			return false, err
		}
	}, nil
}

type benchIsMapArguments[K any] struct {
	Target PMapGetter[K]
}

func newBenchIsMapFactory[K any]() Factory[K] {
	return NewFactory("IsMap", &benchIsMapArguments[K]{}, createBenchIsMapFunction[K])
}

func createBenchIsMapFunction[K any](_ FunctionContext, oArgs Arguments) (ExprFunc[K], error) {
	args, ok := oArgs.(*benchIsMapArguments[K])
	if !ok {
		return nil, errors.New("IsMapFactory args must be of type *benchIsMapArguments[K]")
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		_, err := args.Target.Get(ctx, tCtx)
		switch err.(type) {
		case TypeError:
			return false, nil
		case nil:
			return true, nil
		default:
			return false, err
		}
	}, nil
}

type benchIsListArguments[K any] struct {
	Target Getter[K]
}

func newBenchIsListFactory[K any]() Factory[K] {
	return NewFactory("IsList", &benchIsListArguments[K]{}, createBenchIsListFunction[K])
}

func createBenchIsListFunction[K any](_ FunctionContext, oArgs Arguments) (ExprFunc[K], error) {
	args, ok := oArgs.(*benchIsListArguments[K])
	if !ok {
		return nil, errors.New("IsListFactory args must be of type *benchIsListArguments[K]")
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		val, err := args.Target.Get(ctx, tCtx)
		if err != nil {
			return false, err
		}
		switch v := val.(type) {
		case pcommon.Value:
			return v.Type() == pcommon.ValueTypeSlice, nil
		case pcommon.Slice:
			return true, nil
		default:
			return false, nil
		}
	}, nil
}

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
			curr := path
			_ = curr.Keys()
			for {
				next := curr.Next()
				if next == nil {
					break
				}
				curr = next
				_ = curr.Keys()
			}
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

func newBenchParserWithPathAndFunctions(withVM bool, functions map[string]Factory[any], getter func() any) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
		functions,
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

func newBenchParserWithPathVMGetter(withVM bool, getter func() int64) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			_ = path.Keys()
			base := StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return getter(), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}
			return StandardVMGetSetter[any]{
				StandardGetSetter: base,
				VMGetterFunc: VMGetterFunc[any](func(context.Context, any) (ir.Value, error) {
					return ir.Int64Value(getter()), nil
				}),
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

func newBenchParserWithPathVMGetterAny(withVM bool, functions map[string]Factory[any], getter func() any, vmGetter func() ir.Value) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options, WithVMEnabled[any]())
	}
	return NewParser[any](
		functions,
		func(path Path[any]) (GetSetter[any], error) {
			_ = path.Keys()
			base := StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return getter(), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}
			return StandardVMGetSetter[any]{
				StandardGetSetter: base,
				VMGetterFunc: VMGetterFunc[any](func(context.Context, any) (ir.Value, error) {
					return vmGetter(), nil
				}),
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

func newBenchParserWithPaths(withVM bool, getter func(pathName, key string) any) (Parser[any], error) {
	options := []Option[any]{}
	if withVM {
		options = append(options,
			WithVMEnabled[any](),
			WithVMAttrGetter[any](func(_ any, key string) (ir.Value, error) {
				switch key {
				case "foo":
					return ir.Int64Value(7), nil
				case "bar":
					return ir.Int64Value(9), nil
				default:
					return ir.Int64Value(0), nil
				}
			}),
			WithVMAttrContextNames[any]([]string{""}),
		)
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

func BenchmarkOTTLComparisonPathEq_VMGetter(b *testing.B) {
	p, err := newBenchParserWithPathVMGetter(true, func() int64 { return 7 })
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

func BenchmarkOTTLInterpreterIsMatchLiteral(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMatchFactory[any]())
	p, err := newBenchParserWithPathAndFunctions(false, functions, func() any {
		return "operation[AC]"
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMatch("operationA", "operation[AC]")`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonIsMatchLiteral_VM(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMatchFactory[any]())
	p, err := newBenchParserWithPathAndFunctions(true, functions, func() any {
		return "operation[AC]"
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMatch("operationA", "operation[AC]")`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLInterpreterIsMatchDynamic(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMatchFactory[any]())
	p, err := newBenchParserWithPathAndFunctions(false, functions, func() any {
		return "operation[AC]"
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMatch("operationA", attributes["pattern"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonIsMatchDynamic_VM(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMatchFactory[any]())
	p, err := newBenchParserWithPathAndFunctions(true, functions, func() any {
		return "operation[AC]"
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMatch("operationA", attributes["pattern"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLInterpreterIsInt(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsIntFactory[any]())
	p, err := newBenchParserWithPathAndFunctions(false, functions, func() any {
		return int64(1)
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsInt(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonIsInt_VM(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsIntFactory[any]())
	p, err := newBenchParserWithPathVMGetterAny(true, functions, func() any { return int64(1) }, func() ir.Value {
		return ir.Int64Value(1)
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsInt(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLInterpreterIsMap(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMapFactory[any]())
	pm := pcommon.NewMap()
	pm.PutStr("k", "v")
	p, err := newBenchParserWithPathAndFunctions(false, functions, func() any {
		return pm
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMap(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonIsMap_VM(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsMapFactory[any]())
	pm := pcommon.NewMap()
	pm.PutStr("k", "v")
	p, err := newBenchParserWithPathVMGetterAny(true, functions, func() any { return pm }, func() ir.Value {
		return pMapValue(pm)
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsMap(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLInterpreterIsList(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsListFactory[any]())
	ps := pcommon.NewSlice()
	ps.AppendEmpty().SetInt(1)
	p, err := newBenchParserWithPathAndFunctions(false, functions, func() any {
		return ps
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsList(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonIsList_VM(b *testing.B) {
	functions := CreateFactoryMap[any](newBenchIsListFactory[any]())
	ps := pcommon.NewSlice()
	ps.AppendEmpty().SetInt(1)
	p, err := newBenchParserWithPathVMGetterAny(true, functions, func() any { return ps }, func() ir.Value {
		return pSliceValue(ps)
	})
	if err != nil {
		b.Fatal(err)
	}
	expr, err := parseCondition(`IsList(attributes["foo"])`)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLInterpreterMixedPath(b *testing.B) {
	// Setup: attributes["root"]["nested"][1]["id"] == 2
	m := pcommon.NewMap()
	root := m.PutEmptyMap("root")
	nested := root.PutEmptySlice("nested")
	nested.AppendEmpty().SetEmptyMap().PutInt("id", 1)
	nested.AppendEmpty().SetEmptyMap().PutInt("id", 2)

	p, err := newBenchParserWithPath(false, func() any { return int64(2) })
	if err != nil {
		b.Fatal(err)
	}

	// path: attributes["root"]["nested"][1]["id"]
	pathExpr := &mathExprLiteral{
		Path: &path{
			Fields: []field{
				{Name: "attributes", Keys: []key{{String: stringp("root")}}},
				{Name: "nested", Keys: []key{{Int: int64p(1)}}},
				{Name: "id", Keys: []key{}},
			},
		},
	}
	cmp := &comparison{
		Left:  value{Literal: pathExpr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
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

func BenchmarkOTTLComparisonMixedPath_VM(b *testing.B) {
	// Setup: attributes["root"]["nested"][1]["id"] == 2
	m := pcommon.NewMap()
	root := m.PutEmptyMap("root")
	nested := root.PutEmptySlice("nested")
	nested.AppendEmpty().SetEmptyMap().PutInt("id", 1)
	nested.AppendEmpty().SetEmptyMap().PutInt("id", 2)

	p, err := newBenchParserWithPath(true, func() any { return int64(2) })
	if err != nil {
		b.Fatal(err)
	}

	// path: attributes["root"]["nested"][1]["id"]
	pathExpr := &mathExprLiteral{
		Path: &path{
			Fields: []field{
				{Name: "attributes", Keys: []key{{String: stringp("root")}}},
				{Name: "nested", Keys: []key{{Int: int64p(1)}}},
				{Name: "id", Keys: []key{}},
			},
		},
	}
	cmp := &comparison{
		Left:  value{Literal: pathExpr},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
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

// Pattern: ((A and B) or (C and D)) and (E or F)
func BenchmarkOTTLInterpreterDeepBooleanChain(b *testing.B) {
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

	innerAnd1 := &term{
		Left: &booleanValue{Comparison: buildCmp(1, 1)},
		Right: []*opAndBooleanValue{
			{Operator: "and", Value: &booleanValue{Comparison: buildCmp(2, 2)}},
		},
	}
	innerAnd2 := &term{
		Left: &booleanValue{Comparison: buildCmp(3, 3)},
		Right: []*opAndBooleanValue{
			{Operator: "and", Value: &booleanValue{Comparison: buildCmp(4, 4)}},
		},
	}
	innerOr := &booleanExpression{
		Left: innerAnd1,
		Right: []*opOrTerm{
			{Operator: "or", Term: innerAnd2},
		},
	}
	outerOr := &term{
		Left: &booleanValue{Comparison: buildCmp(5, 5)},
	}
	outerOrExpr := &booleanExpression{
		Left: outerOr,
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(6, 6)}}},
		},
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{SubExpr: innerOr},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{SubExpr: outerOrExpr}},
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

func BenchmarkOTTLComparisonDeepBooleanChain_VM(b *testing.B) {
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

	innerAnd1 := &term{
		Left: &booleanValue{Comparison: buildCmp(1, 1)},
		Right: []*opAndBooleanValue{
			{Operator: "and", Value: &booleanValue{Comparison: buildCmp(2, 2)}},
		},
	}
	innerAnd2 := &term{
		Left: &booleanValue{Comparison: buildCmp(3, 3)},
		Right: []*opAndBooleanValue{
			{Operator: "and", Value: &booleanValue{Comparison: buildCmp(4, 4)}},
		},
	}
	innerOr := &booleanExpression{
		Left: innerAnd1,
		Right: []*opOrTerm{
			{Operator: "or", Term: innerAnd2},
		},
	}
	outerOr := &term{
		Left: &booleanValue{Comparison: buildCmp(5, 5)},
	}
	outerOrExpr := &booleanExpression{
		Left: outerOr,
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(6, 6)}}},
		},
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{SubExpr: innerOr},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{SubExpr: outerOrExpr}},
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

// Pattern: not A and not B and not C and not D
func BenchmarkOTTLInterpreterManyNots(b *testing.B) {
	p, err := newBenchParser(false)
	if err != nil {
		b.Fatal(err)
	}

	buildNegatedCmp := func(left, right int64) *booleanValue {
		return &booleanValue{
			Negation: stringp("not"),
			Comparison: &comparison{
				Left:  value{Literal: &mathExprLiteral{Int: int64p(left)}},
				Op:    eq,
				Right: value{Literal: &mathExprLiteral{Int: int64p(right)}},
			},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: buildNegatedCmp(1, 2),
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: buildNegatedCmp(3, 4)},
				{Operator: "and", Value: buildNegatedCmp(5, 6)},
				{Operator: "and", Value: buildNegatedCmp(7, 8)},
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

func BenchmarkOTTLComparisonManyNots_VM(b *testing.B) {
	p, err := newBenchParser(true)
	if err != nil {
		b.Fatal(err)
	}

	buildNegatedCmp := func(left, right int64) *booleanValue {
		return &booleanValue{
			Negation: stringp("not"),
			Comparison: &comparison{
				Left:  value{Literal: &mathExprLiteral{Int: int64p(left)}},
				Op:    eq,
				Right: value{Literal: &mathExprLiteral{Int: int64p(right)}},
			},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{
			Left: buildNegatedCmp(1, 2),
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: buildNegatedCmp(3, 4)},
				{Operator: "and", Value: buildNegatedCmp(5, 6)},
				{Operator: "and", Value: buildNegatedCmp(7, 8)},
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

func BenchmarkOTTLInterpreterStringEquality(b *testing.B) {
	p, err := newBenchParserWithPath(false, func() any { return "ERROR" })
	if err != nil {
		b.Fatal(err)
	}

	cmp := &comparison{
		Left: value{Literal: &mathExprLiteral{
			Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("level")}}}}},
		}},
		Op:    eq,
		Right: value{String: stringp("ERROR")},
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

func BenchmarkOTTLComparisonStringEquality_VM(b *testing.B) {
	p, err := newBenchParserWithPathVMGetterAny(true, nil, func() any { return "ERROR" }, func() ir.Value {
		return ir.StringValue("ERROR")
	})
	if err != nil {
		b.Fatal(err)
	}

	cmp := &comparison{
		Left: value{Literal: &mathExprLiteral{
			Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("level")}}}}},
		}},
		Op:    eq,
		Right: value{String: stringp("ERROR")},
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

// Pattern: level == "ERROR" or level == "WARN" or level == "WARNING"
func BenchmarkOTTLInterpreterMultipleStringChecks(b *testing.B) {
	p, err := newBenchParserWithPath(false, func() any { return "WARNING" })
	if err != nil {
		b.Fatal(err)
	}

	buildStringCmp := func(val string) *comparison {
		return &comparison{
			Left: value{Literal: &mathExprLiteral{
				Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("level")}}}}},
			}},
			Op:    eq,
			Right: value{String: stringp(val)},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{Left: &booleanValue{Comparison: buildStringCmp("ERROR")}},
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildStringCmp("WARN")}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildStringCmp("WARNING")}}},
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

func BenchmarkOTTLComparisonMultipleStringChecks_VM(b *testing.B) {
	p, err := newBenchParserWithPathVMGetterAny(true, nil, func() any { return "WARNING" }, func() ir.Value {
		return ir.StringValue("WARNING")
	})
	if err != nil {
		b.Fatal(err)
	}

	buildStringCmp := func(val string) *comparison {
		return &comparison{
			Left: value{Literal: &mathExprLiteral{
				Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringp("level")}}}}},
			}},
			Op:    eq,
			Right: value{String: stringp(val)},
		}
	}

	boolExpr := &booleanExpression{
		Left: &term{Left: &booleanValue{Comparison: buildStringCmp("ERROR")}},
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildStringCmp("WARN")}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildStringCmp("WARNING")}}},
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

func BenchmarkOTTLInterpreterShortCircuitAnd(b *testing.B) {
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
			Left: &booleanValue{Comparison: buildCmp(1, 2)},
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

func BenchmarkOTTLComparisonShortCircuitAnd_VM(b *testing.B) {
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
			Left: &booleanValue{Comparison: buildCmp(1, 2)},
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

func BenchmarkOTTLInterpreterShortCircuitOr(b *testing.B) {
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
		Left: &term{Left: &booleanValue{Comparison: buildCmp(1, 1)}},
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(2, 3)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(3, 4)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(4, 5)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(5, 6)}}},
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

func BenchmarkOTTLComparisonShortCircuitOr_VM(b *testing.B) {
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
		Left: &term{Left: &booleanValue{Comparison: buildCmp(1, 1)}},
		Right: []*opOrTerm{
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(2, 3)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(3, 4)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(4, 5)}}},
			{Operator: "or", Term: &term{Left: &booleanValue{Comparison: buildCmp(5, 6)}}},
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

// =============================================================================
// Real-World Production Benchmarks (from Sawmills filtersamplingprocessor)
// =============================================================================

// newBenchParserForRealWorld creates a parser that handles multiple path types
// used in real production OTTL expressions (attributes, resource.attributes,
// instrumentation_scope.attributes, body, body.string)
func newBenchParserForRealWorld(withVM bool, pathValues map[string]any) (Parser[any], error) {
	functions := CreateFactoryMap[any](
		newBenchIsMatchFactory[any](),
		newBenchIsMapFactory[any](),
	)

	options := []Option[any]{}

	// Build a stable key index for both interpreter and VM to avoid repeated map hashing.
	keyIndex := make(map[string]uint32, len(pathValues)*2)
	indexedRaw := make([]any, 0, len(pathValues)*2)
	addIndexedRaw := func(key string, v any) {
		keyIndex[key] = uint32(len(indexedRaw))
		indexedRaw = append(indexedRaw, v)
	}
	for k, v := range pathValues {
		addIndexedRaw(k, v)
		if m, ok := v.(map[string]any); ok {
			// flatten nested maps into index
			flattenRawMap(k, m, addIndexedRaw)
		}
	}
	if withVM {
		// Precompute VM values (including flattened nested maps) for fast-path attribute loads
		vmVals := make(map[string]ir.Value, len(pathValues)*2)
		addVMVals("", pathValues, vmVals)

		// Build an index table keyed by attr key to avoid string hashing in VMAttrGetter
		keyIndex := make(map[string]uint32, len(vmVals))
		indexedVals := make([]ir.Value, 0, len(vmVals))
		for k, v := range vmVals {
			keyIndex[k] = uint32(len(indexedVals))
			indexedVals = append(indexedVals, v)
		}

		options = append(options, WithVMEnabled[any](), WithVMAttrGetter(func(_ any, key string) (ir.Value, error) {
			if idx, ok := keyIndex[key]; ok {
				return indexedVals[idx], nil
			}
			return ir.Value{Type: ir.TypeNone}, nil
		}))
	}

	return NewParser[any](
		functions,
		func(path Path[any]) (GetSetter[any], error) {
			// Build the full path string for lookup
			pathStr := buildPathString(path)
			idx, ok := keyIndex[pathStr]
			if !ok {
				return StandardGetSetter[any]{
					Getter: func(context.Context, any) (any, error) { return nil, nil },
					Setter: func(context.Context, any, any) error { return nil },
				}, nil
			}
			raw := indexedRaw[idx]
			vmVal, err := valueToVM(raw)
			if err != nil {
				// Fall back to standard getter if conversion unsupported
				return StandardGetSetter[any]{
					Getter: func(context.Context, any) (any, error) { return raw, nil },
					Setter: func(context.Context, any, any) error { return nil },
				}, nil
			}
			return vmStaticGetterSetter{
				raw:   raw,
				vmVal: vmVal,
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

// addVMVals flattens map values into vmVals using buildPathString-style keys.
// Only stores leaves that can be converted to ir.Value; unsupported leaves are skipped.
func addVMVals(prefix string, vals map[string]any, out map[string]ir.Value) {
	for k, v := range vals {
		key := k
		if prefix != "" {
			key = prefix + k
		}
		switch m := v.(type) {
		case map[string]any:
			// Flatten nested map
			flattenNestedMap(key, m, out)
			// Skip storing the map itself (valueToVM does not support generic map[string]any)
		default:
			vmVal, err := valueToVM(v)
			if err != nil {
				continue
			}
			out[key] = vmVal
		}
	}
}

func flattenNestedMap(prefix string, m map[string]any, out map[string]ir.Value) {
	for childKey, childVal := range m {
		fullKey := prefix + "[" + childKey + "]"
		if nested, ok := childVal.(map[string]any); ok {
			flattenNestedMap(fullKey, nested, out)
			continue
		}
		vmVal, err := valueToVM(childVal)
		if err != nil {
			continue
		}
		out[fullKey] = vmVal
	}
}

// flattenRawMap mirrors flattenNestedMap but stores raw values via callback to avoid map hashes.
func flattenRawMap(prefix string, m map[string]any, add func(string, any)) {
	for childKey, childVal := range m {
		fullKey := prefix + "[" + childKey + "]"
		if nested, ok := childVal.(map[string]any); ok {
			flattenRawMap(fullKey, nested, add)
			continue
		}
		add(fullKey, childVal)
	}
}

// vmStaticGetterSetter returns precomputed raw and VM values and exposes a VMGetterProvider.
type vmStaticGetterSetter struct {
	raw   any
	vmVal ir.Value
}

func (g vmStaticGetterSetter) Get(context.Context, any) (any, error) { return g.raw, nil }
func (g vmStaticGetterSetter) Set(context.Context, any, any) error   { return nil }
func (g vmStaticGetterSetter) VMGetter() VMGetter[any] {
	return VMGetterFunc[any](func(context.Context, any) (ir.Value, error) {
		return g.vmVal, nil
	})
}

// buildPathString constructs a path string like "attributes[level]" or "resource.attributes[key]"
func buildPathString(path Path[any]) string {
	result := path.Name()
	keys := path.Keys()
	for _, k := range keys {
		s, err := k.String(context.Background(), nil)
		if err == nil && s != nil {
			result += "[" + *s + "]"
		}
	}
	next := path.Next()
	if next != nil {
		result += "." + buildPathString(next)
	}
	return result
}

// Real-world production expression from Sawmills filtersamplingprocessor
// This is a 54-line expression used in actual production filtering
const realWorldExpr = `(
	not (
		attributes["level"] == "ERROR" 
		or resource.attributes["level"] == "ERROR" 
		or instrumentation_scope.attributes["level"] == "ERROR" 
		or (IsMap(body) and body["level"] == "ERROR")
	) and not (
		attributes["level"] == "WARN" 
		or resource.attributes["level"] == "WARN"
		or instrumentation_scope.attributes["level"] == "WARN" 
		or (IsMap(body) and body["level"] == "WARN")
	) and not (
		attributes["level"] == "WARNING" 
		or resource.attributes["level"] == "WARNING" 
		or instrumentation_scope.attributes["level"] == "WARNING" 
		or (IsMap(body) and body["level"] == "WARNING")
	) and not (
		IsMatch(body.string, "(?i)The predictor zombie scan")
	) and not (
		IsMatch(body.string, "(?i)Checking for waiting")
	) 
	and IsMatch(attributes["service"], "^bigid-ml.*") 
	and not (
		IsMatch(attributes["ml_scan_id"], ".*") 
		or IsMatch(resource.attributes["ml_scan_id"], ".*") 
		or IsMatch(instrumentation_scope.attributes["ml_scan_id"], ".*") 
		or (IsMap(body) and IsMatch(body["ml_scan_id"], ".*"))
	) and not (
		IsMatch(attributes["scan_id"], ".*") 
		or IsMatch(resource.attributes["scan_id"], ".*") 
		or IsMatch(instrumentation_scope.attributes["scan_id"], ".*") 
		or (IsMap(body) and IsMatch(body["scan_id"], ".*"))
	) and not (
		IsMatch(attributes["chunks"], ".*") 
		or IsMatch(resource.attributes["chunks"], ".*") 
		or IsMatch(instrumentation_scope.attributes["chunks"], ".*") 
		or (IsMap(body) and IsMatch(body["chunks"], ".*"))
	) and not (
		IsMatch(attributes["training_iteration"], ".*") 
		or IsMatch(resource.attributes["training_iteration"], ".*") 
		or IsMatch(instrumentation_scope.attributes["training_iteration"], ".*") 
		or (IsMap(body) and IsMatch(body["training_iteration"], ".*"))
	) and not (
		IsMatch(attributes["queue_status"], ".*") 
		or IsMatch(resource.attributes["queue_status"], ".*") 
		or IsMatch(instrumentation_scope.attributes["queue_status"], ".*") 
		or (IsMap(body) and IsMatch(body["queue_status"], ".*"))
	) 
	and not IsMatch(resource.attributes["ddtags"]["pod_name"], "^bigid-ml-worker.*") 
	and not IsMatch(resource.attributes["ddtags"]["pod_name"], "^bigid-ml-scheduler.*") 
	and attributes["sawmills"]["index_name"] == nil
)`

func BenchmarkOTTLInterpreterRealWorld(b *testing.B) {
	// Set up path values that make the expression return true
	pathValues := map[string]any{
		"attributes[level]":                                    "INFO",
		"resource.attributes[level]":                           "INFO",
		"instrumentation_scope.attributes[level]":              "INFO",
		"body[level]":                                          nil,
		"body.string":                                          "Normal log message",
		"attributes[service]":                                  "bigid-ml-foo",
		"attributes[ml_scan_id]":                               nil,
		"resource.attributes[ml_scan_id]":                      nil,
		"instrumentation_scope.attributes[ml_scan_id]":         nil,
		"body[ml_scan_id]":                                     nil,
		"attributes[scan_id]":                                  nil,
		"resource.attributes[scan_id]":                         nil,
		"instrumentation_scope.attributes[scan_id]":            nil,
		"body[scan_id]":                                        nil,
		"attributes[chunks]":                                   nil,
		"resource.attributes[chunks]":                          nil,
		"instrumentation_scope.attributes[chunks]":             nil,
		"body[chunks]":                                         nil,
		"attributes[training_iteration]":                       nil,
		"resource.attributes[training_iteration]":              nil,
		"instrumentation_scope.attributes[training_iteration]": nil,
		"body[training_iteration]":                             nil,
		"attributes[queue_status]":                             nil,
		"resource.attributes[queue_status]":                    nil,
		"instrumentation_scope.attributes[queue_status]":       nil,
		"body[queue_status]":                                   nil,
		"resource.attributes[ddtags][pod_name]":                "some-other-pod",
		"attributes[sawmills][index_name]":                     nil,
		"body":                                                 "string body", // not a map
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(realWorldExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

// Nested body benchmark (deep map indexing)
func BenchmarkOTTLBodyNested(b *testing.B) {
	pathValues := map[string]any{
		"body": map[string]any{
			"a": map[string]any{
				"b": map[string]any{
					"c": "v",
				},
			},
		},
	}

	expr, err := parseCondition(`body["a"]["b"]["c"] == "v"`)
	if err != nil {
		b.Fatal(err)
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = res
	}
}

func BenchmarkOTTLBodyNested_VM(b *testing.B) {
	pathValues := map[string]any{
		"body": map[string]any{
			"a": map[string]any{
				"b": map[string]any{
					"c": "v",
				},
			},
		},
	}

	expr, err := parseCondition(`body["a"]["b"]["c"] == "v"`)
	if err != nil {
		b.Fatal(err)
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := p.newBoolExpr(expr)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if _, err := evaluator.Eval(ctx, nil); err != nil {
		b.Skipf("VM does not support this pattern: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := evaluator.Eval(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		benchBoolSink = res
	}
}

func BenchmarkOTTLComparisonRealWorld_VM(b *testing.B) {
	// Set up path values that make the expression return true
	pathValues := map[string]any{
		"attributes[level]":                                    "INFO",
		"resource.attributes[level]":                           "INFO",
		"instrumentation_scope.attributes[level]":              "INFO",
		"body[level]":                                          nil,
		"body.string":                                          "Normal log message",
		"attributes[service]":                                  "bigid-ml-foo",
		"attributes[ml_scan_id]":                               nil,
		"resource.attributes[ml_scan_id]":                      nil,
		"instrumentation_scope.attributes[ml_scan_id]":         nil,
		"body[ml_scan_id]":                                     nil,
		"attributes[scan_id]":                                  nil,
		"resource.attributes[scan_id]":                         nil,
		"instrumentation_scope.attributes[scan_id]":            nil,
		"body[scan_id]":                                        nil,
		"attributes[chunks]":                                   nil,
		"resource.attributes[chunks]":                          nil,
		"instrumentation_scope.attributes[chunks]":             nil,
		"body[chunks]":                                         nil,
		"attributes[training_iteration]":                       nil,
		"resource.attributes[training_iteration]":              nil,
		"instrumentation_scope.attributes[training_iteration]": nil,
		"body[training_iteration]":                             nil,
		"attributes[queue_status]":                             nil,
		"resource.attributes[queue_status]":                    nil,
		"instrumentation_scope.attributes[queue_status]":       nil,
		"body[queue_status]":                                   nil,
		"resource.attributes[ddtags][pod_name]":                "some-other-pod",
		"attributes[sawmills][index_name]":                     nil,
		"body":                                                 "string body",
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(realWorldExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	if _, err := evaluator.Eval(ctx, nil); err != nil {
		b.Skipf("VM does not support this pattern: %v", err)
	}

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

// Deep condition benchmark: 4-level nested boolean from Sawmills test suite
const deepConditionExpr = `(
	(
		attributes["k1"] == "v1" and attributes["k2"] == "v2"
	)
	or 
	(
		attributes["k3"] == "v" and attributes["k4"] == "v"
	)
) and (
	(
		(attributes["k5"] == "v" and attributes["k9"] == "v9") or (attributes["k6"] == "v")
	)
	or
	(
		(attributes["k7"] == "v7") or (attributes["k8"] == "v")
	)
)`

func BenchmarkOTTLInterpreterDeepCondition(b *testing.B) {
	pathValues := map[string]any{
		"attributes[k1]": "v1",
		"attributes[k2]": "v2",
		"attributes[k3]": "v3",
		"attributes[k4]": "v4",
		"attributes[k5]": "v5",
		"attributes[k6]": "v6",
		"attributes[k7]": "v7",
		"attributes[k8]": "v8",
		"attributes[k9]": "v9",
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(deepConditionExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonDeepCondition_VM(b *testing.B) {
	pathValues := map[string]any{
		"attributes[k1]": "v1",
		"attributes[k2]": "v2",
		"attributes[k3]": "v3",
		"attributes[k4]": "v4",
		"attributes[k5]": "v5",
		"attributes[k6]": "v6",
		"attributes[k7]": "v7",
		"attributes[k8]": "v8",
		"attributes[k9]": "v9",
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(deepConditionExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	if _, err := evaluator.Eval(ctx, nil); err != nil {
		b.Skipf("VM does not support this pattern: %v", err)
	}

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

// Multi-path or-fallback pattern: Check same key across multiple paths (Sawmills pattern)
const multiPathOrFallbackExpr = `(
	attributes["level"] == "ERROR" 
	or resource.attributes["level"] == "ERROR" 
	or instrumentation_scope.attributes["level"] == "ERROR" 
	or (IsMap(body) and body["level"] == "ERROR")
)`

func BenchmarkOTTLInterpreterMultiPathOrFallback(b *testing.B) {
	pathValues := map[string]any{
		"attributes[level]":                       "INFO",
		"resource.attributes[level]":              "INFO",
		"instrumentation_scope.attributes[level]": "ERROR", // Match on 3rd path
		"body[level]":                             nil,
		"body":                                    "string body", // not a map
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(multiPathOrFallbackExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonMultiPathOrFallback_VM(b *testing.B) {
	pathValues := map[string]any{
		"attributes[level]":                       "INFO",
		"resource.attributes[level]":              "INFO",
		"instrumentation_scope.attributes[level]": "ERROR",
		"body[level]":                             nil,
		"body":                                    "string body",
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(multiPathOrFallbackExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	if _, err := evaluator.Eval(ctx, nil); err != nil {
		b.Skipf("VM does not support this pattern: %v", err)
	}

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

// Nested attribute 3-level access with IsMap guards (Sawmills pattern)
const nestedAttr3LevelExpr = `(
	IsMap(attributes["string"]) 
	and IsMap(attributes["string"]["attribute"]) 
	and IsMap(attributes["string"]["attribute"]["not"]) 
	and attributes["string"]["attribute"]["not"]["exist"] == "found"
)`

func BenchmarkOTTLInterpreterNestedAttr3Level(b *testing.B) {
	// Simulate nested map structure
	pathValues := map[string]any{
		"attributes[string]":                        pcommon.NewMap(),
		"attributes[string][attribute]":             pcommon.NewMap(),
		"attributes[string][attribute][not]":        pcommon.NewMap(),
		"attributes[string][attribute][not][exist]": "found",
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(nestedAttr3LevelExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonNestedAttr3Level_VM(b *testing.B) {
	// Simulate nested map structure
	pathValues := map[string]any{
		"attributes[string]":                        pcommon.NewMap(),
		"attributes[string][attribute]":             pcommon.NewMap(),
		"attributes[string][attribute][not]":        pcommon.NewMap(),
		"attributes[string][attribute][not][exist]": "found",
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(nestedAttr3LevelExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

// Range comparison pattern: HTTP status code filtering (common pattern)
const rangeComparisonExpr = `(
	(attributes["http.status_code"] >= 400 and attributes["http.status_code"] <= 499) 
	or (IsMap(body) and (body["http.status_code"] >= 400 and body["http.status_code"] <= 499))
)`

func BenchmarkOTTLInterpreterRangeComparison(b *testing.B) {
	pathValues := map[string]any{
		"attributes[http.status_code]": int64(404),
		"body[http.status_code]":       nil,
		"body":                         "string body", // not a map
	}

	p, err := newBenchParserForRealWorld(false, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(rangeComparisonExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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

func BenchmarkOTTLComparisonRangeComparison_VM(b *testing.B) {
	pathValues := map[string]any{
		"attributes[http.status_code]": int64(404),
		"body[http.status_code]":       nil,
		"body":                         "string body", // not a map
	}

	p, err := newBenchParserForRealWorld(true, pathValues)
	if err != nil {
		b.Fatal(err)
	}

	expr, err := parseCondition(rangeComparisonExpr)
	if err != nil {
		b.Fatal(err)
	}

	evaluator, err := p.newBoolExpr(expr)
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
