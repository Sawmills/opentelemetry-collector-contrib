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
