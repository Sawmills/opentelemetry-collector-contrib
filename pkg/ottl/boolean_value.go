// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"errors"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.uber.org/zap"
)

// boolExpressionEvaluator is a function that returns the result.
type boolExpressionEvaluator[K any] func(ctx context.Context, tCtx K) (bool, error)

// BoolExpr represents a condition in OTTL
type BoolExpr[K any] struct {
	boolExpressionEvaluator[K]
}

// Eval evaluates an OTTL condition
func (e BoolExpr[K]) Eval(ctx context.Context, tCtx K) (bool, error) {
	return e.boolExpressionEvaluator(ctx, tCtx)
}

//nolint:unparam
func not[K any](original BoolExpr[K]) (BoolExpr[K], error) {
	return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
		result, err := original.Eval(ctx, tCtx)
		return !result, err
	}}, nil
}

func alwaysTrue[K any](context.Context, K) (bool, error) {
	return true, nil
}

func alwaysFalse[K any](context.Context, K) (bool, error) {
	return false, nil
}

// builds a function that returns a short-circuited result of ANDing
// boolExpressionEvaluator funcs
func andFuncs[K any](funcs []BoolExpr[K]) BoolExpr[K] {
	return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
		for _, f := range funcs {
			result, err := f.Eval(ctx, tCtx)
			if err != nil {
				return false, err
			}
			if !result {
				return false, nil
			}
		}
		return true, nil
	}}
}

// builds a function that returns a short-circuited result of ORing
// boolExpressionEvaluator funcs
func orFuncs[K any](funcs []BoolExpr[K]) BoolExpr[K] {
	return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
		for _, f := range funcs {
			result, err := f.Eval(ctx, tCtx)
			if err != nil {
				return false, err
			}
			if result {
				return true, nil
			}
		}
		return false, nil
	}}
}

func runBoolVMConstOnly[K any](program *microProgram[K]) (bool, error) {
	var stack []ir.Value
	var holder *vm.StackHolder
	fromPool := false
	if program.stackPool != nil {
		holder = program.stackPool.GetHolder()
		stack = holder.Stack
		if program.stack > len(stack) {
			program.stackPool.PutHolder(holder)
			stack = nil
			holder = nil
		} else {
			fromPool = true
		}
	}
	if stack == nil {
		if program.stack > defaultMicroVMStackSize {
			stack = make([]ir.Value, program.stack)
		} else {
			var stackArr [defaultMicroVMStackSize]ir.Value
			stack = stackArr[:program.stack]
		}
	}
	val, err := vm.RunWithStackGeneric(stack[:program.stack], program.program)
	if fromPool {
		program.stackPool.PutHolder(holder)
	}
	if err != nil {
		return false, err
	}
	result, ok := val.Bool()
	if !ok {
		return false, fmt.Errorf("vm result is not bool")
	}
	return result, nil
}

func (p *Parser[K]) tryInlineSuperinstruction(program *microProgram[K]) BoolExpr[K] {
	code := program.program.Code

	if len(code) == 1 {
		return p.tryInline1Inst(program)
	}
	if len(code) == 2 {
		return p.tryInline2Inst(program)
	}
	return BoolExpr[K]{}
}

func (p *Parser[K]) tryInline1Inst(program *microProgram[K]) BoolExpr[K] {
	inst := program.program.Code[0]
	op := inst.Op()
	arg := inst.Arg()

	switch op {
	case ir.OpAttrFastEqConstString:
		if program.program.AttrGetter == nil {
			return BoolExpr[K]{}
		}
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		attrGetter := program.program.AttrGetter
		key := program.program.AttrKeys[keyIdx]
		constVal := program.program.Consts[constIdx]
		return BoolExpr[K]{func(_ context.Context, tCtx K) (bool, error) {
			attrVal, err := attrGetter(tCtx, key)
			if err != nil {
				return false, err
			}
			return ir.StringsEqual(attrVal, constVal), nil
		}}

	case ir.OpAttrFastNeConstString:
		if program.program.AttrGetter == nil {
			return BoolExpr[K]{}
		}
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		attrGetter := program.program.AttrGetter
		key := program.program.AttrKeys[keyIdx]
		constVal := program.program.Consts[constIdx]
		return BoolExpr[K]{func(_ context.Context, tCtx K) (bool, error) {
			attrVal, err := attrGetter(tCtx, key)
			if err != nil {
				return false, err
			}
			return !ir.StringsEqual(attrVal, constVal), nil
		}}

	case ir.OpAttrEqConstString:
		attrIdx, constIdx := ir.UnpackAttrConst(arg)
		if int(attrIdx) >= len(program.program.Accessors) || program.program.Accessors[attrIdx] == nil {
			return BoolExpr[K]{}
		}
		accessor := program.program.Accessors[attrIdx]
		constVal := program.program.Consts[constIdx]
		return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return false, err
			}
			return ir.StringsEqual(attrVal, constVal), nil
		}}

	case ir.OpAttrNeConstString:
		attrIdx, constIdx := ir.UnpackAttrConst(arg)
		if int(attrIdx) >= len(program.program.Accessors) || program.program.Accessors[attrIdx] == nil {
			return BoolExpr[K]{}
		}
		accessor := program.program.Accessors[attrIdx]
		constVal := program.program.Consts[constIdx]
		return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return false, err
			}
			return !ir.StringsEqual(attrVal, constVal), nil
		}}

	case ir.OpAttrIsMatchConst:
		attrIdx, regexIdx := ir.UnpackAttrConst(arg)
		if int(attrIdx) >= len(program.program.Accessors) || program.program.Accessors[attrIdx] == nil {
			return BoolExpr[K]{}
		}
		if int(regexIdx) >= len(program.program.Regexps) || program.program.Regexps[regexIdx] == nil {
			return BoolExpr[K]{}
		}
		accessor := program.program.Accessors[attrIdx]
		regex := program.program.Regexps[regexIdx]
		return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return false, err
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return false, nil
				}
				return regex.MatchString(str), nil
			}
			return vm.IsMatchValue(attrVal, regex)
		}}

	case ir.OpAttrFastIsMatchConst:
		keyIdx, regexIdx := ir.UnpackAttrConst(arg)
		if int(keyIdx) >= len(program.program.AttrKeys) {
			return BoolExpr[K]{}
		}
		if int(regexIdx) >= len(program.program.Regexps) || program.program.Regexps[regexIdx] == nil {
			return BoolExpr[K]{}
		}
		if program.program.AttrGetter == nil {
			return BoolExpr[K]{}
		}
		key := program.program.AttrKeys[keyIdx]
		getter := program.program.AttrGetter
		regex := program.program.Regexps[regexIdx]
		return BoolExpr[K]{func(_ context.Context, tCtx K) (bool, error) {
			attrVal, err := getter(tCtx, key)
			if err != nil {
				return false, err
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return false, nil
				}
				return regex.MatchString(str), nil
			}
			return vm.IsMatchValue(attrVal, regex)
		}}
	}

	return BoolExpr[K]{}
}

func (p *Parser[K]) tryInline2Inst(program *microProgram[K]) BoolExpr[K] {
	inst0 := program.program.Code[0]
	inst1 := program.program.Code[1]
	op0, arg0 := inst0.Op(), inst0.Arg()
	op1, arg1 := inst1.Op(), inst1.Arg()

	if op0 == ir.OpLoadAttrCached && op1 == ir.OpEqConst {
		if int(arg0) >= len(program.program.Accessors) || program.program.Accessors[arg0] == nil {
			return BoolExpr[K]{}
		}
		if int(arg1) >= len(program.program.Consts) {
			return BoolExpr[K]{}
		}
		accessor := program.program.Accessors[arg0]
		constVal := program.program.Consts[arg1]
		constType := constVal.Type
		return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			val, err := accessor(ctx, tCtx)
			if err != nil {
				return false, err
			}
			if val.Type == ir.TypeInt && constType == ir.TypeInt {
				return int64(val.Num) == int64(constVal.Num), nil
			}
			if val.Type == ir.TypeString && constType == ir.TypeString {
				return ir.StringsEqual(val, constVal), nil
			}
			return false, nil
		}}
	}

	if op0 == ir.OpLoadAttrCached && op1 == ir.OpNeConst {
		if int(arg0) >= len(program.program.Accessors) || program.program.Accessors[arg0] == nil {
			return BoolExpr[K]{}
		}
		if int(arg1) >= len(program.program.Consts) {
			return BoolExpr[K]{}
		}
		accessor := program.program.Accessors[arg0]
		constVal := program.program.Consts[arg1]
		constType := constVal.Type
		return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			val, err := accessor(ctx, tCtx)
			if err != nil {
				return false, err
			}
			if val.Type == ir.TypeInt && constType == ir.TypeInt {
				return int64(val.Num) != int64(constVal.Num), nil
			}
			if val.Type == ir.TypeString && constType == ir.TypeString {
				return !ir.StringsEqual(val, constVal), nil
			}
			return true, nil
		}}
	}

	return BoolExpr[K]{}
}

func (p *Parser[K]) runBoolVM(ctx context.Context, tCtx K, program *microProgram[K]) (bool, error) {
	var stack []ir.Value
	var holder *vm.StackHolder
	fromPool := false
	if program.stackPool != nil {
		holder = program.stackPool.GetHolder()
		stack = holder.Stack
		if program.stack > len(stack) {
			program.stackPool.PutHolder(holder)
			stack = nil
			holder = nil
		} else {
			fromPool = true
		}
	}
	if stack == nil {
		if program.stack > defaultMicroVMStackSize {
			stack = make([]ir.Value, program.stack)
		} else {
			var stackArr [defaultMicroVMStackSize]ir.Value
			stack = stackArr[:program.stack]
		}
	}

	// Use context-aware runner with pre-compiled accessors - no per-run closure allocation
	val, err := vm.RunWithStackAndContext(stack[:program.stack], program.program, ctx, tCtx)
	if fromPool {
		program.stackPool.PutHolder(holder)
	}

	if err != nil {
		p.logVMError(err)
		return false, err
	}
	result, ok := val.Bool()
	if !ok {
		return false, fmt.Errorf("vm result is not bool")
	}
	return result, nil
}

func (p *Parser[K]) newComparisonEvaluator(comparison *comparison) (BoolExpr[K], error) {
	if comparison == nil {
		return BoolExpr[K]{alwaysTrue[K]}, nil
	}

	if p.vmEnabled {
		program, err := p.getOrCompileVMProgram(comparison)
		if err == nil {
			if len(program.getters) == 0 && len(program.program.AttrKeys) == 0 && !program.needsContext {
				return BoolExpr[K]{func(context.Context, K) (bool, error) {
					result, err := runBoolVMConstOnly(program)
					if err != nil {
						p.logVMError(err)
						return false, err
					}
					return result, nil
				}}, nil
			}
			if inlined := p.tryInlineSuperinstruction(program); inlined.boolExpressionEvaluator != nil {
				return inlined, nil
			}
			return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
				return p.runBoolVM(ctx, tCtx, program)
			}}, nil
		}
	}

	left, err := p.newGetter(comparison.Left)
	if err != nil {
		return BoolExpr[K]{}, err
	}
	right, err := p.newGetter(comparison.Right)
	if err != nil {
		return BoolExpr[K]{}, err
	}

	comparator := NewValueComparator()
	return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
		a, leftErr := left.Get(ctx, tCtx)
		if leftErr != nil {
			return false, leftErr
		}
		b, rightErr := right.Get(ctx, tCtx)
		if rightErr != nil {
			return false, rightErr
		}
		return comparator.compare(a, b, comparison.Op), nil
	}}, nil
}

func (p *Parser[K]) newBoolExpr(expr *booleanExpression) (BoolExpr[K], error) {
	return p.newBoolExprWithOrigText(expr, "")
}

func (p *Parser[K]) newBoolExprWithOrigText(expr *booleanExpression, origText string) (BoolExpr[K], error) {
	if expr == nil {
		return BoolExpr[K]{alwaysTrue[K]}, nil
	}

	if p.shadowMode && p.vmEnabled {
		return p.newShadowBoolExpr(expr, origText)
	}

	if p.vmEnabled {
		program, err := p.getOrCompileVMBoolProgram(expr)
		if err == nil {
			if len(program.getters) == 0 && len(program.program.AttrKeys) == 0 && !program.needsContext {
				return BoolExpr[K]{func(context.Context, K) (bool, error) {
					result, err := runBoolVMConstOnly(program)
					if err != nil {
						p.logVMError(err)
						return false, err
					}
					return result, nil
				}}, nil
			}
			return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
				return p.runBoolVM(ctx, tCtx, program)
			}}, nil
		}
	}
	return p.newInterpreterBoolExpr(expr)
}

func (p *Parser[K]) newInterpreterBoolExpr(expr *booleanExpression) (BoolExpr[K], error) {
	f, err := p.newBooleanTermEvaluator(expr.Left)
	if err != nil {
		return BoolExpr[K]{}, err
	}
	funcs := []BoolExpr[K]{f}
	for _, rhs := range expr.Right {
		f, err := p.newBooleanTermEvaluator(rhs.Term)
		if err != nil {
			return BoolExpr[K]{}, err
		}
		funcs = append(funcs, f)
	}

	return orFuncs(funcs), nil
}

func (p *Parser[K]) newShadowBoolExpr(expr *booleanExpression, origText string) (BoolExpr[K], error) {
	interpExpr, err := p.newInterpreterBoolExpr(expr)
	if err != nil {
		return BoolExpr[K]{}, err
	}

	program, err := p.getOrCompileVMBoolProgram(expr)
	if err != nil {
		return interpExpr, nil
	}

	var vmExpr BoolExpr[K]
	if len(program.getters) == 0 && len(program.program.AttrKeys) == 0 && !program.needsContext {
		vmExpr = BoolExpr[K]{func(context.Context, K) (bool, error) {
			return runBoolVMConstOnly(program)
		}}
	} else {
		vmExpr = BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
			return p.runBoolVM(ctx, tCtx, program)
		}}
	}

	return wrapWithShadow(interpExpr, vmExpr, origText, p.telemetrySettings.Logger), nil
}

func (p *Parser[K]) logVMError(err error) {
	if errors.Is(err, vm.ErrGasExhausted) {
		p.telemetrySettings.Logger.Warn("OTTL VM gas exhausted", zap.Error(err))
		return
	}
	p.telemetrySettings.Logger.Debug("OTTL VM execution error", zap.Error(err))
}

func (p *Parser[K]) newBooleanTermEvaluator(term *term) (BoolExpr[K], error) {
	if term == nil {
		return BoolExpr[K]{alwaysTrue[K]}, nil
	}
	f, err := p.newBooleanValueEvaluator(term.Left)
	if err != nil {
		return BoolExpr[K]{}, err
	}
	funcs := []BoolExpr[K]{f}
	for _, rhs := range term.Right {
		f, err := p.newBooleanValueEvaluator(rhs.Value)
		if err != nil {
			return BoolExpr[K]{}, err
		}
		funcs = append(funcs, f)
	}

	return andFuncs(funcs), nil
}

func (p *Parser[K]) newBooleanValueEvaluator(value *booleanValue) (BoolExpr[K], error) {
	if value == nil {
		return BoolExpr[K]{alwaysTrue[K]}, nil
	}

	var boolExpr BoolExpr[K]
	var err error
	switch {
	case value.Comparison != nil:
		boolExpr, err = p.newComparisonEvaluator(value.Comparison)
		if err != nil {
			return BoolExpr[K]{}, err
		}
	case value.ConstExpr != nil:
		switch {
		case value.ConstExpr.Boolean != nil:
			if *value.ConstExpr.Boolean {
				boolExpr = BoolExpr[K]{alwaysTrue[K]}
			} else {
				boolExpr = BoolExpr[K]{alwaysFalse[K]}
			}
		case value.ConstExpr.Converter != nil:
			boolExpr, err = p.newConverterEvaluator(*value.ConstExpr.Converter)
			if err != nil {
				return BoolExpr[K]{}, err
			}
		default:
			return BoolExpr[K]{}, fmt.Errorf("unhandled boolean operation %v", value)
		}
	case value.SubExpr != nil:
		boolExpr, err = p.newBoolExpr(value.SubExpr)
		if err != nil {
			return BoolExpr[K]{}, err
		}
	default:
		return BoolExpr[K]{}, fmt.Errorf("unhandled boolean operation %v", value)
	}

	if value.Negation != nil {
		return not(boolExpr)
	}
	return boolExpr, nil
}

func (p *Parser[K]) newConverterEvaluator(c converter) (BoolExpr[K], error) {
	getter, err := p.newGetterFromConverter(c)
	if err != nil {
		return BoolExpr[K]{}, err
	}
	return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
		result, err := getter.Get(ctx, tCtx)
		if err != nil {
			return false, err
		}
		boolResult, ok := result.(bool)
		if !ok {
			return false, fmt.Errorf("value returned from Converter in constant expression must be bool but got %T", result)
		}
		return boolResult, nil
	}}, nil
}
