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
	var stackArr [defaultMicroVMStackSize]ir.Value
	val, err := vm.RunWithStackGeneric(stackArr[:program.stack], program.program)
	if err != nil {
		return false, err
	}
	result, ok := val.Bool()
	if !ok {
		return false, fmt.Errorf("vm result is not bool")
	}
	return result, nil
}

func (p *Parser[K]) runBoolVM(ctx context.Context, tCtx K, program *microProgram[K]) (bool, error) {
	// Use stack-allocated array instead of sync.Pool to avoid 24B/1 alloc overhead
	var stackArr [defaultMicroVMStackSize]ir.Value
	stack := stackArr[:program.stack]

	// Use context-aware runner with pre-compiled accessors - no per-run closure allocation
	val, err := vm.RunWithStackAndContext(stack, program.program, ctx, tCtx)

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
			if len(program.getters) == 0 {
				return BoolExpr[K]{func(context.Context, K) (bool, error) {
					result, err := runBoolVMConstOnly(program)
					if err != nil {
						p.logVMError(err)
						return false, err
					}
					return result, nil
				}}, nil
			}
			// For simple path==const comparisons (1 getter, simple op), the direct
			// interpreter is faster than VM dispatch overhead. Fall through to use
			// the direct path for these trivial cases.
			if !isTrivialComparison(comparison, program) {
				return BoolExpr[K]{func(ctx context.Context, tCtx K) (bool, error) {
					return p.runBoolVM(ctx, tCtx, program)
				}}, nil
			}
			// Fall through to direct interpreter for trivial comparisons
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
	// The parser ensures that we'll never get an invalid comparison.Op, so we don't have to check that case.
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

// isTrivialComparison returns true for simple path==const patterns where
// the direct interpreter is faster than VM dispatch overhead.
func isTrivialComparison[K any](cmp *comparison, program *microProgram[K]) bool {
	// Trivial: exactly 1 getter (path) and simple equality/inequality
	if len(program.getters) != 1 {
		return false
	}
	// Only optimize eq/ne - relational ops may benefit from VM's typed opcodes
	switch cmp.Op {
	case eq, ne:
		return true
	default:
		return false
	}
}

func (p *Parser[K]) newBoolExpr(expr *booleanExpression) (BoolExpr[K], error) {
	if expr == nil {
		return BoolExpr[K]{alwaysTrue[K]}, nil
	}

	if p.vmEnabled {
		program, err := p.getOrCompileVMBoolProgram(expr)
		if err == nil {
			if len(program.getters) == 0 {
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
