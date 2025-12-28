// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"fmt"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
)

// VMProgram exposes a compiled VM program with its getters and stack depth.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
type VMProgram[K any] struct {
	Program  *vm.Program
	Getters  []Getter[K]
	Stack    int
	pool     *vm.StackPool
	poolOnce sync.Once
}

// Run executes the compiled VM program and returns the boolean result.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func (v *VMProgram[K]) Run(ctx context.Context, tCtx K) (bool, error) {
	if v == nil || v.Program == nil {
		return false, fmt.Errorf("program is nil")
	}

	var stack []ir.Value
	if pool := v.stackPool(); pool != nil {
		stack = pool.Get()
		defer pool.Put(stack)
		stack = stack[:v.Stack]
	} else {
		stack = make([]ir.Value, v.Stack)
	}
	val, err := vm.RunWithStackAndLoader(stack, v.Program, func(idx uint32) (ir.Value, error) {
		if int(idx) >= len(v.Getters) {
			return ir.Value{}, vm.ErrInvalidGetter
		}
		raw, getErr := v.Getters[idx].Get(ctx, tCtx)
		if getErr != nil {
			return ir.Value{}, getErr
		}
		return valueToVM(raw)
	})
	if err != nil {
		return false, err
	}

	result, ok := val.Bool()
	if !ok {
		return false, fmt.Errorf("vm result is not bool")
	}
	return result, nil
}

func (v *VMProgram[K]) stackPool() *vm.StackPool {
	if v == nil || v.Stack <= 0 {
		return nil
	}
	v.poolOnce.Do(func() {
		v.pool = vm.NewStackPool(v.Stack)
	})
	return v.pool
}

// CompileConditionVM parses a condition string and compiles it into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func (p *Parser[K]) CompileConditionVM(condition string) (*VMProgram[K], error) {
	if p == nil {
		return nil, fmt.Errorf("parser is nil")
	}
	expr, err := parseCondition(condition)
	if err != nil {
		return nil, err
	}
	return p.CompileBoolExpressionVM(expr)
}

// CompileBoolExpressionVM compiles a parsed boolean expression into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func (p *Parser[K]) CompileBoolExpressionVM(expr *BooleanExpression) (*VMProgram[K], error) {
	if p == nil {
		return nil, fmt.Errorf("parser is nil")
	}
	if expr == nil {
		return nil, fmt.Errorf("boolean expression is nil")
	}
	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		return nil, err
	}
	return &VMProgram[K]{
		Program: program.program,
		Getters: program.getters,
		Stack:   program.stack,
	}, nil
}

// CompileComparisonVM compiles a parsed comparison into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func (p *Parser[K]) CompileComparisonVM(cmp *Comparison) (*VMProgram[K], error) {
	if p == nil {
		return nil, fmt.Errorf("parser is nil")
	}
	if cmp == nil {
		return nil, fmt.Errorf("comparison is nil")
	}
	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		return nil, err
	}
	return &VMProgram[K]{
		Program: program.program,
		Getters: program.getters,
		Stack:   program.stack,
	}, nil
}
