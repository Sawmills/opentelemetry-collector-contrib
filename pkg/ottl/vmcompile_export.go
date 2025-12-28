// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
)

// VMProgram exposes a compiled VM program with its getters and stack depth.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
type VMProgram[K any] struct {
	Program *vm.Program[K]
	Getters []Getter[K]
	Stack   int
}

// Run executes the compiled VM program and returns the boolean result.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func (v *VMProgram[K]) Run(ctx context.Context, tCtx K) (bool, error) {
	if v == nil || v.Program == nil {
		return false, fmt.Errorf("program is nil")
	}

	// Use stack-allocated array instead of sync.Pool to avoid alloc overhead
	var stackArr [defaultMicroVMStackSize]ir.Value
	stack := stackArr[:v.Stack]

	// Use context-aware runner with pre-compiled accessors
	val, err := vm.RunWithStackAndContext(stack, v.Program, ctx, tCtx)
	if err != nil {
		return false, err
	}

	result, ok := val.Bool()
	if !ok {
		return false, fmt.Errorf("vm result is not bool")
	}
	return result, nil
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
