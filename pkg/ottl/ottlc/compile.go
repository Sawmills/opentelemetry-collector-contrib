// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package ottlc provides standalone compilation functions for the OTTL bytecode VM.
//
// EXPERIMENTAL: This package is experimental and may change in future releases.
package ottlc // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlc"

import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

// Program is an alias for the compiled VM program wrapper.
//
// EXPERIMENTAL: This type is experimental and may change in future releases.
type Program[K any] = ottl.VMProgram[K]

// CompileCondition parses a condition string and compiles it into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func CompileCondition[K any](parser *ottl.Parser[K], condition string) (*Program[K], error) {
	return parser.CompileConditionVM(condition)
}

// CompileBoolExpression compiles a parsed boolean expression into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func CompileBoolExpression[K any](parser *ottl.Parser[K], expr *ottl.BooleanExpression) (*Program[K], error) {
	return parser.CompileBoolExpressionVM(expr)
}

// CompileComparison compiles a parsed comparison into VM bytecode.
//
// EXPERIMENTAL: This API is experimental and may change in future releases.
func CompileComparison[K any](parser *ottl.Parser[K], cmp *ottl.Comparison) (*Program[K], error) {
	return parser.CompileComparisonVM(cmp)
}
