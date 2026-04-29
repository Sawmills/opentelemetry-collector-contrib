// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASTExportsAreUsable(t *testing.T) {
	var (
		_ BooleanExpression
		_ Term
		_ OpOrTerm
		_ BooleanValue
		_ OpAndBooleanValue
		_ Comparison
		_ ConstExpr
		_ Value
		_ Argument
		_ MathExprLiteral
		_ ASTPath
		_ PathStruct
		_ Converter
		_ Editor
		_ MapValue
		_ List
		_ MathExpression
		_ AddSubTerm
		_ OpAddSubTerm
		_ MathValue
		_ Field
		_ ASTKey
		_ GrammerKey
		_ IsNil
		_ ByteSlice
		_ String
		_ Boolean
		_ OpMultDivValue
		_ MapItem
		_ CompareOp
		_ MathOp
	)

	require.Equal(t, Eq, CompareOpTable["=="])
	require.Equal(t, Ne, CompareOpTable["!="])
	require.Equal(t, Lt, CompareOpTable["<"])
	require.Equal(t, Lte, CompareOpTable["<="])
	require.Equal(t, Gt, CompareOpTable[">"])
	require.Equal(t, Gte, CompareOpTable[">="])
	require.Equal(t, []MathOp{Add, Sub, Mult, Div}, []MathOp{add, sub, mult, div})

	condition, err := ParseCondition("true")
	require.NoError(t, err)
	require.NotNil(t, condition)
}
