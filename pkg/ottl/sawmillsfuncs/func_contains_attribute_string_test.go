// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsAttributeStringDoesNotAllocate(t *testing.T) {
	tCtx, _, _ := newContainsLogContext("")
	defer tCtx.Close()
	tCtx.GetLogRecord().Attributes().PutStr("value", strings.Repeat("x", 1024))
	condition := parseBodyStringCondition(t, `attributes["value"]`, "missing")
	var evalErr error

	allocations := testing.AllocsPerRun(100, func() {
		_, evalErr = condition.Eval(t.Context(), tCtx)
	})

	require.NoError(t, evalErr)
	require.Zero(t, allocations)
}

func TestContainsAttributeStringMissingValue(t *testing.T) {
	tCtx, _, _ := newContainsLogContext("")
	defer tCtx.Close()
	condition := parseBodyStringCondition(t, `attributes["missing"]`, "")

	got, err := condition.Eval(t.Context(), tCtx)

	require.NoError(t, err)
	require.False(t, got)
}

func TestContainsAttributeStringWrongType(t *testing.T) {
	tCtx, _, _ := newContainsLogContext("")
	defer tCtx.Close()
	tCtx.GetLogRecord().Attributes().PutInt("value", 123)
	condition := parseBodyStringCondition(t, `attributes["value"]`, "123")

	got, err := condition.Eval(t.Context(), tCtx)

	require.NoError(t, err)
	require.False(t, got)
}

func TestContainsAttributeStringNestedSlice(t *testing.T) {
	tCtx, _, _ := newContainsLogContext("")
	defer tCtx.Close()
	tCtx.GetLogRecord().Attributes().PutEmptySlice("values").AppendEmpty().SetStr("hello")
	condition := parseBodyStringCondition(t, `attributes["values"][0]`, "ell")

	got, err := condition.Eval(t.Context(), tCtx)

	require.NoError(t, err)
	require.True(t, got)
}
