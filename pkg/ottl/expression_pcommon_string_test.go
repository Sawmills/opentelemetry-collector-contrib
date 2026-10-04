// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type pcommonStringTestGetter struct {
	value pcommon.Value
	found bool
	err   error
}

func (g pcommonStringTestGetter) GetPcommonValue(context.Context, any) (pcommon.Value, bool, error) {
	return g.value, g.found, g.err
}

func TestPcommonStringGetterMissingValue(t *testing.T) {
	g := stringGetterWithPcommonValue[any]{getter: pcommonStringTestGetter{}}
	var typeErr TypeError

	_, err := g.Get(t.Context(), nil)

	require.EqualError(t, err, "expected string but got nil")
	require.ErrorAs(t, err, &typeErr)
}

func TestPcommonStringGetterEmptyValue(t *testing.T) {
	g := stringGetterWithPcommonValue[any]{getter: pcommonStringTestGetter{value: pcommon.NewValueEmpty(), found: true}}
	var typeErr TypeError

	_, err := g.Get(t.Context(), nil)

	require.EqualError(t, err, "expected string but got nil")
	require.ErrorAs(t, err, &typeErr)
}

func TestPcommonStringGetterBooleanTypeError(t *testing.T) {
	g := stringGetterWithPcommonValue[any]{getter: pcommonStringTestGetter{value: pcommon.NewValueBool(true), found: true}}
	var typeErr TypeError

	_, err := g.Get(t.Context(), nil)

	require.EqualError(t, err, "expected string but got bool")
	require.ErrorAs(t, err, &typeErr)
}

func TestPcommonStringGetterBytesTypeError(t *testing.T) {
	g := stringGetterWithPcommonValue[any]{getter: pcommonStringTestGetter{value: pcommon.NewValueBytes(), found: true}}
	var typeErr TypeError

	_, err := g.Get(t.Context(), nil)

	require.EqualError(t, err, "expected string but got []uint8")
	require.ErrorAs(t, err, &typeErr)
}

func TestPcommonStringGetterPreservesLookupError(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	g := stringGetterWithPcommonValue[any]{getter: pcommonStringTestGetter{err: lookupErr}}

	_, err := g.Get(t.Context(), nil)

	require.EqualError(t, err, "error getting value in ottl.StandardStringGetter[interface {}]: lookup failed")
	require.ErrorIs(t, err, lookupErr)
}
