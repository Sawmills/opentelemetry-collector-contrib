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

type pcommonStringLikeTestGetter struct {
	pcommonStringTestGetter
}

func (g pcommonStringLikeTestGetter) Get(context.Context, any) (any, error) {
	return g.value, g.err
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

func TestPcommonStringLikeGetterUsesValueWithoutBoxing(t *testing.T) {
	cases := []struct {
		name  string
		value pcommon.Value
		found bool
		want  *string
	}{
		{name: "missing", want: nil},
		{name: "empty", value: pcommon.NewValueEmpty(), found: true, want: nil},
		{name: "string", value: pcommon.NewValueStr("hello"), found: true, want: ptr("hello")},
		{name: "bool", value: pcommon.NewValueBool(true), found: true, want: ptr("true")},
		{name: "integer", value: pcommon.NewValueInt(42), found: true, want: ptr("42")},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			getter := standardStringLikeGetterWithPcommonValue[any]{
				getter:   pcommonStringTestGetter{value: tt.value, found: tt.found},
				standard: StandardStringLikeGetter[any]{},
			}

			got, err := getter.Get(t.Context(), nil)
			require.NoError(t, err)
			if tt.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, *tt.want, *got)
		})
	}
}

func TestPcommonStringLikeGetterPreservesLookupError(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	g := standardStringLikeGetterWithPcommonValue[any]{
		getter:   pcommonStringTestGetter{err: lookupErr},
		standard: StandardStringLikeGetter[any]{},
	}

	_, err := g.Get(t.Context(), nil)

	require.ErrorIs(t, err, lookupErr)
}

func TestNewStandardStringLikeGetterUsesPcommonValue(t *testing.T) {
	g, err := newStandardStringLikeGetter[any](pcommonStringLikeTestGetter{
		pcommonStringTestGetter: pcommonStringTestGetter{
			value: pcommon.NewValueStr("hello"),
			found: true,
		},
	})

	require.NoError(t, err)
	_, ok := g.(standardStringLikeGetterWithPcommonValue[any])
	require.True(t, ok)
	got, err := g.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, "hello", *got)
}

func TestNewStandardPMapGetterUsesPcommonValue(t *testing.T) {
	g, err := newStandardPMapGetter[any](pcommonStringLikeTestGetter{
		pcommonStringTestGetter: pcommonStringTestGetter{
			value: pcommon.NewValueMap(),
			found: true,
		},
	})

	require.NoError(t, err)
	_, ok := g.(pMapGetterWithPcommonValue[any])
	require.True(t, ok)
	got, err := g.Get(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 0, got.Len())
}

func TestPcommonMapGetterTypeErrorsMatchStandardGetter(t *testing.T) {
	g := pMapGetterWithPcommonValue[any]{
		getter: pcommonStringLikeTestGetter{
			pcommonStringTestGetter: pcommonStringTestGetter{
				value: pcommon.NewValueBool(true),
				found: true,
			},
		},
	}

	_, err := g.Get(t.Context(), nil)

	var typeErr TypeError
	require.ErrorAs(t, err, &typeErr)
	require.EqualError(t, err, "expected pcommon.Map but got Bool")
}

func TestPcommonMapGetterEmptyValueMatchesStandardGetter(t *testing.T) {
	g := pMapGetterWithPcommonValue[any]{
		getter: pcommonStringLikeTestGetter{
			pcommonStringTestGetter: pcommonStringTestGetter{
				value: pcommon.NewValueEmpty(),
				found: true,
			},
		},
	}

	_, err := g.Get(t.Context(), nil)

	var typeErr TypeError
	require.ErrorAs(t, err, &typeErr)
	require.EqualError(t, err, "expected pcommon.Map but got Empty")
}
