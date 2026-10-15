// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

func TestTryParseJSONPreservesParseJSONFields(t *testing.T) {
	for _, body := range []string{
		`{"message":"synthetic","nested":{"values":[true,null,42]},"number":1.5}`,
		` {"escaped":"line\nvalue","duplicate":1,"duplicate":2} `,
		`["synthetic",42,true,null,{"nested":[]}]`,
		`{}`, `[]`,
	} {
		getter := ottl.StandardStringGetter[any]{Getter: func(context.Context, any) (any, error) { return body, nil }}
		parse, err := ottlfuncs.NewParseJSONFactory[any]().CreateFunction(ottl.FunctionContext{}, &ottlfuncs.ParseJSONArguments[any]{Target: getter})
		require.NoError(t, err)
		want, err := parse(t.Context(), nil)
		require.NoError(t, err)
		got, err := evaluateTryParseJSON(t.Context(), getter)
		require.NoError(t, err)
		switch expected := want.(type) {
		case pcommon.Map:
			require.Equal(t, expected.AsRaw(), got.(pcommon.Map).AsRaw())
		case pcommon.Slice:
			require.Equal(t, expected.AsRaw(), got.(pcommon.Slice).AsRaw())
		default:
			t.Fatalf("unexpected ParseJSON result: %T", want)
		}
	}
}

func TestTryParseJSONReturnsNilForInvalidOrNonStringInput(t *testing.T) {
	for _, value := range []any{
		`{not json}`, `{"message":`, `{"message":"synthetic"}}`,
		`{"message":"synthetic"}]`, `{"message":"synthetic"} trailing`,
		`{"number":1e10000}`, "", "plain text", "null", "true", "42", `"scalar"`,
		nil, int64(42), true, map[string]any{"message": "synthetic"},
	} {
		getter := ottl.StandardStringGetter[any]{Getter: func(context.Context, any) (any, error) { return value, nil }}
		result, err := evaluateTryParseJSON(t.Context(), getter)
		require.NoError(t, err, "input=%#v", value)
		require.Nil(t, result, "input=%#v", value)
	}
}

func TestTryParseJSONReturnsNilForGetterFailure(t *testing.T) {
	getter := ottl.StandardStringGetter[any]{Getter: func(context.Context, any) (any, error) {
		return nil, errors.New("synthetic getter failure")
	}}
	result, err := evaluateTryParseJSON(t.Context(), getter)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestTryParseJSONRejectsIncorrectFactoryArguments(t *testing.T) {
	_, err := ottlfuncs.NewTryParseJSONFactory[any]().CreateFunction(ottl.FunctionContext{}, &ottlfuncs.IsStringArguments[any]{})
	require.ErrorContains(t, err, "TryParseJSONFactory args must be of type")
}

func evaluateTryParseJSON(ctx context.Context, getter ottl.StringGetter[any]) (any, error) {
	function, err := ottlfuncs.NewTryParseJSONFactory[any]().CreateFunction(ottl.FunctionContext{}, &ottlfuncs.TryParseJSONArguments[any]{Target: getter})
	if err != nil {
		return nil, err
	}
	return function(ctx, nil)
}
