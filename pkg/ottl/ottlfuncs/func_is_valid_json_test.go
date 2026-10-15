// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

func TestIsValidJSONAcceptsValidObject(t *testing.T) {
	body := `{"message":"synthetic","nested":{"values":[true,null,42]}}`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.True(t, valid)
}

func TestIsValidJSONRejectsNumberOutsideDecoderRange(t *testing.T) {
	body := `{"number":1e10000}`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsMalformedObject(t *testing.T) {
	body := `{not json}`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsTruncatedObject(t *testing.T) {
	body := `{"message":`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsTrailingInput(t *testing.T) {
	body := `{"message":"synthetic"} trailing`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsExtraClosingBrace(t *testing.T) {
	body := `{"message":"synthetic"}}`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsExtraClosingBracket(t *testing.T) {
	body := `{"message":"synthetic"}]`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsEmptyString(t *testing.T) {
	body := ""

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsMapValue(t *testing.T) {
	body := map[string]any{"message": "synthetic"}

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsIntegerValue(t *testing.T) {
	body := int64(42)

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONRejectsNilValue(t *testing.T) {
	valid, err := validJSONValue(t.Context(), nil)

	require.NoError(t, err)
	require.False(t, valid)
}

func TestIsValidJSONAcceptsArray(t *testing.T) {
	body := `["synthetic",42,true,null]`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.True(t, valid)
}

func TestIsValidJSONAcceptsScalarJSON(t *testing.T) {
	body := `null`

	valid, err := validJSONValue(t.Context(), body)

	require.NoError(t, err)
	require.True(t, valid)
}

func TestIsValidJSONPropagatesGetterFailure(t *testing.T) {
	failure := errors.New("synthetic getter failure")
	getter := ottl.StandardStringGetter[any]{Getter: func(context.Context, any) (any, error) {
		return nil, failure
	}}

	result, err := evaluateJSONFactory(t.Context(), &getter)

	require.ErrorIs(t, err, failure)
	require.Nil(t, result)
}

func TestIsValidJSONRejectsIncorrectFactoryArguments(t *testing.T) {
	factory := ottlfuncs.NewIsValidJSONFactory[any]()

	_, err := factory.CreateFunction(ottl.FunctionContext{}, &ottlfuncs.IsStringArguments[any]{})

	require.ErrorContains(t, err, "IsValidJSONFactory args must be of type")
}

func evaluateJSONFactory(ctx context.Context, getter ottl.StringGetter[any]) (any, error) {
	factory := ottlfuncs.NewIsValidJSONFactory[any]()
	function, err := factory.CreateFunction(ottl.FunctionContext{}, &ottlfuncs.IsValidJSONArguments[any]{Target: getter})
	if err != nil {
		return nil, err
	}
	return function(ctx, nil)
}

func validJSONValue(ctx context.Context, value any) (bool, error) {
	getter := ottl.StandardStringGetter[any]{Getter: func(context.Context, any) (any, error) {
		return value, nil
	}}
	result, err := evaluateJSONFactory(ctx, &getter)
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}
