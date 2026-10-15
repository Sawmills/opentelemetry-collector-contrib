// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"

import (
	"context"
	"errors"

	"github.com/goccy/go-json"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type IsValidJSONArguments[K any] struct {
	Target ottl.StringGetter[K]
}

func NewIsValidJSONFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("IsValidJSON", &IsValidJSONArguments[K]{}, createIsValidJSONFunction[K])
}

func createIsValidJSONFunction[K any](_ ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
	parsed, ok := args.(*IsValidJSONArguments[K])
	if !ok {
		return nil, errors.New("IsValidJSONFactory args must be of type *IsValidJSONArguments[K]")
	}
	return isValidJSON(parsed.Target), nil
}

//nolint:errorlint // Match IsString: only a direct getter type mismatch is false.
func isValidJSON[K any](target ottl.StringGetter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		value, err := target.Get(ctx, tCtx)
		if _, typeMismatch := err.(ottl.TypeError); typeMismatch {
			return false, nil
		}
		if err != nil {
			return nil, err
		}
		return json.Valid([]byte(value)), nil
	}
}
