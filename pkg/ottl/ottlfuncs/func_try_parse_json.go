// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"

import (
	"context"
	"errors"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type TryParseJSONArguments[K any] struct {
	Target ottl.StringGetter[K]
}

func NewTryParseJSONFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("TryParseJSON", &TryParseJSONArguments[K]{}, createTryParseJSONFunction[K])
}

func createTryParseJSONFunction[K any](_ ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
	parsed, ok := args.(*TryParseJSONArguments[K])
	if !ok {
		return nil, errors.New("TryParseJSONFactory args must be of type *TryParseJSONArguments[K]")
	}
	parse := parseJSON(parsed.Target)
	return func(ctx context.Context, tCtx K) (any, error) {
		value, err := parse(ctx, tCtx)
		if err != nil {
			return nil, nil
		}
		return value, nil
	}, nil
}
