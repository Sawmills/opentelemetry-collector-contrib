package sawmillsfuncs

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type IsInRangeArguments[K any] struct {
	Target ottl.FloatLikeGetter[K]
	Min    ottl.FloatLikeGetter[K]
	Max    ottl.FloatLikeGetter[K]
}

func NewIsInRangeFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("IsInRange", &IsInRangeArguments[K]{}, createIsInRangeFunction[K])
}

func createIsInRangeFunction[K any](
	_ ottl.FunctionContext,
	oArgs ottl.Arguments,
) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*IsInRangeArguments[K])
	if !ok {
		return nil, fmt.Errorf("IsInRangeFactory args must be of type *IsInRangeArguments[K]")
	}

	if args.Target == nil {
		return nil, fmt.Errorf("target is required")
	}
	if args.Min == nil {
		return nil, fmt.Errorf("min is required")
	}
	if args.Max == nil {
		return nil, fmt.Errorf("max is required")
	}

	return func(ctx context.Context, tCtx K) (any, error) {
		targetFloat, err := getRequiredRangeFloat(ctx, tCtx, args.Target, "target")
		if err != nil {
			return nil, err
		}
		minFloat, err := getRequiredRangeFloat(ctx, tCtx, args.Min, "min")
		if err != nil {
			return nil, err
		}
		maxFloat, err := getRequiredRangeFloat(ctx, tCtx, args.Max, "max")
		if err != nil {
			return nil, err
		}

		if minFloat > maxFloat {
			return nil, fmt.Errorf("min must be less than or equal to max")
		}
		return targetFloat >= minFloat && targetFloat <= maxFloat, nil
	}, nil
}

func getRequiredRangeFloat[K any](
	ctx context.Context,
	tCtx K,
	getter ottl.FloatLikeGetter[K],
	name string,
) (float64, error) {
	value, err := getter.Get(ctx, tCtx)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number", name)
	}
	if value == nil {
		return 0, fmt.Errorf("%s value is nil", name)
	}
	return *value, nil
}
