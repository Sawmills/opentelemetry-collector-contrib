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

	ctx := context.Background()

	target, err := args.Target.Get(ctx, *new(K))
	if err != nil {
		return nil, fmt.Errorf("target must be a number")
	}
	if target == nil {
		return nil, fmt.Errorf("target value is nil")
	}

	min, err := args.Min.Get(ctx, *new(K))
	if err != nil {
		return nil, fmt.Errorf("min must be a number")
	}
	if min == nil {
		return nil, fmt.Errorf("min value is nil")
	}
	minFloat := *min

	max, err := args.Max.Get(ctx, *new(K))
	if err != nil {
		return nil, fmt.Errorf("max must be a number")
	}
	if max == nil {
		return nil, fmt.Errorf("max value is nil")
	}
	maxFloat := *max

	if minFloat > maxFloat {
		return nil, fmt.Errorf("min must be less than or equal to max")
	}

	return func(ctx context.Context, tCtx K) (any, error) {
		target, err := args.Target.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return nil, fmt.Errorf("target value is nil")
		}

		targetFloat := *target
		return targetFloat >= minFloat && targetFloat <= maxFloat, nil
	}, nil
}
