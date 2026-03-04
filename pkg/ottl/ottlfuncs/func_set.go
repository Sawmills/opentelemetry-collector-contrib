// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"

import (
	"context"
	"errors"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
)

type SetArguments[K any] struct {
	Target ottl.Setter[K]
	Value  ottl.Getter[K]
}

func NewSetFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("set", &SetArguments[K]{}, createSetFunction[K])
}

func createSetFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*SetArguments[K])

	if !ok {
		return nil, errors.New("SetFactory args must be of type *SetArguments[K]")
	}

	return set(args.Target, args.Value), nil
}

func set[K any](target ottl.Setter[K], value ottl.Getter[K]) ottl.ExprFunc[K] {
	if vmTarget, ok := target.(ottl.VMAttrSetterProvider[K]); ok {
		if _, ok := value.(ottl.LiteralGetter); ok {
			raw, err := value.Get(context.Background(), *new(K))
			if err == nil && raw != nil {
				if vmVal, ok := valueToVMConst(raw); ok {
					if key, ok := vmTarget.VMAttrKey(); ok {
						if setter := vmTarget.VMAttrSetter(); setter != nil {
							program := &vm.Program[K]{
								Code: []ir.Instruction{
									ir.Encode(ir.OpLoadConst, 0),
									ir.Encode(ir.OpSetAttrFast, 0),
									ir.Encode(ir.OpLoadConst, 1),
								},
								Consts:     []ir.Value{vmVal, ir.BoolValue(true)},
								AttrKeys:   []string{key},
								AttrSetter: setter,
								GasLimit:   vm.DefaultGasLimit,
							}
							return func(ctx context.Context, tCtx K) (any, error) {
								var stack [2]ir.Value
								_, err := vm.RunWithStackAndContext(stack[:], program, ctx, tCtx)
								if err != nil {
									return nil, err
								}
								return nil, nil
							}
						}
					}
				}
			}
		}
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		val, err := value.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}

		// No fields currently support `null` as a valid type.
		if val != nil {
			err = target.Set(ctx, tCtx, val)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
}

func valueToVMConst(val any) (ir.Value, bool) {
	switch v := val.(type) {
	case int64:
		return ir.Int64Value(v), true
	case float64:
		return ir.Float64Value(v), true
	case bool:
		return ir.BoolValue(v), true
	case string:
		return ir.StringValue(v), true
	default:
		return ir.Value{}, false
	}
}
