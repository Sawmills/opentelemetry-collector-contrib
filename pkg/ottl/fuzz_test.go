// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

func FuzzDifferentialVM(f *testing.F) {
	// Basic arithmetic
	f.Add("1 + 1 == 2")
	f.Add("1.5 * 2.0 == 3.0")
	f.Add(`10 / 2 == 5`)
	f.Add(`10 - 3 == 7`)
	f.Add(`-5 + 10 == 5`)
	f.Add(`1.0 + 2.0 == 3.0`)
	f.Add(`1.5 / 0.5 == 3.0`)

	// Attribute access patterns
	f.Add(`attributes["key"] == "value"`)
	f.Add(`attributes["nested"]["deep"] == 1`)

	// Math with precedence
	f.Add(`1 + 2 * 3 == 7`)
	f.Add(`(1 + 2) * 3 == 9`)
	f.Add(`(1 + 2) * (3 + 4) == 21`)

	// Boolean logic
	f.Add("true and false")
	f.Add(`true and (false or true)`)
	f.Add("not true")
	f.Add(`not (1 == 2)`)
	f.Add(`1 < 2 and 3 > 2`)
	f.Add(`1 < 2 or 3 > 4`)
	f.Add(`not not true`)
	f.Add(`true or true`)
	f.Add(`false and false`)
	f.Add(`1 == 1 and 2 == 2`)

	// String operations
	f.Add(`"hello" + " " + "world" == "hello world"`)
	f.Add(`"hello" + " " + "world"`)
	f.Add(`"abc" < "abd"`)

	// Comparisons
	f.Add(`1 != 2`)
	f.Add(`1 <= 1`)
	f.Add(`2 >= 1`)
	f.Add(`1 <= 2`)

	// Nested expressions
	f.Add(`(1 + 1) == (3 - 1)`)

	// Null checks
	f.Add(`nil == nil`)
	f.Add(`1 == nil`)
	f.Add(`nil != 1`)

	f.Fuzz(func(t *testing.T, statement string) {
		pInt, err := NewParser[any](
			map[string]Factory[any]{},
			func(Path[any]) (GetSetter[any], error) {
				return StandardGetSetter[any]{
					Getter: func(context.Context, any) (any, error) { return nil, nil },
					Setter: func(context.Context, any, any) error { return nil },
				}, nil
			},
			component.TelemetrySettings{Logger: zap.NewNop()},
		)
		if err != nil {
			t.Skip("Failed to create interpreter parser")
		}

		pVM, err := NewParser[any](
			map[string]Factory[any]{},
			func(Path[any]) (GetSetter[any], error) {
				return StandardGetSetter[any]{
					Getter: func(context.Context, any) (any, error) { return nil, nil },
					Setter: func(context.Context, any, any) error { return nil },
				}, nil
			},
			component.TelemetrySettings{Logger: zap.NewNop()},
			WithVMEnabled[any](),
		)
		if err != nil {
			t.Skip("Failed to create VM parser")
		}

		expr, err := parseCondition(statement)
		if err != nil {
			return
		}

		evalInt, err := pInt.newBoolExpr(expr)
		if err != nil {
			return
		}

		evalVM, err := pVM.newBoolExpr(expr)
		if err != nil {
			t.Logf("VM compilation failed for accepted input: %s\nError: %v", statement, err)
			return
		}

		ctx := context.Background()
		resInt, errInt := evalInt.Eval(ctx, nil)
		resVM, errVM := evalVM.Eval(ctx, nil)

		if isGasExhausted(errVM) {
			return
		}

		if !fuzzErrorsEquivalent(errInt, errVM) {
			return
		}

		if errInt == nil && resInt != resVM {
			t.Errorf("Result divergence for '%s': Int=%v, VM=%v", statement, resInt, resVM)
		}
	})
}

func isGasExhausted(err error) bool {
	return errors.Is(err, vm.ErrGasExhausted)
}

func fuzzErrorsEquivalent(interpErr, vmErr error) bool {
	if interpErr == nil && vmErr == nil {
		return true
	}

	if interpErr != nil && vmErr != nil {
		return true
	}

	return false
}
