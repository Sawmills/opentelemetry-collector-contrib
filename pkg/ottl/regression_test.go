// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
)

// Regression for fuzz-found divergence: IsMatch on float literals must align between interpreter and VM.
func TestVMIsMatchFloatLiteral(t *testing.T) {
	stmt := `IsMatch(.1E-7,"0")`

	functions := CreateFactoryMap[any](
		newBenchIsMatchFactory[any](),
		newBenchIsMapFactory[any](),
	)

	noopGetter := func(Path[any]) (GetSetter[any], error) {
		return StandardGetSetter[any]{
			Getter: func(context.Context, any) (any, error) { return nil, nil },
			Setter: func(context.Context, any, any) error { return nil },
		}, nil
	}

	pInt, err := NewParser[any](functions, noopGetter, component.TelemetrySettings{Logger: zap.NewNop()})
	if err != nil {
		t.Fatalf("interp parser: %v", err)
	}
	pVM, err := NewParser[any](functions, noopGetter, component.TelemetrySettings{Logger: zap.NewNop()}, WithVMEnabled[any]())
	if err != nil {
		t.Fatalf("vm parser: %v", err)
	}
	expr, err := parseCondition(stmt)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	eInt, err := pInt.newBoolExpr(expr)
	if err != nil {
		t.Fatalf("interp compile: %v", err)
	}
	eVM, err := pVM.newBoolExpr(expr)
	if err != nil {
		t.Fatalf("vm compile: %v", err)
	}
	if prog, progErr := pVM.getOrCompileVMBoolProgram(expr); progErr == nil {
		_ = prog // program compiled; nothing else needed
	} else {
		t.Logf("vm compile fallback: %v", progErr)
	}

	ctx := context.Background()
	rInt, errInt := eInt.Eval(ctx, nil)
	if errInt != nil {
		t.Fatalf("interp eval: %v", errInt)
	}
	rVM, errVM := eVM.Eval(ctx, nil)
	if errVM != nil {
		t.Fatalf("vm eval: %v", errVM)
	}
	if rInt != rVM {
		t.Fatalf("mismatch: interp=%v vm=%v", rInt, rVM)
	}
}

func TestVMIsMatchEmptySliceLiteral(t *testing.T) {
	stmt := `IsMatch([],"") and a!=""`

	functions := CreateFactoryMap[any](
		newBenchIsMatchFactory[any](),
		newBenchIsMapFactory[any](),
	)

	noopGetter := func(Path[any]) (GetSetter[any], error) {
		return StandardGetSetter[any]{
			Getter: func(context.Context, any) (any, error) { return "non-empty", nil },
			Setter: func(context.Context, any, any) error { return nil },
		}, nil
	}

	pInt, err := NewParser[any](functions, noopGetter, component.TelemetrySettings{Logger: zap.NewNop()})
	if err != nil {
		t.Fatalf("interp parser: %v", err)
	}
	pVM, err := NewParser[any](functions, noopGetter, component.TelemetrySettings{Logger: zap.NewNop()}, WithVMEnabled[any]())
	if err != nil {
		t.Fatalf("vm parser: %v", err)
	}

	expr, err := parseCondition(stmt)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	eInt, err := pInt.newBoolExpr(expr)
	if err != nil {
		t.Fatalf("interp compile: %v", err)
	}
	eVM, err := pVM.newBoolExpr(expr)
	if err != nil {
		t.Fatalf("vm compile: %v", err)
	}

	ctx := context.Background()
	rInt, errInt := eInt.Eval(ctx, nil)
	if errInt != nil {
		t.Fatalf("interp eval: %v", errInt)
	}
	rVM, errVM := eVM.Eval(ctx, nil)
	if errVM != nil {
		t.Fatalf("vm eval: %v", errVM)
	}
	if rInt != rVM {
		prog, err := pVM.compileMicroBoolExpression(expr)
		if err != nil {
			t.Fatalf("compile vm program: %v", err)
		}
		t.Fatalf("mismatch: interp=%v vm=%v\nVM:\n%s", rInt, rVM, vm.Disassemble(prog.program))
	}
}
