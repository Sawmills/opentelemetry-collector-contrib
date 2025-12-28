// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlc

import (
	"context"
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component/componenttest"
)

func TestCompileCondition(t *testing.T) {
	parser, err := ottl.NewParser[struct{}](
		map[string]ottl.Factory[struct{}]{},
		func(ottl.Path[struct{}]) (ottl.GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	program, err := CompileCondition(&parser, "1 + 2 == 3")
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program.Program == nil {
		t.Fatalf("expected program, got nil")
	}
	if program.Stack == 0 {
		t.Fatalf("expected non-zero stack depth")
	}
	if got := vm.Disassemble(program.Program); got == "" {
		t.Fatalf("expected disassembly output")
	}
}

func TestCompileCondition_NilParser(t *testing.T) {
	_, err := CompileCondition[struct{}](nil, "1 == 1")
	if err == nil {
		t.Fatalf("expected error for nil parser")
	}
}

func TestCompileCondition_InvalidSyntax(t *testing.T) {
	parser, err := ottl.NewParser[struct{}](
		map[string]ottl.Factory[struct{}]{},
		func(ottl.Path[struct{}]) (ottl.GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	_, err = CompileCondition(&parser, "1 ++ 2")
	if err == nil {
		t.Fatalf("expected error for invalid syntax")
	}
}

func TestCompileBoolExpression_Nil(t *testing.T) {
	parser, err := ottl.NewParser[struct{}](
		map[string]ottl.Factory[struct{}]{},
		func(ottl.Path[struct{}]) (ottl.GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	_, err = CompileBoolExpression(&parser, nil)
	if err == nil {
		t.Fatalf("expected error for nil expression")
	}
}

func TestCompileComparison_Nil(t *testing.T) {
	parser, err := ottl.NewParser[struct{}](
		map[string]ottl.Factory[struct{}]{},
		func(ottl.Path[struct{}]) (ottl.GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	_, err = CompileComparison(&parser, nil)
	if err == nil {
		t.Fatalf("expected error for nil comparison")
	}
}

type testCtx struct {
	value int64
}

func TestCompileCondition_WithPath(t *testing.T) {
	parser, err := ottl.NewParser[testCtx](
		map[string]ottl.Factory[testCtx]{},
		func(path ottl.Path[testCtx]) (ottl.GetSetter[testCtx], error) {
			return ottl.StandardGetSetter[testCtx]{
				Getter: func(_ context.Context, tCtx testCtx) (any, error) {
					return tCtx.value, nil
				},
				Setter: func(context.Context, testCtx, any) error { return nil },
			}, nil
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	program, err := CompileCondition(&parser, "foo == 42")
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program.Program == nil {
		t.Fatalf("expected program, got nil")
	}
	if len(program.Getters) == 0 {
		t.Fatalf("expected getters for path expression")
	}

	// Run via VM to verify end-to-end
	ctx := context.Background()
	tCtx := testCtx{value: 42}
	result, err := program.Run(ctx, tCtx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !result {
		t.Fatalf("expected true, got false")
	}

	// Test with non-matching value
	tCtx.value = 99
	result, err = program.Run(ctx, tCtx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if result {
		t.Fatalf("expected false, got true")
	}
}
