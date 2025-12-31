package ottl

import (
	"context"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// Regression harness for fuzz-found divergence: not IsMatch(0, "")
func TestReproIsMatch0(t *testing.T) {
	statement := `not IsMatch(0, "")`

	functions := CreateFactoryMap[any](
		newBenchIsMatchFactory[any](),
	)

	noopGetter := func(Path[any]) (GetSetter[any], error) {
		return StandardGetSetter[any]{
			Getter: func(context.Context, any) (any, error) { return nil, nil },
			Setter: func(context.Context, any, any) error { return nil },
		}, nil
	}

	pInt, err := NewParser[any](
		functions,
		noopGetter,
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	assert.NoError(t, err)

	pVM, err := NewParser[any](
		functions,
		noopGetter,
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	assert.NoError(t, err)

	expr, err := ParseCondition(statement)
	assert.NoError(t, err)

	evalInt, err := pInt.newBoolExpr(expr)
	assert.NoError(t, err)

	evalVM, err := pVM.newBoolExpr(expr)
	assert.NoError(t, err)

	program, err := pVM.compileMicroBoolExpression(expr)
	assert.NoError(t, err)
	t.Logf("VM bytecode:\n%s", vm.Disassemble(program.program))

	ctx := context.Background()
	resInt, errInt := evalInt.Eval(ctx, nil)
	resVM, errVM := evalVM.Eval(ctx, nil)

	t.Logf("Int: %v, %v", resInt, errInt)
	t.Logf("VM:  %v, %v", resVM, errVM)

	assert.Equal(t, errInt, errVM)
	assert.Equal(t, resInt, resVM)
}

// Regression for float-to-string IsMatch formatting
func TestReproIsMatchFloatExponentZero(t *testing.T) {
	statement := `IsMatch(.2E17, "0")`

	functions := CreateFactoryMap[any](
		newBenchIsMatchFactory[any](),
	)

	noopGetter := func(Path[any]) (GetSetter[any], error) {
		return StandardGetSetter[any]{
			Getter: func(context.Context, any) (any, error) { return nil, nil },
			Setter: func(context.Context, any, any) error { return nil },
		}, nil
	}

	pInt, err := NewParser[any](
		functions,
		noopGetter,
		component.TelemetrySettings{Logger: zap.NewNop()},
	)
	assert.NoError(t, err)

	pVM, err := NewParser[any](
		functions,
		noopGetter,
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	assert.NoError(t, err)

	expr, err := ParseCondition(statement)
	assert.NoError(t, err)

	evalInt, err := pInt.newBoolExpr(expr)
	assert.NoError(t, err)

	evalVM, err := pVM.newBoolExpr(expr)
	assert.NoError(t, err)

	program, err := pVM.compileMicroBoolExpression(expr)
	assert.NoError(t, err)
	t.Logf("VM bytecode:\n%s", vm.Disassemble(program.program))

	ctx := context.Background()
	// Inspect StandardStringLikeGetter formatting
	g := StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) { return 0.2e17, nil }}
	strVal, err := g.Get(ctx, nil)
	assert.NoError(t, err)
	t.Logf("StandardStringLikeGetter(0.2e17) -> %q", *strVal)

	resInt, errInt := evalInt.Eval(ctx, nil)
	resVM, errVM := evalVM.Eval(ctx, nil)

	t.Logf("Int: %v, %v", resInt, errInt)
	t.Logf("VM:  %v, %v", resVM, errVM)

	assert.Equal(t, errInt, errVM)
	assert.Equal(t, resInt, resVM)
}
