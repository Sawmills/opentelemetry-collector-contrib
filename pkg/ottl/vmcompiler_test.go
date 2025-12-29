// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func stringpTest(v string) *string {
	return &v
}

func bytespTest(t *testing.T, v string) *byteSlice {
	t.Helper()
	var b byteSlice
	if err := b.Capture([]string{v}); err != nil {
		t.Fatalf("invalid byte literal %q: %v", v, err)
	}
	return &b
}

func boolp(v bool) *boolean {
	b := boolean(v)
	return &b
}

func newTestParser(t *testing.T, withVM bool) Parser[any] {
	opts := []Option[any]{}
	if withVM {
		opts = append(opts, WithVMEnabled[any]())
	}
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return nil, errors.New("path parsing not supported in test")
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		opts...,
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}
	return p
}

type logTestCtx struct {
	lr plog.LogRecord
}

func (c logTestCtx) GetLogRecord() plog.LogRecord {
	return c.lr
}

// --- Constant Folding Tests ---

func TestCompileBoolExpressionConstFold(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 + 2 == 3")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true constant, got %v", val)
	}
}

func TestCompileBoolExpressionConstFold_Multi(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 == 1 and 2 == 3 or 4 == 4")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true constant, got %v", val)
	}
}

func TestCompileBoolExpression_Converter(t *testing.T) {
	functions := CreateFactoryMap[any](
		NewFactory(
			"Foo",
			nil,
			func(FunctionContext, Arguments) (ExprFunc[any], error) {
				return func(context.Context, any) (any, error) {
					return true, nil
				}, nil
			},
		),
	)
	parser, err := NewParser[any](
		functions,
		func(Path[any]) (GetSetter[any], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}
	expr, err := parseCondition("Foo()")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpLoadAttrCached {
		t.Fatalf("expected LOAD_ATTR_CACHED, got %v", got)
	}
}

func TestCompileBoolExpression_IsMatchLiteralPattern(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}
	expr, err := parseCondition(`IsMatch("foo", "fo+")`)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpIsMatch {
		t.Fatalf("expected IS_MATCH, got %v", got)
	}
	if len(program.program.Regexps) != 1 {
		t.Fatalf("expected 1 regex, got %d", len(program.program.Regexps))
	}
}

func TestCompileBoolExpressionConstFold_AllFalse(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 == 2 or 3 == 4")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	program, err := parser.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || got {
		t.Fatalf("expected false constant, got %v", val)
	}
}

func TestCompileComparisonConstFold(t *testing.T) {
	parser, err := NewParser[struct{}](
		map[string]Factory[struct{}]{},
		func(Path[struct{}]) (GetSetter[struct{}], error) {
			return nil, fmt.Errorf("path parsing not supported")
		},
		componenttest.NewNopTelemetrySettings(),
	)
	if err != nil {
		t.Fatalf("parser setup failed: %v", err)
	}

	expr, err := parseCondition("1 + 2 == 4")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if expr.Left == nil || expr.Left.Left == nil || expr.Left.Left.Comparison == nil {
		t.Fatalf("expected comparison expression")
	}
	cmp := expr.Left.Left.Comparison
	program, err := parser.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 1 {
		t.Fatalf("expected 1 instruction, got %d", len(program.program.Code))
	}
	inst := program.program.Code[0]
	if inst.Op() != ir.OpLoadConst {
		t.Fatalf("expected LOAD_CONST, got %v", inst.Op())
	}
	val := program.program.Consts[inst.Arg()]
	got, ok := val.Bool()
	if !ok || got {
		t.Fatalf("expected false constant, got %v", val)
	}
}

// --- Runtime Tests (not constant-foldable) ---

func TestCompileMicroComparison_PathGetter(t *testing.T) {
	getterParser, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(7), nil
				},
				Setter: func(context.Context, any, any) error {
					return nil
				},
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	evaluator, err := getterParser.newComparisonEvaluator(cmp)
	if err != nil {
		t.Fatalf("build evaluator failed: %v", err)
	}

	got, err := evaluator.Eval(context.Background(), nil)
	if err != nil {
		t.Fatalf("eval failed: %v", err)
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
}

func TestCompileMicroComparison_PathEqConstOpcode(t *testing.T) {
	getterParser, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(7), nil
				},
				Setter: func(context.Context, any, any) error {
					return nil
				},
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	program, err := getterParser.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpLoadAttrCached {
		t.Fatalf("expected LOAD_ATTR_CACHED, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_PathEqConstSwapOpcode(t *testing.T) {
	getterParser, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(7), nil
				},
				Setter: func(context.Context, any, any) error {
					return nil
				},
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Int: int64p(7)}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
	}

	program, err := getterParser.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpLoadAttrCached {
		t.Fatalf("expected LOAD_ATTR_CACHED, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_AttrFastOpcode(t *testing.T) {
	p, err := NewParser[logTestCtx](
		map[string]Factory[logTestCtx]{},
		func(Path[logTestCtx]) (GetSetter[logTestCtx], error) {
			return nil, errors.New("path parsing not supported in test")
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[logTestCtx](),
		WithVMAttrGetter[logTestCtx](func(tCtx logTestCtx, key string) (ir.Value, error) {
			val, ok := tCtx.GetLogRecord().Attributes().Get(key)
			if !ok {
				return ir.Value{}, vm.ErrTypeMismatch
			}
			switch val.Type() {
			case pcommon.ValueTypeInt:
				return ir.Int64Value(val.Int()), nil
			case pcommon.ValueTypeDouble:
				return ir.Float64Value(val.Double()), nil
			case pcommon.ValueTypeBool:
				return ir.BoolValue(val.Bool()), nil
			case pcommon.ValueTypeStr:
				return ir.StringValue(val.Str()), nil
			default:
				return ir.Value{}, vm.ErrTypeMismatch
			}
		}),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}
	cmp := &comparison{
		Left: value{Literal: &mathExprLiteral{Path: &path{
			Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("foo")}}}},
		}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpLoadAttrFast {
		t.Fatalf("expected LOAD_ATTR_FAST, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
	if len(program.program.AttrKeys) != 1 || program.program.AttrKeys[0] != "foo" {
		t.Fatalf("unexpected attr keys: %v", program.program.AttrKeys)
	}
}

func TestCompileMicroComparison_DirectFieldBodyOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "body"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("hello")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetBody {
		t.Fatalf("expected GET_BODY, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSeverityOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "severity_number"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(13)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSeverity {
		t.Fatalf("expected GET_SEVERITY, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldTimestampOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "time_unix_nano"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(42)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetTimestamp {
		t.Fatalf("expected GET_TIMESTAMP, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanNameOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "name"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("root")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanName {
		t.Fatalf("expected GET_SPAN_NAME, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanStartTimeOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "start_time_unix_nano"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(7)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanStartTime {
		t.Fatalf("expected GET_SPAN_START_TIME, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanEndTimeOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "end_time_unix_nano"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(9)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanEndTime {
		t.Fatalf("expected GET_SPAN_END_TIME, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanKindOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "kind"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanKind {
		t.Fatalf("expected GET_SPAN_KIND, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanStatusOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "status"}, {Name: "code"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanStatus {
		t.Fatalf("expected GET_SPAN_STATUS, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldMetricNameOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "name"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("requests")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricName {
		t.Fatalf("expected GET_METRIC_NAME, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldMetricUnitOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "unit"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("ms")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricUnit {
		t.Fatalf("expected GET_METRIC_UNIT, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldMetricTypeOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "type"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricType {
		t.Fatalf("expected GET_METRIC_TYPE, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanStatusMessageOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "status"}, {Name: "message"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("oops")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanStatusMsg {
		t.Fatalf("expected GET_SPAN_STATUS_MSG, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldResourceDroppedAttributesCountOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "resource", Fields: []field{{Name: "dropped_attributes_count"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(3)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetResourceDroppedAttributesCount {
		t.Fatalf("expected GET_RESOURCE_DROPPED_ATTRIBUTES_COUNT, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldScopeNameOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "scope", Fields: []field{{Name: "name"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("lib")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetScopeName {
		t.Fatalf("expected GET_SCOPE_NAME, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldScopeVersionOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "scope", Fields: []field{{Name: "version"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("1.0.0")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetScopeVersion {
		t.Fatalf("expected GET_SCOPE_VERSION, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldScopeDroppedAttributesCountOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "scope", Fields: []field{{Name: "dropped_attributes_count"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(5)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetScopeDroppedAttributesCount {
		t.Fatalf("expected GET_SCOPE_DROPPED_ATTRIBUTES_COUNT, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldObservedTimestampOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "observed_time_unix_nano"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(42)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetObservedTimestamp {
		t.Fatalf("expected GET_OBSERVED_TIMESTAMP, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSeverityTextOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "severity_text"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("warn")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSeverityText {
		t.Fatalf("expected GET_SEVERITY_TEXT, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldLogFlagsOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "flags"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetLogFlags {
		t.Fatalf("expected GET_LOG_FLAGS, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanTraceIDStringOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "trace_id"}, {Name: "string"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("traceid")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanTraceIDString {
		t.Fatalf("expected GET_SPAN_TRACE_ID_STRING, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanIDStringOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "span_id"}, {Name: "string"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("spanid")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanIDString {
		t.Fatalf("expected GET_SPAN_ID_STRING, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanTraceIDOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "trace_id"}}}}},
		Op:    eq,
		Right: value{Bytes: bytespTest(t, "0x01020304")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanTraceID {
		t.Fatalf("expected GET_SPAN_TRACE_ID, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanIDOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "span_id"}}}}},
		Op:    eq,
		Right: value{Bytes: bytespTest(t, "0x0a0b0c0d")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanID {
		t.Fatalf("expected GET_SPAN_ID, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanParentIDOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "parent_span_id"}}}}},
		Op:    eq,
		Right: value{Bytes: bytespTest(t, "0x11121314")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanParentID {
		t.Fatalf("expected GET_SPAN_PARENT_ID, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanParentIDStringOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "parent_span_id"}, {Name: "string"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("parentid")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanParentIDString {
		t.Fatalf("expected GET_SPAN_PARENT_ID_STRING, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanTraceStateOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: "trace_state"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("rojo=00f067aa0ba902b7")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanTraceState {
		t.Fatalf("expected GET_SPAN_TRACE_STATE, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanTraceStateKeyOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left: value{Literal: &mathExprLiteral{Path: &path{
			Context: "span",
			Fields: []field{{
				Name: "trace_state",
				Keys: []key{{String: stringpTest("rojo")}},
			}},
		}}},
		Op:    eq,
		Right: value{String: stringpTest("00f067aa0ba902b7")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetSpanTraceStateKey {
		t.Fatalf("expected GET_SPAN_TRACE_STATE_KEY, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldSpanDroppedCountsOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cases := []struct {
		name   string
		field  string
		opcode ir.Opcode
	}{
		{name: "attributes", field: "dropped_attributes_count", opcode: ir.OpGetSpanDroppedAttributesCount},
		{name: "events", field: "dropped_events_count", opcode: ir.OpGetSpanDroppedEventsCount},
		{name: "links", field: "dropped_links_count", opcode: ir.OpGetSpanDroppedLinksCount},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			cmp := &comparison{
				Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "span", Fields: []field{{Name: tt.field}}}}},
				Op:    eq,
				Right: value{Literal: &mathExprLiteral{Int: int64p(3)}},
			}

			program, err := p.compileMicroComparisonVM(cmp)
			if err != nil {
				t.Fatalf("compile failed: %v", err)
			}
			if len(program.program.Code) != 2 {
				t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
			}
			if got := program.program.Code[0].Op(); got != tt.opcode {
				t.Fatalf("expected %v, got %v", tt.opcode, got)
			}
			if got := program.program.Code[1].Op(); got != ir.OpEqConst {
				t.Fatalf("expected EQ_CONST, got %v", got)
			}
		})
	}
}

func TestCompileMicroComparison_DirectFieldMetricDescriptionOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "description"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("desc")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricDescription {
		t.Fatalf("expected GET_METRIC_DESCRIPTION, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldResourceSchemaURLOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "resource", Fields: []field{{Name: "schema_url"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("https://example.com/schema")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetResourceSchemaURL {
		t.Fatalf("expected GET_RESOURCE_SCHEMA_URL, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldScopeSchemaURLOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "scope", Fields: []field{{Name: "schema_url"}}}}},
		Op:    eq,
		Right: value{String: stringpTest("https://example.com/scope-schema")},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetScopeSchemaURL {
		t.Fatalf("expected GET_SCOPE_SCHEMA_URL, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldMetricAggTemporalityOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "aggregation_temporality"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricAggTemporality {
		t.Fatalf("expected GET_METRIC_AGG_TEMPORALITY, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_DirectFieldMetricIsMonotonicOpcode(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Context: "metric", Fields: []field{{Name: "is_monotonic"}}}}},
		Op:    eq,
		Right: value{Bool: boolp(true)},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if len(program.program.Code) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(program.program.Code))
	}
	if got := program.program.Code[0].Op(); got != ir.OpGetMetricIsMonotonic {
		t.Fatalf("expected GET_METRIC_IS_MONOTONIC, got %v", got)
	}
	if got := program.program.Code[1].Op(); got != ir.OpEqConst {
		t.Fatalf("expected EQ_CONST, got %v", got)
	}
}

func TestCompileMicroComparison_Unsupported(t *testing.T) {
	p := newTestParser(t, false)
	n := isNil(true)
	cmp := &comparison{
		Left:  value{IsNil: &n},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	_, err := p.compileMicroComparisonVM(cmp)
	if err == nil {
		t.Fatalf("expected error for unsupported value")
	}
}

func TestCompileMicroComparison_StringLtUnsupported(t *testing.T) {
	p := newTestParser(t, false)
	cmp := &comparison{
		Left:  value{String: stringpTest("a")},
		Op:    lt,
		Right: value{String: stringpTest("b")},
	}

	_, err := p.compileMicroComparisonVM(cmp)
	if err == nil {
		t.Fatalf("expected error for unsupported comparison")
	}
}

func TestCompileMicroBooleanExpression_ShortCircuitOr(t *testing.T) {
	var boomCalls int
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			keys := path.Keys()
			var key string
			if len(keys) > 0 {
				s, err := keys[0].String(context.Background(), nil)
				if err != nil {
					return nil, err
				}
				if s != nil {
					key = *s
				}
			}
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					if key == "boom" {
						boomCalls++
						return nil, errors.New("boom")
					}
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmpOK := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("ok")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}
	cmpBoom := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("boom")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpOK},
		},
		Right: []*opOrTerm{
			{
				Operator: "or",
				Term: &term{
					Left: &booleanValue{Comparison: cmpBoom},
				},
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	var stack [defaultMicroVMStackSize]ir.Value
	// Use context-aware runner since we now emit OpLoadAttrCached
	val, err := vm.RunWithStackAndContext(stack[:], program.program, context.Background(), nil)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if !got {
		t.Fatalf("expected true, got false")
	}
	if boomCalls != 0 {
		t.Fatalf("expected boom getter to be skipped, got %d calls", boomCalls)
	}
}

func TestCompileMicroBooleanExpression_ShortCircuitAnd(t *testing.T) {
	var boomCalls int
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(path Path[any]) (GetSetter[any], error) {
			keys := path.Keys()
			var key string
			if len(keys) > 0 {
				s, err := keys[0].String(context.Background(), nil)
				if err != nil {
					return nil, err
				}
				if s != nil {
					key = *s
				}
			}
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					if key == "boom" {
						boomCalls++
						return nil, errors.New("boom")
					}
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	cmpFalse := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("ok")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	cmpBoom := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "attributes", Keys: []key{{String: stringpTest("boom")}}}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(2)}},
	}
	expr := &booleanExpression{
		Left: &term{
			Left: &booleanValue{Comparison: cmpFalse},
			Right: []*opAndBooleanValue{
				{Operator: "and", Value: &booleanValue{Comparison: cmpBoom}},
			},
		},
	}

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	var stack [defaultMicroVMStackSize]ir.Value
	// Use context-aware runner since we now emit OpLoadAttrCached
	val, err := vm.RunWithStackAndContext(stack[:], program.program, context.Background(), nil)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok {
		t.Fatalf("expected bool result")
	}
	if got {
		t.Fatalf("expected false, got true")
	}
	if boomCalls != 0 {
		t.Fatalf("expected boom getter to be skipped, got %d calls", boomCalls)
	}
}

func TestCompileMicroComparison_GasLimitOption(t *testing.T) {
	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(1), nil
				},
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: zap.NewNop()},
		WithVMEnabled[any](),
		WithVMGasLimit[any](123),
	)
	if err != nil {
		t.Fatalf("parser init failed: %v", err)
	}

	// Use a path so constant folding doesn't apply
	cmp := &comparison{
		Left:  value{Literal: &mathExprLiteral{Path: &path{Fields: []field{{Name: "foo"}}}}},
		Op:    eq,
		Right: value{Literal: &mathExprLiteral{Int: int64p(1)}},
	}

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if program.program.GasLimit != 123 {
		t.Fatalf("expected gas limit 123, got %d", program.program.GasLimit)
	}
}
