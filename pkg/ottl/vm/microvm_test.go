// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

type logCtx struct {
	lr plog.LogRecord
}

func (c logCtx) GetLogRecord() plog.LogRecord {
	return c.lr
}

type spanCtx struct {
	span ptrace.Span
}

func (c spanCtx) GetSpan() ptrace.Span {
	return c.span
}

type metricCtx struct {
	metric pmetric.Metric
}

func (c metricCtx) GetMetric() pmetric.Metric {
	return c.metric
}

type resourceCtx struct {
	res pcommon.Resource
}

func (c resourceCtx) GetResource() pcommon.Resource {
	return c.res
}

type scopeCtx struct {
	scope pcommon.InstrumentationScope
}

func (c scopeCtx) GetInstrumentationScope() pcommon.InstrumentationScope {
	return c.scope
}

func TestMicroVMRun_AddEq(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAdd, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
			ir.Int64Value(3),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_EqConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpEqConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(7),
			ir.Int64Value(7),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true, got %v", val)
	}
}

func TestMicroVMRun_LtConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLtConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(5),
			ir.Int64Value(7),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true, got %v", val)
	}
}

func TestRunWithStackAndContext_LoadAttrFast(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.Attributes().PutInt("foo", 7)
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code:     []ir.Instruction{ir.Encode(ir.OpLoadAttrFast, 0)},
		AttrKeys: []string{"foo"},
		AttrGetter: func(tCtx logCtx, key string) (ir.Value, error) {
			val, ok := tCtx.GetLogRecord().Attributes().Get(key)
			if !ok {
				return ir.Value{}, ErrTypeMismatch
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
				return ir.Value{}, ErrTypeMismatch
			}
		},
	}

	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != 7 {
		t.Fatalf("unexpected attr value: %v", val)
	}
}

func TestRunWithStackAndContext_SetAttrFast(t *testing.T) {
	lr := plog.NewLogRecord()
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetAttrFast, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts:   []ir.Value{ir.Int64Value(9), ir.BoolValue(true)},
		AttrKeys: []string{"foo"},
		AttrSetter: func(tCtx logCtx, key string, val ir.Value) error {
			switch val.Type {
			case ir.TypeInt:
				tCtx.GetLogRecord().Attributes().PutInt(key, int64(val.Num))
				return nil
			default:
				return ErrTypeMismatch
			}
		},
	}

	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := lr.Attributes().Get("foo")
	if !ok || got.Type() != pcommon.ValueTypeInt || got.Int() != 9 {
		t.Fatalf("unexpected attr value: %v", got)
	}
}

func TestRunWithStackAndContext_GetBody(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.Body().SetStr("hello")
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetBody, 0),
		},
	}

	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "hello" {
		t.Fatalf("unexpected body: %v", val)
	}
}

func TestRunWithStackAndContext_SetBody(t *testing.T) {
	lr := plog.NewLogRecord()
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetBody, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.StringValue("updated"),
			ir.BoolValue(true),
		},
	}

	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := lr.Body().AsString(); got != "updated" {
		t.Fatalf("unexpected body: %q", got)
	}
}

func TestRunWithStackAndContext_GetSeverity(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.SetSeverityNumber(plog.SeverityNumber(13))
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetSeverity, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != 13 {
		t.Fatalf("unexpected severity: %v", val)
	}
}

func TestRunWithStackAndContext_SetSeverity(t *testing.T) {
	lr := plog.NewLogRecord()
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSeverity, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(9),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := lr.SeverityNumber(); got != plog.SeverityNumber(9) {
		t.Fatalf("unexpected severity: %v", got)
	}
}

func TestRunWithStackAndContext_GetTimestamp(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 42)))
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetTimestamp, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != 42 {
		t.Fatalf("unexpected timestamp: %v", val)
	}
}

func TestRunWithStackAndContext_SetTimestamp(t *testing.T) {
	lr := plog.NewLogRecord()
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetTimestamp, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(99),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := lr.Timestamp().AsTime().UnixNano(); got != 99 {
		t.Fatalf("unexpected timestamp: %d", got)
	}
}

func TestRunWithStackAndContext_GetSpanName(t *testing.T) {
	span := ptrace.NewSpan()
	span.SetName("root")
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetSpanName, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "root" {
		t.Fatalf("unexpected span name: %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanName(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanName, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.StringValue("updated"),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.Name(); got != "updated" {
		t.Fatalf("unexpected span name: %q", got)
	}
}

func TestRunWithStackAndContext_GetSpanStartEnd(t *testing.T) {
	span := ptrace.NewSpan()
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 7)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 9)))
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetSpanStartTime, 0),
			ir.Encode(ir.OpGetSpanEndTime, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := stack[0]; got.Type != ir.TypeInt || int64(got.Num) != 7 {
		t.Fatalf("unexpected span start time: %v", got)
	}
	if got := stack[1]; got.Type != ir.TypeInt || int64(got.Num) != 9 {
		t.Fatalf("unexpected span end time: %v", got)
	}
}

func TestRunWithStackAndContext_SetSpanStartEnd(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanStartTime, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetSpanEndTime, 0),
			ir.Encode(ir.OpLoadConst, 2),
		},
		Consts: []ir.Value{
			ir.Int64Value(11),
			ir.Int64Value(13),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.StartTimestamp().AsTime().UnixNano(); got != 11 {
		t.Fatalf("unexpected span start time: %d", got)
	}
	if got := span.EndTimestamp().AsTime().UnixNano(); got != 13 {
		t.Fatalf("unexpected span end time: %d", got)
	}
}

func TestRunWithStackAndContext_GetSpanKind(t *testing.T) {
	span := ptrace.NewSpan()
	span.SetKind(ptrace.SpanKindServer)
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetSpanKind, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != int64(ptrace.SpanKindServer) {
		t.Fatalf("unexpected span kind: %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanKind(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanKind, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(int64(ptrace.SpanKindClient)),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.Kind(); got != ptrace.SpanKindClient {
		t.Fatalf("unexpected span kind: %v", got)
	}
}

func TestRunWithStackAndContext_GetSpanStatus(t *testing.T) {
	span := ptrace.NewSpan()
	span.Status().SetCode(ptrace.StatusCodeError)
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetSpanStatus, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != int64(ptrace.StatusCodeError) {
		t.Fatalf("unexpected span status: %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanStatus(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanStatus, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(int64(ptrace.StatusCodeOk)),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.Status().Code(); got != ptrace.StatusCodeOk {
		t.Fatalf("unexpected span status: %v", got)
	}
}

func TestRunWithStackAndContext_GetMetricNameUnitType(t *testing.T) {
	metric := pmetric.NewMetric()
	metric.SetName("requests")
	metric.SetUnit("ms")
	metric.SetEmptySum()
	ctx := metricCtx{metric: metric}

	program := &Program[metricCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetMetricName, 0),
			ir.Encode(ir.OpGetMetricUnit, 0),
			ir.Encode(ir.OpGetMetricType, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got, ok := stack[0].String(); !ok || got != "requests" {
		t.Fatalf("unexpected metric name: %v", stack[0])
	}
	if got, ok := stack[1].String(); !ok || got != "ms" {
		t.Fatalf("unexpected metric unit: %v", stack[1])
	}
	if got, ok := stack[2].Int64(); !ok || got != int64(pmetric.MetricTypeSum) {
		t.Fatalf("unexpected metric type: %v", stack[2])
	}
}

func TestRunWithStackAndContext_SetMetricNameUnit(t *testing.T) {
	metric := pmetric.NewMetric()
	ctx := metricCtx{metric: metric}

	program := &Program[metricCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetMetricName, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetMetricUnit, 0),
			ir.Encode(ir.OpLoadConst, 2),
		},
		Consts: []ir.Value{
			ir.StringValue("cpu"),
			ir.StringValue("percent"),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := metric.Name(); got != "cpu" {
		t.Fatalf("unexpected metric name: %q", got)
	}
	if got := metric.Unit(); got != "percent" {
		t.Fatalf("unexpected metric unit: %q", got)
	}
}

func TestRunWithStackAndContext_GetSpanStatusMessage(t *testing.T) {
	span := ptrace.NewSpan()
	span.Status().SetMessage("oops")
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetSpanStatusMsg, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "oops" {
		t.Fatalf("unexpected span status message: %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanStatusMessage(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanStatusMsg, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.StringValue("bad"),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.Status().Message(); got != "bad" {
		t.Fatalf("unexpected span status message: %q", got)
	}
}

func TestRunWithStackAndContext_GetResourceDroppedAttributesCount(t *testing.T) {
	res := pcommon.NewResource()
	res.SetDroppedAttributesCount(7)
	ctx := resourceCtx{res: res}

	program := &Program[resourceCtx]{Code: []ir.Instruction{ir.Encode(ir.OpGetResourceDroppedAttributesCount, 0)}}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != 7 {
		t.Fatalf("unexpected resource dropped attributes: %v", val)
	}
}

func TestRunWithStackAndContext_SetResourceDroppedAttributesCount(t *testing.T) {
	res := pcommon.NewResource()
	ctx := resourceCtx{res: res}

	program := &Program[resourceCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetResourceDroppedAttributesCount, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(9),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := res.DroppedAttributesCount(); got != 9 {
		t.Fatalf("unexpected resource dropped attributes: %v", got)
	}
}

func TestRunWithStackAndContext_GetScopeFields(t *testing.T) {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("lib")
	scope.SetVersion("1.2.3")
	scope.SetDroppedAttributesCount(5)
	ctx := scopeCtx{scope: scope}

	program := &Program[scopeCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetScopeName, 0),
			ir.Encode(ir.OpGetScopeVersion, 0),
			ir.Encode(ir.OpGetScopeDroppedAttributesCount, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got, ok := stack[0].String(); !ok || got != "lib" {
		t.Fatalf("unexpected scope name: %v", stack[0])
	}
	if got, ok := stack[1].String(); !ok || got != "1.2.3" {
		t.Fatalf("unexpected scope version: %v", stack[1])
	}
	if got, ok := stack[2].Int64(); !ok || got != 5 {
		t.Fatalf("unexpected scope dropped attributes: %v", stack[2])
	}
}

func TestRunWithStackAndContext_SetScopeFields(t *testing.T) {
	scope := pcommon.NewInstrumentationScope()
	ctx := scopeCtx{scope: scope}

	program := &Program[scopeCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetScopeName, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetScopeVersion, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpSetScopeDroppedAttributesCount, 0),
			ir.Encode(ir.OpLoadConst, 3),
		},
		Consts: []ir.Value{
			ir.StringValue("svc"),
			ir.StringValue("9.9.9"),
			ir.Int64Value(12),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := scope.Name(); got != "svc" {
		t.Fatalf("unexpected scope name: %q", got)
	}
	if got := scope.Version(); got != "9.9.9" {
		t.Fatalf("unexpected scope version: %q", got)
	}
	if got := scope.DroppedAttributesCount(); got != 12 {
		t.Fatalf("unexpected scope dropped attributes: %v", got)
	}
}

func TestMicroVMRun_FloatMul(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpMul, 0),
		},
		Consts: []ir.Value{
			ir.Float64Value(1.5),
			ir.Float64Value(2.0),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Float64()
	if !ok {
		t.Fatalf("expected float result")
	}
	if got != 3.0 {
		t.Fatalf("expected 3.0, got %v", got)
	}
}

func TestMicroVMRun_InvalidConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
		},
	}

	vm := NewMicroVM(2)
	_, err := vm.Run(program)
	if err != ErrInvalidConst {
		t.Fatalf("expected ErrInvalidConst, got %v", err)
	}
}

func TestMicroVMRun_StackUnderflow(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpAdd, 0),
		},
	}

	vm := NewMicroVM(2)
	_, err := vm.Run(program)
	if err != ErrStackUnderflow {
		t.Fatalf("expected ErrStackUnderflow, got %v", err)
	}
}

func TestMicroVMRun_StackOverflow(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
		},
	}

	vm := NewMicroVM(1)
	_, err := vm.Run(program)
	if err != ErrStackOverflow {
		t.Fatalf("expected ErrStackOverflow, got %v", err)
	}
}

func TestMicroVMRun_TypeMismatch(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpAdd, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.BoolValue(true),
		},
	}

	vm := NewMicroVM(4)
	_, err := vm.Run(program)
	if err != ErrTypeMismatch {
		t.Fatalf("expected ErrTypeMismatch, got %v", err)
	}
}

func TestMicroVMRun_DivideByZero(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpDiv, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(0),
		},
	}

	vm := NewMicroVM(4)
	_, err := vm.Run(program)
	if err != ErrDivideByZero {
		t.Fatalf("expected ErrDivideByZero, got %v", err)
	}
}

func TestMicroVMRun_InvalidOpcode(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.Opcode(99), 0),
		},
	}

	vm := NewMicroVM(2)
	_, err := vm.Run(program)
	if err != ErrInvalidOpcode {
		t.Fatalf("expected ErrInvalidOpcode, got %v", err)
	}
}

func TestMicroVMRun_EmptyStack(t *testing.T) {
	program := &ProgramAny{}
	vm := NewMicroVM(1)
	_, err := vm.Run(program)
	if err != ErrEmptyStack {
		t.Fatalf("expected ErrEmptyStack, got %v", err)
	}
}

func TestMicroVMRun_CompareMixedNumeric(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(3),
			ir.Float64Value(3.0),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_CompareStringEq(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpEq, 0),
		},
		Consts: []ir.Value{
			ir.StringValue("foo"),
			ir.StringValue("foo"),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_CompareBoolNe(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpNe, 0),
		},
		Consts: []ir.Value{
			ir.BoolValue(true),
			ir.BoolValue(false),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_Not(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpNot, 0),
		},
		Consts: []ir.Value{
			ir.BoolValue(true),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_Pop(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpPop, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.BoolValue(true),
			ir.BoolValue(false),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_JumpIfTrue(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpJumpIfTrue, 3),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpLoadConst, 2),
		},
		Consts: []ir.Value{
			ir.BoolValue(true),
			ir.BoolValue(false),
			ir.BoolValue(true),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_JumpIfFalse(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpJumpIfFalse, 3),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpLoadConst, 2),
		},
		Consts: []ir.Value{
			ir.BoolValue(false),
			ir.BoolValue(true),
			ir.BoolValue(false),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
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
}

func TestMicroVMRun_InvalidJump(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpJump, 2),
		},
	}

	vm := NewMicroVM(2)
	_, err := vm.Run(program)
	if err != ErrInvalidJump {
		t.Fatalf("expected ErrInvalidJump, got %v", err)
	}
}

func TestMicroVMRun_GasExhausted(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
			ir.Int64Value(2),
		},
		GasLimit: 1,
	}

	vm := NewMicroVM(4)
	_, err := vm.Run(program)
	if err != ErrGasExhausted {
		t.Fatalf("expected ErrGasExhausted, got %v", err)
	}
}

func TestMicroVMRun_BackwardJumpGasExhausted(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpJump, 0),
		},
		GasLimit: 5,
	}

	vm := NewMicroVM(1)
	_, err := vm.Run(program)
	if err != ErrGasExhausted {
		t.Fatalf("expected ErrGasExhausted, got %v", err)
	}
}

func TestMicroVMRun_DivIntByZero(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpDivInt, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(10),
			ir.Int64Value(0),
		},
	}

	vm := NewMicroVM(4)
	_, err := vm.Run(program)
	if err != ErrDivideByZero {
		t.Fatalf("expected ErrDivideByZero, got %v", err)
	}
}

func TestMicroVMRun_DivFloatByZero(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpDivFloat, 0),
		},
		Consts: []ir.Value{
			ir.Float64Value(10.0),
			ir.Float64Value(0.0),
		},
	}

	vm := NewMicroVM(4)
	_, err := vm.Run(program)
	if err != ErrDivideByZero {
		t.Fatalf("expected ErrDivideByZero, got %v", err)
	}
}

func TestMicroVMRun_SpecializedIntOps(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0), // 10
			ir.Encode(ir.OpLoadConst, 1), // 3
			ir.Encode(ir.OpDivInt, 0),    // 10 / 3 = 3
		},
		Consts: []ir.Value{
			ir.Int64Value(10),
			ir.Int64Value(3),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok {
		t.Fatalf("expected int result")
	}
	if got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestMicroVMRun_SpecializedFloatOps(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0), // 10.0
			ir.Encode(ir.OpLoadConst, 1), // 4.0
			ir.Encode(ir.OpDivFloat, 0),  // 10.0 / 4.0 = 2.5
		},
		Consts: []ir.Value{
			ir.Float64Value(10.0),
			ir.Float64Value(4.0),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Float64()
	if !ok {
		t.Fatalf("expected float result")
	}
	if got != 2.5 {
		t.Fatalf("expected 2.5, got %v", got)
	}
}

func TestMicroVMRun_Dup(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpDup, 0),
			ir.Encode(ir.OpAddInt, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(21),
		},
	}

	vm := NewMicroVM(4)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok {
		t.Fatalf("expected int result")
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestMicroVMRun_DupStackOverflow(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpDup, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
		},
	}

	vm := NewMicroVM(1)
	_, err := vm.Run(program)
	if err != ErrStackOverflow {
		t.Fatalf("expected ErrStackOverflow, got %v", err)
	}
}

func TestMicroVMRun_NegInt(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpNegInt, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(42),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok {
		t.Fatalf("expected int result")
	}
	if got != -42 {
		t.Fatalf("expected -42, got %d", got)
	}
}

func TestMicroVMRun_NegFloat(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpNegFloat, 0),
		},
		Consts: []ir.Value{
			ir.Float64Value(3.14),
		},
	}

	vm := NewMicroVM(2)
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Float64()
	if !ok {
		t.Fatalf("expected float result")
	}
	if got != -3.14 {
		t.Fatalf("expected -3.14, got %v", got)
	}
}
