// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"context"
	"regexp"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/trace"

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

func TestMicroVMRun_EqConstBytes(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpEqConst, 1),
		},
		Consts: []ir.Value{
			ir.BytesValue([]byte{0x01, 0x02, 0x03}),
			ir.BytesValue([]byte{0x01, 0x02, 0x03}),
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

func TestMicroVMRun_NeConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpNeConst, 1),
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

func TestMicroVMRun_LteConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpLteConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(5),
			ir.Int64Value(5),
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

func TestMicroVMRun_GteConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpGteConst, 1),
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

func TestMicroVMRun_GtConst(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpGtConst, 1),
		},
		Consts: []ir.Value{
			ir.Int64Value(9),
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

func TestRunWithStackAndContext_GetResourceSchemaURL(t *testing.T) {
	ctx := resourceCtx{res: pcommon.NewResource()}
	program := &Program[resourceCtx]{
		Code: []ir.Instruction{ir.Encode(ir.OpGetResourceSchemaURL, 0)},
		ResourceSchemaURLGetter: func(_ resourceCtx) string {
			return "https://example.com/schema"
		},
	}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "https://example.com/schema" {
		t.Fatalf("unexpected resource schema url: %v", val)
	}
}

func TestRunWithStackAndContext_SetResourceSchemaURL(t *testing.T) {
	ctx := resourceCtx{res: pcommon.NewResource()}
	var got string
	program := &Program[resourceCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetResourceSchemaURL, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.StringValue("https://example.com/new-schema"),
			ir.BoolValue(true),
		},
		ResourceSchemaURLSetter: func(_ resourceCtx, schemaURL string) {
			got = schemaURL
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got != "https://example.com/new-schema" {
		t.Fatalf("unexpected resource schema url: %q", got)
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

func TestRunWithStackAndContext_GetScopeSchemaURL(t *testing.T) {
	ctx := scopeCtx{scope: pcommon.NewInstrumentationScope()}
	program := &Program[scopeCtx]{
		Code: []ir.Instruction{ir.Encode(ir.OpGetScopeSchemaURL, 0)},
		ScopeSchemaURLGetter: func(_ scopeCtx) string {
			return "https://example.com/scope-schema"
		},
	}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "https://example.com/scope-schema" {
		t.Fatalf("unexpected scope schema url: %v", val)
	}
}

func TestRunWithStackAndContext_SetScopeSchemaURL(t *testing.T) {
	ctx := scopeCtx{scope: pcommon.NewInstrumentationScope()}
	var got string
	program := &Program[scopeCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetScopeSchemaURL, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts: []ir.Value{
			ir.StringValue("https://example.com/new-scope-schema"),
			ir.BoolValue(true),
		},
		ScopeSchemaURLSetter: func(_ scopeCtx, schemaURL string) {
			got = schemaURL
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got != "https://example.com/new-scope-schema" {
		t.Fatalf("unexpected scope schema url: %q", got)
	}
}

func TestRunWithStackAndContext_GetLogObservedTimestampSeverityTextFlags(t *testing.T) {
	lr := plog.NewLogRecord()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 77)))
	lr.SetSeverityText("warn")
	lr.SetFlags(plog.LogRecordFlags(3))
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetObservedTimestamp, 0),
			ir.Encode(ir.OpGetSeverityText, 0),
			ir.Encode(ir.OpGetLogFlags, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := stack[0]; got.Type != ir.TypeInt || int64(got.Num) != 77 {
		t.Fatalf("unexpected observed timestamp: %v", got)
	}
	if got, ok := stack[1].String(); !ok || got != "warn" {
		t.Fatalf("unexpected severity text: %v", stack[1])
	}
	if got := stack[2]; got.Type != ir.TypeInt || int64(got.Num) != 3 {
		t.Fatalf("unexpected log flags: %v", got)
	}
}

func TestRunWithStackAndContext_SetLogObservedTimestampSeverityTextFlags(t *testing.T) {
	lr := plog.NewLogRecord()
	ctx := logCtx{lr: lr}

	program := &Program[logCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetObservedTimestamp, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetSeverityText, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpSetLogFlags, 0),
			ir.Encode(ir.OpLoadConst, 3),
		},
		Consts: []ir.Value{
			ir.Int64Value(101),
			ir.StringValue("err"),
			ir.Int64Value(5),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := lr.ObservedTimestamp().AsTime().UnixNano(); got != 101 {
		t.Fatalf("unexpected observed timestamp: %d", got)
	}
	if got := lr.SeverityText(); got != "err" {
		t.Fatalf("unexpected severity text: %q", got)
	}
	if got := lr.Flags(); got != plog.LogRecordFlags(5) {
		t.Fatalf("unexpected log flags: %v", got)
	}
}

func TestRunWithStackAndContext_GetSpanIDsTraceStateAndDroppedCounts(t *testing.T) {
	span := ptrace.NewSpan()
	traceID := pcommon.TraceID{0x01, 0x02, 0x03, 0x04}
	spanID := pcommon.SpanID{0x0a, 0x0b, 0x0c, 0x0d}
	parentID := pcommon.SpanID{0x11, 0x12, 0x13, 0x14}
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentID)
	span.TraceState().FromRaw("rojo=00f067aa0ba902b7")
	span.SetDroppedAttributesCount(2)
	span.SetDroppedEventsCount(3)
	span.SetDroppedLinksCount(4)
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetSpanTraceIDString, 0),
			ir.Encode(ir.OpGetSpanIDString, 0),
			ir.Encode(ir.OpGetSpanParentIDString, 0),
			ir.Encode(ir.OpGetSpanTraceState, 0),
			ir.Encode(ir.OpGetSpanDroppedAttributesCount, 0),
			ir.Encode(ir.OpGetSpanDroppedEventsCount, 0),
			ir.Encode(ir.OpGetSpanDroppedLinksCount, 0),
		},
	}
	var stack [8]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got, ok := stack[0].String(); !ok || got == "" {
		t.Fatalf("unexpected trace id string: %v", stack[0])
	}
	if got, ok := stack[1].String(); !ok || got == "" {
		t.Fatalf("unexpected span id string: %v", stack[1])
	}
	if got, ok := stack[2].String(); !ok || got == "" {
		t.Fatalf("unexpected parent span id string: %v", stack[2])
	}
	if got, ok := stack[3].String(); !ok || got != "rojo=00f067aa0ba902b7" {
		t.Fatalf("unexpected trace state: %v", stack[3])
	}
	if got := stack[4]; got.Type != ir.TypeInt || int64(got.Num) != 2 {
		t.Fatalf("unexpected dropped attributes: %v", got)
	}
	if got := stack[5]; got.Type != ir.TypeInt || int64(got.Num) != 3 {
		t.Fatalf("unexpected dropped events: %v", got)
	}
	if got := stack[6]; got.Type != ir.TypeInt || int64(got.Num) != 4 {
		t.Fatalf("unexpected dropped links: %v", got)
	}
}

func TestRunWithStackAndContext_GetSpanIDsRaw(t *testing.T) {
	span := ptrace.NewSpan()
	traceID := pcommon.TraceID{0x01, 0x02, 0x03, 0x04}
	spanID := pcommon.SpanID{0x0a, 0x0b, 0x0c, 0x0d}
	parentID := pcommon.SpanID{0x11, 0x12, 0x13, 0x14}
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentID)
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetSpanTraceID, 0),
			ir.Encode(ir.OpGetSpanID, 0),
			ir.Encode(ir.OpGetSpanParentID, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got, ok := stack[0].Bytes(); !ok || len(got) == 0 {
		t.Fatalf("unexpected trace id bytes: %v", stack[0])
	}
	if got, ok := stack[1].Bytes(); !ok || len(got) == 0 {
		t.Fatalf("unexpected span id bytes: %v", stack[1])
	}
	if got, ok := stack[2].Bytes(); !ok || len(got) == 0 {
		t.Fatalf("unexpected parent span id bytes: %v", stack[2])
	}
}

func TestRunWithStackAndContext_CompareSpanTraceID(t *testing.T) {
	span := ptrace.NewSpan()
	traceID := pcommon.TraceID{0x01, 0x02, 0x03, 0x04}
	span.SetTraceID(traceID)
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetSpanTraceID, 0),
			ir.Encode(ir.OpGetSpanTraceID, 0),
			ir.Encode(ir.OpEq, 0),
		},
	}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true, got %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanTraceStateAndDroppedCounts(t *testing.T) {
	span := ptrace.NewSpan()
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanTraceState, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetSpanDroppedAttributesCount, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpSetSpanDroppedEventsCount, 0),
			ir.Encode(ir.OpLoadConst, 3),
			ir.Encode(ir.OpSetSpanDroppedLinksCount, 0),
			ir.Encode(ir.OpLoadConst, 4),
		},
		Consts: []ir.Value{
			ir.StringValue("k1=v1"),
			ir.Int64Value(7),
			ir.Int64Value(8),
			ir.Int64Value(9),
			ir.BoolValue(true),
		},
	}
	var stack [8]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := span.TraceState().AsRaw(); got != "k1=v1" {
		t.Fatalf("unexpected trace state: %q", got)
	}
	if got := span.DroppedAttributesCount(); got != 7 {
		t.Fatalf("unexpected dropped attributes: %v", got)
	}
	if got := span.DroppedEventsCount(); got != 8 {
		t.Fatalf("unexpected dropped events: %v", got)
	}
	if got := span.DroppedLinksCount(); got != 9 {
		t.Fatalf("unexpected dropped links: %v", got)
	}
}

func TestRunWithStackAndContext_GetSpanTraceStateKey(t *testing.T) {
	span := ptrace.NewSpan()
	span.TraceState().FromRaw("rojo=00f067aa0ba902b7,congo=t61rcWkgMzE")
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code:     []ir.Instruction{ir.Encode(ir.OpGetSpanTraceStateKey, 0)},
		AttrKeys: []string{"congo"},
	}
	var stack [4]ir.Value
	val, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.String()
	if !ok || got != "t61rcWkgMzE" {
		t.Fatalf("unexpected trace state value: %v", val)
	}
}

func TestRunWithStackAndContext_SetSpanTraceStateKey(t *testing.T) {
	span := ptrace.NewSpan()
	span.TraceState().FromRaw("rojo=00f067aa0ba902b7")
	ctx := spanCtx{span: span}

	program := &Program[spanCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetSpanTraceStateKey, 0),
			ir.Encode(ir.OpLoadConst, 1),
		},
		Consts:   []ir.Value{ir.StringValue("t61rcWkgMzE"), ir.BoolValue(true)},
		AttrKeys: []string{"congo"},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	parsed, err := trace.ParseTraceState(span.TraceState().AsRaw())
	if err != nil {
		t.Fatalf("parse trace state failed: %v", err)
	}
	if got := parsed.Get("congo"); got != "t61rcWkgMzE" {
		t.Fatalf("unexpected trace state value: %q", got)
	}
}

func TestRunWithStackAndContext_GetMetricDescriptionAggTemporalityIsMonotonic(t *testing.T) {
	metric := pmetric.NewMetric()
	metric.SetDescription("desc")
	metric.SetEmptySum()
	metric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	metric.Sum().SetIsMonotonic(true)
	ctx := metricCtx{metric: metric}

	program := &Program[metricCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpGetMetricDescription, 0),
			ir.Encode(ir.OpGetMetricAggTemporality, 0),
			ir.Encode(ir.OpGetMetricIsMonotonic, 0),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got, ok := stack[0].String(); !ok || got != "desc" {
		t.Fatalf("unexpected metric description: %v", stack[0])
	}
	if got := stack[1]; got.Type != ir.TypeInt || int64(got.Num) != int64(pmetric.AggregationTemporalityCumulative) {
		t.Fatalf("unexpected metric agg temporality: %v", got)
	}
	if got := stack[2]; got.Type != ir.TypeBool || got.Num == 0 {
		t.Fatalf("unexpected metric is monotonic: %v", got)
	}
}

func TestRunWithStackAndContext_SetMetricDescriptionAggTemporalityIsMonotonic(t *testing.T) {
	metric := pmetric.NewMetric()
	metric.SetEmptySum()
	ctx := metricCtx{metric: metric}

	program := &Program[metricCtx]{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpSetMetricDescription, 0),
			ir.Encode(ir.OpLoadConst, 1),
			ir.Encode(ir.OpSetMetricAggTemporality, 0),
			ir.Encode(ir.OpLoadConst, 2),
			ir.Encode(ir.OpSetMetricIsMonotonic, 0),
			ir.Encode(ir.OpLoadConst, 3),
		},
		Consts: []ir.Value{
			ir.StringValue("updated"),
			ir.Int64Value(int64(pmetric.AggregationTemporalityDelta)),
			ir.BoolValue(true),
			ir.BoolValue(true),
		},
	}
	var stack [4]ir.Value
	_, err := RunWithStackAndContext(stack[:], program, context.Background(), ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := metric.Description(); got != "updated" {
		t.Fatalf("unexpected metric description: %q", got)
	}
	if got := metric.Sum().AggregationTemporality(); got != pmetric.AggregationTemporalityDelta {
		t.Fatalf("unexpected metric agg temporality: %v", got)
	}
	if got := metric.Sum().IsMonotonic(); !got {
		t.Fatalf("expected metric to be monotonic")
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

func TestMicroVMRun_Int(t *testing.T) {
	vm := NewMicroVM(2)
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpInt, 0),
		},
		Consts: []ir.Value{
			ir.StringValue("42"),
		},
	}
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Int64()
	if !ok || got != 42 {
		t.Fatalf("expected 42, got %v", val)
	}

	program = &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpInt, 0),
		},
		Consts: []ir.Value{
			ir.StringValue("nope"),
		},
	}
	val, err = vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if val.Type != ir.TypeNone {
		t.Fatalf("expected nil result, got %v", val)
	}
}

func TestMicroVMRun_IsNil(t *testing.T) {
	vm := NewMicroVM(2)
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpIsNil, 0),
		},
		Consts: []ir.Value{
			{Type: ir.TypeNone},
		},
	}
	val, err := vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok := val.Bool()
	if !ok || !got {
		t.Fatalf("expected true, got %v", val)
	}

	program = &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpIsNil, 0),
		},
		Consts: []ir.Value{
			ir.Int64Value(1),
		},
	}
	val, err = vm.Run(program)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	got, ok = val.Bool()
	if !ok || got {
		t.Fatalf("expected false, got %v", val)
	}
}

func TestMicroVMRun_IsMatch(t *testing.T) {
	program := &ProgramAny{
		Code: []ir.Instruction{
			ir.Encode(ir.OpLoadConst, 0),
			ir.Encode(ir.OpIsMatch, 0),
		},
		Consts: []ir.Value{
			ir.StringValue("operationA"),
		},
		Regexps: []*regexp.Regexp{
			regexp.MustCompile("operation[AC]"),
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
