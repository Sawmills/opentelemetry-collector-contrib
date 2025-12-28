// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

type logCtx struct {
	lr plog.LogRecord
}

func (c logCtx) GetLogRecord() plog.LogRecord {
	return c.lr
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
