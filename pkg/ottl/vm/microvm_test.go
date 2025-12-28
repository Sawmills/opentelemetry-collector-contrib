// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

func TestMicroVMRun_AddEq(t *testing.T) {
	program := &Program{
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

func TestMicroVMRun_FloatMul(t *testing.T) {
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{}
	vm := NewMicroVM(1)
	_, err := vm.Run(program)
	if err != ErrEmptyStack {
		t.Fatalf("expected ErrEmptyStack, got %v", err)
	}
}

func TestMicroVMRun_CompareMixedNumeric(t *testing.T) {
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
	program := &Program{
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
