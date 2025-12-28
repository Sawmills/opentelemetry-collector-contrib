// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ir // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"

import (
	"math"
	"unsafe"
)

// Type represents the runtime type stored in a Value.
type Type uint8

const (
	TypeNone Type = iota
	TypeInt
	TypeFloat
	TypeBool
	TypeString
	TypeBytes
	TypePMap
	TypePSlice
)

// Value is a tagged-union for VM stack values.
// Layout: 24 bytes on 64-bit arch (1 + 7 padding + 8 + 8).
type Value struct {
	Type Type    // 1 byte
	_    [7]byte // padding
	Num  uint64  // int64/float64/bool payload
	Ptr  unsafe.Pointer
}

// Compile-time size assertion: Value must be exactly 24 bytes.
var _ [24]byte = [unsafe.Sizeof(Value{})]byte{}

// Int64Value constructs an int Value.
func Int64Value(v int64) Value {
	return Value{Type: TypeInt, Num: uint64(v)}
}

// Float64Value constructs a float Value.
func Float64Value(v float64) Value {
	return Value{Type: TypeFloat, Num: math.Float64bits(v)}
}

// BoolValue constructs a bool Value.
func BoolValue(v bool) Value {
	if v {
		return Value{Type: TypeBool, Num: 1}
	}
	return Value{Type: TypeBool}
}

// StringValue constructs a string Value.
//
// Implementation note: store the string data pointer in Ptr and the length in Num
// to keep Value at 24 bytes and avoid per-call heap allocation.
func StringValue(v string) Value {
	if len(v) == 0 {
		return Value{Type: TypeString}
	}
	return Value{Type: TypeString, Num: uint64(len(v)), Ptr: unsafe.Pointer(unsafe.StringData(v))}
}

// Int64 returns the int64 payload when TypeInt.
func (v Value) Int64() (int64, bool) {
	if v.Type != TypeInt {
		return 0, false
	}
	return int64(v.Num), true
}

// Float64 returns the float64 payload when TypeFloat.
func (v Value) Float64() (float64, bool) {
	if v.Type != TypeFloat {
		return 0, false
	}
	return math.Float64frombits(v.Num), true
}

// String returns the string payload when TypeString.
func (v Value) String() (string, bool) {
	if v.Type != TypeString {
		return "", false
	}
	if v.Num == 0 {
		return "", true
	}
	if v.Ptr == nil {
		return "", false
	}
	return unsafe.String((*byte)(v.Ptr), int(v.Num)), true
}

// Bool returns the bool payload when TypeBool.
func (v Value) Bool() (bool, bool) {
	if v.Type != TypeBool {
		return false, false
	}
	return v.Num != 0, true
}

// Opcode is a bytecode opcode.
type Opcode uint8

const (
	OpLoadConst Opcode = iota
	OpLoadGetter
	OpAdd
	OpSub
	OpMul
	OpDiv
	OpEq
	OpNe
	OpLt
	OpLte
	OpGt
	OpGte
	OpJump
	OpJumpIfTrue
	OpJumpIfFalse
	OpPop
	OpNot
)

// Instruction is a 32-bit fixed-width instruction.
// Encoding: [opcode:8][arg:24] in big-endian layout.
type Instruction uint32

// Op returns the opcode for the instruction.
func (i Instruction) Op() Opcode {
	return Opcode(i >> 24)
}

// Arg returns the 24-bit argument for the instruction.
func (i Instruction) Arg() uint32 {
	return uint32(i) & 0x00FFFFFF
}

// Encode builds an Instruction from an opcode and argument.
func Encode(op Opcode, arg uint32) Instruction {
	return Instruction(uint32(op)<<24 | (arg & 0x00FFFFFF))
}
