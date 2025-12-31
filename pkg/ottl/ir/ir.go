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

var (
	trueValue  = Value{Type: TypeBool, Num: 1}
	falseValue = Value{Type: TypeBool, Num: 0}
	emptyByte  byte
)

// cap defensive string lengths to avoid runaway unsafe.String on corrupted values
const maxStringLen = 16 << 20 // 16 MiB

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
		return trueValue
	}
	return falseValue
}

// StringValue constructs a string Value without heap allocation.
func StringValue(v string) Value {
	if len(v) == 0 {
		return Value{Type: TypeString}
	}
	return Value{Type: TypeString, Num: uint64(len(v)), Ptr: unsafe.Pointer(&v)}
}

// BytesValue constructs a byte slice Value without heap allocation.
func BytesValue(v []byte) Value {
	if v == nil {
		return Value{Type: TypeBytes}
	}
	if len(v) == 0 {
		return Value{Type: TypeBytes, Ptr: unsafe.Pointer(&emptyByte)}
	}
	return Value{Type: TypeBytes, Num: uint64(len(v)), Ptr: unsafe.Pointer(unsafe.SliceData(v))}
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
	if v.Ptr == nil {
		return "", true
	}
	s := *(*string)(v.Ptr)
	if len(s) > maxStringLen {
		return "", false
	}
	return s, true
}

// Bytes returns the byte slice payload when TypeBytes.
func (v Value) Bytes() ([]byte, bool) {
	if v.Type != TypeBytes {
		return nil, false
	}
	if v.Num == 0 {
		if v.Ptr == nil {
			return nil, true
		}
		return []byte{}, true
	}
	if v.Num > uint64(^uint(0)>>1) {
		return nil, false
	}
	if v.Ptr == nil {
		return nil, false
	}
	return unsafe.Slice((*byte)(v.Ptr), int(v.Num)), true
}

// Bool returns the bool payload when TypeBool.
func (v Value) Bool() (bool, bool) {
	if v.Type != TypeBool {
		return false, false
	}
	return v.Num != 0, true
}

// StringsEqual compares two string Values for equality using pointer fast-path.
func StringsEqual(a, b Value) bool {
	if a.Type != TypeString || b.Type != TypeString {
		return false
	}
	if a.Num != b.Num {
		return false
	}
	if a.Ptr == b.Ptr {
		return true
	}
	if a.Num == 0 {
		return true
	}
	as := unsafe.String((*byte)(a.Ptr), int(a.Num))
	bs := unsafe.String((*byte)(b.Ptr), int(b.Num))
	return as == bs
}

// Opcode is a bytecode opcode.
type Opcode uint8

const (
	OpLoadConst Opcode = iota
	OpLoadGetter

	// Generic math/compare (with runtime type dispatch)
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

	// Specialized int64 ops (no type check, inlined)
	OpAddInt
	OpSubInt
	OpMulInt
	OpDivInt
	OpEqInt
	OpNeInt
	OpLtInt
	OpLteInt
	OpGtInt
	OpGteInt

	// Specialized float64 ops (no type check, inlined)
	OpAddFloat
	OpSubFloat
	OpMulFloat
	OpDivFloat
	OpEqFloat
	OpNeFloat
	OpLtFloat
	OpLteFloat
	OpGtFloat
	OpGteFloat

	// Specialized string ops (no type check, optimized pointer+length comparison)
	OpEqString
	OpNeString

	// Control flow
	OpJump
	OpJumpIfTrue
	OpJumpIfFalse
	OpJumpIfTruePop  // Pop top of stack if true; otherwise jump
	OpJumpIfFalsePop // Pop top of stack if false; otherwise jump
	OpPop
	OpNot

	// Stack manipulation
	OpDup // Duplicate top of stack

	// Unary ops
	OpNegInt   // Negate int64
	OpNegFloat // Negate float64

	// Cached attribute access (Phase 3)
	OpLoadAttrCached // Load attribute via cached accessor; arg = accessor index
	OpSetAttrCached  // Set attribute via cached setter; arg = setter index
	OpLoadAttrFast   // Load attribute via fast path; arg = key index
	OpSetAttrFast    // Set attribute via fast path; arg = key index

	// Direct field access opcodes (Phase 4)
	OpGetBody      // Get log body; pushes value to stack
	OpSetBody      // Set log body; pops value from stack
	OpGetSeverity  // Get severity number; pushes int to stack
	OpSetSeverity  // Set severity number; pops int from stack
	OpGetTimestamp // Get timestamp; pushes int (UnixNano) to stack
	OpSetTimestamp // Set timestamp; pops int (UnixNano) from stack

	// Compare against const (Phase 4)
	OpEqConst  // Compare top of stack with const; arg = const index
	OpNeConst  // Compare top of stack with const; arg = const index
	OpLtConst  // Compare top of stack with const; arg = const index
	OpLteConst // Compare top of stack with const; arg = const index
	OpGtConst  // Compare top of stack with const; arg = const index
	OpGteConst // Compare top of stack with const; arg = const index

	// Direct span field access
	OpGetSpanName                       // Get span name; pushes string to stack
	OpSetSpanName                       // Set span name; pops string from stack
	OpGetSpanStartTime                  // Get span start time (UnixNano); pushes int to stack
	OpSetSpanStartTime                  // Set span start time (UnixNano); pops int from stack
	OpGetSpanEndTime                    // Get span end time (UnixNano); pushes int to stack
	OpSetSpanEndTime                    // Set span end time (UnixNano); pops int from stack
	OpGetSpanKind                       // Get span kind; pushes int to stack
	OpSetSpanKind                       // Set span kind; pops int from stack
	OpGetSpanStatus                     // Get span status code; pushes int to stack
	OpSetSpanStatus                     // Set span status code; pops int from stack
	OpGetMetricName                     // Get metric name; pushes string to stack
	OpSetMetricName                     // Set metric name; pops string from stack
	OpGetMetricUnit                     // Get metric unit; pushes string to stack
	OpSetMetricUnit                     // Set metric unit; pops string from stack
	OpGetMetricType                     // Get metric type; pushes int to stack
	OpGetSpanStatusMsg                  // Get span status message; pushes string to stack
	OpSetSpanStatusMsg                  // Set span status message; pops string from stack
	OpGetResourceDroppedAttributesCount // Get resource dropped attributes count; pushes int to stack
	OpSetResourceDroppedAttributesCount // Set resource dropped attributes count; pops int from stack
	OpGetResourceSchemaURL              // Get resource schema URL; pushes string to stack
	OpSetResourceSchemaURL              // Set resource schema URL; pops string from stack
	OpGetScopeName                      // Get scope name; pushes string to stack
	OpSetScopeName                      // Set scope name; pops string from stack
	OpGetScopeVersion                   // Get scope version; pushes string to stack
	OpSetScopeVersion                   // Set scope version; pops string from stack
	OpGetScopeDroppedAttributesCount    // Get scope dropped attributes count; pushes int to stack
	OpSetScopeDroppedAttributesCount    // Set scope dropped attributes count; pops int from stack
	OpGetScopeSchemaURL                 // Get scope schema URL; pushes string to stack
	OpSetScopeSchemaURL                 // Set scope schema URL; pops string from stack
	OpGetObservedTimestamp              // Get log observed time (UnixNano); pushes int to stack
	OpSetObservedTimestamp              // Set log observed time (UnixNano); pops int from stack
	OpGetSeverityText                   // Get log severity text; pushes string to stack
	OpSetSeverityText                   // Set log severity text; pops string from stack
	OpGetLogFlags                       // Get log flags; pushes int to stack
	OpSetLogFlags                       // Set log flags; pops int from stack
	OpGetSpanTraceID                    // Get span trace ID bytes; pushes bytes to stack
	OpGetSpanID                         // Get span ID bytes; pushes bytes to stack
	OpGetSpanParentID                   // Get parent span ID bytes; pushes bytes to stack
	OpGetSpanTraceIDString              // Get span trace ID as hex string; pushes string to stack
	OpGetSpanIDString                   // Get span ID as hex string; pushes string to stack
	OpGetSpanParentIDString             // Get parent span ID as hex string; pushes string to stack
	OpGetSpanTraceState                 // Get span trace state as raw string; pushes string to stack
	OpSetSpanTraceState                 // Set span trace state from raw string; pops string from stack
	OpGetSpanTraceStateKey              // Get span trace state value for key; pushes string or nil to stack
	OpSetSpanTraceStateKey              // Set span trace state value for key; pops string from stack
	OpGetSpanDroppedAttributesCount     // Get span dropped attributes count; pushes int to stack
	OpSetSpanDroppedAttributesCount     // Set span dropped attributes count; pops int from stack
	OpGetSpanDroppedEventsCount         // Get span dropped events count; pushes int to stack
	OpSetSpanDroppedEventsCount         // Set span dropped events count; pops int from stack
	OpGetSpanDroppedLinksCount          // Get span dropped links count; pushes int to stack
	OpSetSpanDroppedLinksCount          // Set span dropped links count; pops int from stack
	OpGetMetricDescription              // Get metric description; pushes string to stack
	OpSetMetricDescription              // Set metric description; pops string from stack
	OpGetMetricAggTemporality           // Get metric aggregation temporality; pushes int or nil to stack
	OpSetMetricAggTemporality           // Set metric aggregation temporality; pops int from stack
	OpGetMetricIsMonotonic              // Get metric monotonicity; pushes bool or nil to stack
	OpSetMetricIsMonotonic              // Set metric monotonicity; pops bool from stack

	OpInt            // Convert top of stack to int (OTTL Int converter semantics)
	OpIsNil          // Check top of stack for nil (TypeNone)
	OpIsType         // Check top of stack type; arg = ir.Type
	OpIsMatch        // Regex match for string-like target; arg = regex index
	OpIsMatchDynamic // Regex match with dynamic pattern; pops pattern + target
	OpCall           // Call function; arg = callsite index

	// Superinstructions (fused operations for common patterns)
	OpAttrEqConstString     // Load attr (cached) + compare string const; arg = packed(attrIdx, constIdx); pushes bool
	OpAttrNeConstString     // Load attr (cached) + compare string const (not equal); arg = packed(attrIdx, constIdx); pushes bool
	OpAttrFastEqConstString // Load attr (fast) + compare string const; arg = packed(keyIdx, constIdx); pushes bool
	OpAttrFastNeConstString // Load attr (fast) + compare string const (not equal); arg = packed(keyIdx, constIdx); pushes bool
	OpAttrEqConst           // Load attr (cached) + compare numeric/bool const; arg = packed(attrIdx, constIdx); pushes bool
	OpAttrNeConst           // Load attr (cached) + compare numeric/bool const (not equal); arg = packed(attrIdx, constIdx); pushes bool
	OpAttrFastEqConst       // Load attr (fast) + compare numeric/bool const; arg = packed(keyIdx, constIdx); pushes bool
	OpAttrFastNeConst       // Load attr (fast) + compare numeric/bool const (not equal); arg = packed(keyIdx, constIdx); pushes bool

	// IsMatch superinstructions (fused attr load + regex match)
	OpAttrIsMatchConst     // Load attr (cached) + regex match; arg = packed(attrIdx, regexIdx); pushes bool
	OpAttrFastIsMatchConst // Load attr (fast) + regex match; arg = packed(keyIdx, regexIdx); pushes bool

	// Nil check superinstructions (fused attr load + nil check)
	OpAttrIsNil        // Load attr (cached) + check nil; arg = attrIdx; pushes bool
	OpAttrIsNotNil     // Load attr (cached) + check not nil; arg = attrIdx; pushes bool
	OpAttrFastIsNil    // Load attr (fast) + check nil; arg = keyIdx; pushes bool
	OpAttrFastIsNotNil // Load attr (fast) + check not nil; arg = keyIdx; pushes bool

	// Superinstructions: attr compare const + conditional jump (consumes bool)
	OpAttrEqConstJumpIfFalsePop       // cached attr == const, jump-if-false-pop; arg = packed(attrIdx,constIdx)
	OpAttrNeConstJumpIfFalsePop       // cached attr != const, jump-if-false-pop
	OpAttrFastEqConstJumpIfFalsePop   // fast attr == const, jump-if-false-pop; arg = packed(keyIdx,constIdx)
	OpAttrFastNeConstJumpIfFalsePop   // fast attr != const, jump-if-false-pop
	OpAttrEqConstStringJumpIfFalsePop // cached attr == const string, jump-if-false-pop
	OpAttrNeConstStringJumpIfFalsePop // cached attr != const string, jump-if-false-pop
	OpAttrFastEqConstStringJumpIfFalsePop // fast attr == const string, jump-if-false-pop
	OpAttrFastNeConstStringJumpIfFalsePop // fast attr != const string, jump-if-false-pop
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

func PackAttrConst(attrIdx, constIdx uint32) uint32 {
	return (attrIdx&0xFFF)<<12 | (constIdx & 0xFFF)
}

func UnpackAttrConst(arg uint32) (attrIdx, constIdx uint32) {
	return (arg >> 12) & 0xFFF, arg & 0xFFF
}
