// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"

import (
	"errors"
	"math"
	"strings"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

// Program is a minimal bytecode program for the micro-VM.
type Program struct {
	Code   []ir.Instruction
	Consts []ir.Value
}

var (
	ErrStackUnderflow = errors.New("stack underflow")
	ErrStackOverflow  = errors.New("stack overflow")
	ErrInvalidOpcode  = errors.New("invalid opcode")
	ErrInvalidConst   = errors.New("invalid const index")
	ErrInvalidGetter  = errors.New("invalid getter index")
	ErrInvalidJump    = errors.New("invalid jump target")
	ErrTypeMismatch   = errors.New("type mismatch")
	ErrEmptyStack     = errors.New("empty stack")
	ErrDivideByZero   = errors.New("divide by zero")
)

// StackPool reuses operand stacks across VM executions.
type StackPool struct {
	size int
	pool sync.Pool
}

// NewStackPool creates a pool of stacks with the given size.
func NewStackPool(size int) *StackPool {
	return &StackPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				return make([]ir.Value, size)
			},
		},
	}
}

// Get returns a stack from the pool.
func (p *StackPool) Get() []ir.Value {
	return p.pool.Get().([]ir.Value)
}

// Put returns a stack to the pool.
func (p *StackPool) Put(stack []ir.Value) {
	if cap(stack) < p.size {
		return
	}
	p.pool.Put(stack[:p.size])
}

// MicroVM executes a tiny subset of the OTTL bytecode for benchmarking.
type MicroVM struct {
	stack []ir.Value
	pool  *StackPool
}

// NewMicroVM creates a VM with a fixed-size stack.
func NewMicroVM(stackSize int) *MicroVM {
	return &MicroVM{stack: make([]ir.Value, stackSize)}
}

// NewMicroVMFromPool creates a VM that borrows its stack from a pool.
func NewMicroVMFromPool(pool *StackPool) *MicroVM {
	return &MicroVM{stack: pool.Get(), pool: pool}
}

// Release returns the stack to the pool when constructed with NewMicroVMFromPool.
func (m *MicroVM) Release() {
	if m.pool == nil || m.stack == nil {
		return
	}
	m.pool.Put(m.stack)
	m.stack = nil
}

// Run executes the program and returns the top value on the stack.
func (m *MicroVM) Run(p *Program) (ir.Value, error) {
	return runProgram(m.stack, p, nil)
}

// RunWithLoader executes the program and uses loader for OpLoadGetter.
func (m *MicroVM) RunWithLoader(p *Program, loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	return runProgram(m.stack, p, loader)
}

// RunWithStack executes a program using the provided stack.
func RunWithStack(stack []ir.Value, p *Program) (ir.Value, error) {
	return runProgram(stack, p, nil)
}

// RunWithStackAndLoader executes a program using the provided stack and loader.
func RunWithStackAndLoader(stack []ir.Value, p *Program, loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	return runProgram(stack, p, loader)
}

func runProgram(stack []ir.Value, p *Program, loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	sp := 0
	for ip := 0; ip < len(p.Code); ip++ {
		inst := p.Code[ip]
		switch inst.Op() {
		case ir.OpLoadConst:
			idx := inst.Arg()
			if int(idx) >= len(p.Consts) {
				return ir.Value{}, ErrInvalidConst
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = p.Consts[idx]
			sp++
		case ir.OpLoadGetter:
			if loader == nil {
				return ir.Value{}, ErrInvalidGetter
			}
			idx := inst.Arg()
			val, err := loader(idx)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = val
			sp++
		case ir.OpAdd, ir.OpSub, ir.OpMul, ir.OpDiv:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			b := stack[sp-1]
			a := stack[sp-2]
			sp -= 2
			result, err := mathOp(inst.Op(), a, b)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = result
			sp++
		case ir.OpEq, ir.OpNe, ir.OpLt, ir.OpLte, ir.OpGt, ir.OpGte:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			b := stack[sp-1]
			a := stack[sp-2]
			sp -= 2

			if inst.Op() == ir.OpEq {
				switch {
				case a.Type == ir.TypeInt && b.Type == ir.TypeInt:
					stack[sp] = ir.BoolValue(int64(a.Num) == int64(b.Num))
					sp++
					continue
				case a.Type == ir.TypeFloat && b.Type == ir.TypeFloat:
					stack[sp] = ir.BoolValue(a.Num == b.Num)
					sp++
					continue
				}
			}

			result, err := compareOp(inst.Op(), a, b)
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp] = result
			sp++
		case ir.OpPop:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
		case ir.OpNot:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			stack[sp-1] = ir.BoolValue(val.Num == 0)
		case ir.OpJump:
			target := int(inst.Arg())
			if target < 0 || target > len(p.Code) {
				return ir.Value{}, ErrInvalidJump
			}
			ip = target - 1
		case ir.OpJumpIfTrue, ir.OpJumpIfFalse:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			cond := val.Num != 0
			shouldJump := inst.Op() == ir.OpJumpIfTrue && cond
			if inst.Op() == ir.OpJumpIfFalse && !cond {
				shouldJump = true
			}
			if shouldJump {
				target := int(inst.Arg())
				if target < 0 || target > len(p.Code) {
					return ir.Value{}, ErrInvalidJump
				}
				ip = target - 1
			}
		default:
			return ir.Value{}, ErrInvalidOpcode
		}
	}
	if sp == 0 {
		return ir.Value{}, ErrEmptyStack
	}
	return stack[sp-1], nil
}

func mathOp(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	if a.Type != b.Type {
		return ir.Value{}, ErrTypeMismatch
	}
	switch a.Type {
	case ir.TypeInt:
		return mathOpInt(op, int64(a.Num), int64(b.Num))
	case ir.TypeFloat:
		return mathOpFloat(op, math.Float64frombits(a.Num), math.Float64frombits(b.Num))
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func mathOpInt(op ir.Opcode, a, b int64) (ir.Value, error) {
	switch op {
	case ir.OpAdd:
		return ir.Int64Value(a + b), nil
	case ir.OpSub:
		return ir.Int64Value(a - b), nil
	case ir.OpMul:
		return ir.Int64Value(a * b), nil
	case ir.OpDiv:
		if b == 0 {
			return ir.Value{}, ErrDivideByZero
		}
		return ir.Int64Value(a / b), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}

func mathOpFloat(op ir.Opcode, a, b float64) (ir.Value, error) {
	switch op {
	case ir.OpAdd:
		return ir.Float64Value(a + b), nil
	case ir.OpSub:
		return ir.Float64Value(a - b), nil
	case ir.OpMul:
		return ir.Float64Value(a * b), nil
	case ir.OpDiv:
		if b == 0 {
			return ir.Value{}, ErrDivideByZero
		}
		return ir.Float64Value(a / b), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}

func compareOp(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	switch {
	case a.Type == ir.TypeInt && b.Type == ir.TypeInt:
		return compareInts(op, int64(a.Num), int64(b.Num))
	case a.Type == ir.TypeFloat && b.Type == ir.TypeFloat:
		return compareFloats(op, math.Float64frombits(a.Num), math.Float64frombits(b.Num))
	case a.Type == ir.TypeInt && b.Type == ir.TypeFloat:
		return compareFloats(op, float64(int64(a.Num)), math.Float64frombits(b.Num))
	case a.Type == ir.TypeFloat && b.Type == ir.TypeInt:
		return compareFloats(op, math.Float64frombits(a.Num), float64(int64(b.Num)))
	case a.Type == ir.TypeBool && b.Type == ir.TypeBool:
		return compareBools(op, a.Num != 0, b.Num != 0)
	case a.Type == ir.TypeString && b.Type == ir.TypeString:
		return compareStrings(op, a, b)
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func compareInts(op ir.Opcode, a, b int64) (ir.Value, error) {
	switch op {
	case ir.OpEq:
		return ir.BoolValue(a == b), nil
	case ir.OpNe:
		return ir.BoolValue(a != b), nil
	case ir.OpLt:
		return ir.BoolValue(a < b), nil
	case ir.OpLte:
		return ir.BoolValue(a <= b), nil
	case ir.OpGt:
		return ir.BoolValue(a > b), nil
	case ir.OpGte:
		return ir.BoolValue(a >= b), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}

func compareFloats(op ir.Opcode, a, b float64) (ir.Value, error) {
	switch op {
	case ir.OpEq:
		return ir.BoolValue(a == b), nil
	case ir.OpNe:
		return ir.BoolValue(a != b), nil
	case ir.OpLt:
		return ir.BoolValue(a < b), nil
	case ir.OpLte:
		return ir.BoolValue(a <= b), nil
	case ir.OpGt:
		return ir.BoolValue(a > b), nil
	case ir.OpGte:
		return ir.BoolValue(a >= b), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}

func compareBools(op ir.Opcode, a, b bool) (ir.Value, error) {
	switch op {
	case ir.OpEq:
		return ir.BoolValue(a == b), nil
	case ir.OpNe:
		return ir.BoolValue(a != b), nil
	case ir.OpLt:
		return ir.BoolValue(!a && b), nil
	case ir.OpLte:
		return ir.BoolValue(!a || b), nil
	case ir.OpGt:
		return ir.BoolValue(a && !b), nil
	case ir.OpGte:
		return ir.BoolValue(a || !b), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}

func compareStrings(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	as, ok := a.String()
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	bs, ok := b.String()
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	cmp := strings.Compare(as, bs)
	switch op {
	case ir.OpEq:
		return ir.BoolValue(cmp == 0), nil
	case ir.OpNe:
		return ir.BoolValue(cmp != 0), nil
	case ir.OpLt:
		return ir.BoolValue(cmp < 0), nil
	case ir.OpLte:
		return ir.BoolValue(cmp <= 0), nil
	case ir.OpGt:
		return ir.BoolValue(cmp > 0), nil
	case ir.OpGte:
		return ir.BoolValue(cmp >= 0), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
	}
}
