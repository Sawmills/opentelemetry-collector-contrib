// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

// PathAccessor is a pre-compiled accessor that retrieves a value directly as ir.Value.
// This avoids the overhead of Go interface{} boxing in the hot path.
// The accessor is compiled once per program and receives ctx/tCtx as arguments
// to avoid per-run closure allocations.
// Generic over K to eliminate interface{} conversions entirely.
type PathAccessor[K any] func(ctx context.Context, tCtx K) (ir.Value, error)

// PathSetter is a pre-compiled setter that writes a value to an attribute.
// This is the write counterpart to PathAccessor for attribute mutations.
// Generic over K to eliminate interface{} conversions entirely.
type PathSetter[K any] func(ctx context.Context, tCtx K, val ir.Value) error

// Program is a minimal bytecode program for the micro-VM.
// Generic over K to support typed path accessors without interface{} overhead.
type Program[K any] struct {
	Code      []ir.Instruction
	Consts    []ir.Value
	Accessors []PathAccessor[K] // cached attribute accessors for OpLoadAttrCached
	Setters   []PathSetter[K]   // cached attribute setters for OpSetAttrCached
	GasLimit  uint64
}

// ProgramAny is an alias for Program[any] for backward compatibility.
type ProgramAny = Program[any]

var (
	ErrStackUnderflow = errors.New("stack underflow")
	ErrStackOverflow  = errors.New("stack overflow")
	ErrInvalidOpcode  = errors.New("invalid opcode")
	ErrInvalidConst   = errors.New("invalid const index")
	ErrInvalidGetter   = errors.New("invalid getter index")
	ErrInvalidAccessor = errors.New("invalid accessor index")
	ErrInvalidSetter   = errors.New("invalid setter index")
	ErrInvalidJump    = errors.New("invalid jump target")
	ErrTypeMismatch   = errors.New("type mismatch")
	ErrEmptyStack     = errors.New("empty stack")
	ErrDivideByZero   = errors.New("divide by zero")
	ErrGasExhausted   = errors.New("gas exhausted")
)

const (
	DefaultGasLimit        uint64 = 10_000
	BackwardJumpPenaltyGas uint64 = 9
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
// For performance-critical paths, use RunWithStackAndContext directly with stack-allocated arrays.
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
// Note: This method only works with Program[any] for backward compatibility.
func (m *MicroVM) Run(p *ProgramAny) (ir.Value, error) {
	return runProgram(m.stack, p, nil)
}

// RunWithLoader executes the program and uses loader for OpLoadGetter.
// Note: This method only works with Program[any] for backward compatibility.
func (m *MicroVM) RunWithLoader(p *ProgramAny, loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	return runProgram(m.stack, p, loader)
}

// RunWithStack executes a program using the provided stack.
// Note: This function only works with Program[any] for backward compatibility.
func RunWithStack(stack []ir.Value, p *ProgramAny) (ir.Value, error) {
	return runProgram(stack, p, nil)
}

// RunWithStackGeneric executes a generic program using the provided stack.
// For const-only programs (no accessors), this avoids the need for context.
func RunWithStackGeneric[K any](stack []ir.Value, p *Program[K]) (ir.Value, error) {
	return runProgram(stack, p, nil)
}

// RunWithStackAndLoader executes a program using the provided stack and loader.
// Note: This function only works with Program[any] for backward compatibility.
func RunWithStackAndLoader(stack []ir.Value, p *ProgramAny, loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	return runProgram(stack, p, loader)
}

// RunWithStackAndContext executes a program using the provided stack, ctx and tCtx.
// This is the fastest execution path for programs with path accessors - no loader closure needed.
// Accessors are called with ctx/tCtx directly, avoiding per-run allocations.
// Generic over K to eliminate interface{} conversions entirely.
func RunWithStackAndContext[K any](stack []ir.Value, p *Program[K], ctx context.Context, tCtx K) (ir.Value, error) {
	return runProgramWithContext(stack, p, ctx, tCtx)
}

func runProgram[K any](stack []ir.Value, p *Program[K], loader func(uint32) (ir.Value, error)) (ir.Value, error) {
	gas := p.GasLimit
	if gas == 0 {
		gas = DefaultGasLimit
	}
	sp := 0
	code := p.Code
	consts := p.Consts
	codeLen := len(code)

	for ip := 0; ip < codeLen; ip++ {
		if gas == 0 {
			return ir.Value{}, ErrGasExhausted
		}
		gas--
		inst := code[ip]
		op := inst.Op()

		switch op {
		case ir.OpLoadConst:
			idx := inst.Arg()
			if int(idx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = consts[idx]
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

		case ir.OpAddInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) + int64(stack[sp].Num))

		case ir.OpSubInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) - int64(stack[sp].Num))

		case ir.OpMulInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) * int64(stack[sp].Num))

		case ir.OpDivInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			if stack[sp-1].Num == 0 {
				return ir.Value{}, ErrDivideByZero
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) / int64(stack[sp].Num))

		case ir.OpEqInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) == int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpNeInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) != int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLtInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) < int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLteInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) <= int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGtInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) > int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGteInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) >= int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpAddFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a + b)

		case ir.OpSubFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a - b)

		case ir.OpMulFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a * b)

		case ir.OpDivFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			b := math.Float64frombits(stack[sp-1].Num)
			if b == 0 {
				return ir.Value{}, ErrDivideByZero
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			stack[sp-1].Num = math.Float64bits(a / b)

		case ir.OpEqFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp-1].Num == stack[sp].Num {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpNeFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp-1].Num != stack[sp].Num {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLtFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			if a < b {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLteFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			if a <= b {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGtFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			if a > b {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGteFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			if a >= b {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpAdd, ir.OpSub, ir.OpMul, ir.OpDiv:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			b := stack[sp-1]
			a := stack[sp-2]
			sp -= 2
			result, err := mathOp(op, a, b)
			if err != nil {
				return ir.Value{}, err
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

			// Fast path for common cases
			if op == ir.OpEq {
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

			result, err := compareOp(op, a, b)
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
			if target < 0 || target > codeLen {
				return ir.Value{}, ErrInvalidJump
			}
			if target <= ip {
				if gas <= BackwardJumpPenaltyGas {
					return ir.Value{}, ErrGasExhausted
				}
				gas -= BackwardJumpPenaltyGas
			}
			ip = target - 1

		case ir.OpJumpIfTrue:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			if val.Num != 0 {
				target := int(inst.Arg())
				if target < 0 || target > codeLen {
					return ir.Value{}, ErrInvalidJump
				}
				if target <= ip {
					if gas <= BackwardJumpPenaltyGas {
						return ir.Value{}, ErrGasExhausted
					}
					gas -= BackwardJumpPenaltyGas
				}
				ip = target - 1
			}

		case ir.OpJumpIfFalse:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			if val.Num == 0 {
				target := int(inst.Arg())
				if target < 0 || target > codeLen {
					return ir.Value{}, ErrInvalidJump
				}
				if target <= ip {
					if gas <= BackwardJumpPenaltyGas {
						return ir.Value{}, ErrGasExhausted
					}
					gas -= BackwardJumpPenaltyGas
				}
				ip = target - 1
			}

		case ir.OpDup:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = stack[sp-1]
			sp++

		case ir.OpNegInt:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1].Num = uint64(-int64(stack[sp-1].Num))

		case ir.OpNegFloat:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1].Num = stack[sp-1].Num ^ (1 << 63) // flip sign bit

		case ir.OpLoadAttrCached:
			// OpLoadAttrCached requires context; use RunWithStackAndContext instead
			return ir.Value{}, ErrInvalidOpcode

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

// runProgramWithContext executes the program with ctx/tCtx for OpLoadAttrCached.
// This is the fast path for programs with path accessors.
// Generic over K to eliminate interface{} conversions entirely.
func runProgramWithContext[K any](stack []ir.Value, p *Program[K], ctx context.Context, tCtx K) (ir.Value, error) {
	gas := p.GasLimit
	if gas == 0 {
		gas = DefaultGasLimit
	}
	sp := 0
	code := p.Code
	consts := p.Consts
	codeLen := len(code)

	for ip := 0; ip < codeLen; ip++ {
		if gas == 0 {
			return ir.Value{}, ErrGasExhausted
		}
		gas--
		inst := code[ip]
		op := inst.Op()

		switch op {
		case ir.OpLoadConst:
			idx := inst.Arg()
			if int(idx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = consts[idx]
			sp++

		case ir.OpLoadAttrCached:
			idx := inst.Arg()
			if int(idx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[idx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			val, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = val
			sp++

		case ir.OpSetAttrCached:
			idx := inst.Arg()
			if int(idx) >= len(p.Setters) {
				return ir.Value{}, ErrInvalidSetter
			}
			setter := p.Setters[idx]
			if setter == nil {
				return ir.Value{}, ErrInvalidSetter
			}
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			val := stack[sp]
			if err := setter(ctx, tCtx, val); err != nil {
				return ir.Value{}, err
			}

		case ir.OpAddInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) + int64(stack[sp].Num))

		case ir.OpSubInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) - int64(stack[sp].Num))

		case ir.OpMulInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) * int64(stack[sp].Num))

		case ir.OpDivInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			if stack[sp-1].Num == 0 {
				return ir.Value{}, ErrDivideByZero
			}
			sp--
			stack[sp-1].Num = uint64(int64(stack[sp-1].Num) / int64(stack[sp].Num))

		case ir.OpEqInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) == int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpNeInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) != int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLtInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) < int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpLteInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) <= int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGtInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) > int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpGteInt:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if int64(stack[sp-1].Num) >= int64(stack[sp].Num) {
				stack[sp-1] = ir.BoolValue(true)
			} else {
				stack[sp-1] = ir.BoolValue(false)
			}

		case ir.OpAddFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a + b)

		case ir.OpSubFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a - b)

		case ir.OpMulFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1].Num = math.Float64bits(a * b)

		case ir.OpDivFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			b := math.Float64frombits(stack[sp-1].Num)
			if b == 0 {
				return ir.Value{}, ErrDivideByZero
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			stack[sp-1].Num = math.Float64bits(a / b)

		case ir.OpEqFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a == b)

		case ir.OpNeFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a != b)

		case ir.OpLtFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a < b)

		case ir.OpLteFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a <= b)

		case ir.OpGtFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a > b)

		case ir.OpGteFloat:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			a := math.Float64frombits(stack[sp-1].Num)
			b := math.Float64frombits(stack[sp].Num)
			stack[sp-1] = ir.BoolValue(a >= b)

		case ir.OpAdd, ir.OpSub, ir.OpMul, ir.OpDiv:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			result, err := mathOp(op, stack[sp-1], stack[sp])
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = result

		case ir.OpEq, ir.OpNe, ir.OpLt, ir.OpLte, ir.OpGt, ir.OpGte:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			result, err := compareOp(op, stack[sp-1], stack[sp])
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = result

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
			if target < 0 || target > codeLen {
				return ir.Value{}, ErrInvalidJump
			}
			if target <= ip {
				if gas <= BackwardJumpPenaltyGas {
					return ir.Value{}, ErrGasExhausted
				}
				gas -= BackwardJumpPenaltyGas
			}
			ip = target - 1

		case ir.OpJumpIfTrue:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			if val.Num != 0 {
				target := int(inst.Arg())
				if target < 0 || target > codeLen {
					return ir.Value{}, ErrInvalidJump
				}
				if target <= ip {
					if gas <= BackwardJumpPenaltyGas {
						return ir.Value{}, ErrGasExhausted
					}
					gas -= BackwardJumpPenaltyGas
				}
				ip = target - 1
			}

		case ir.OpJumpIfFalse:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			val := stack[sp-1]
			if val.Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			if val.Num == 0 {
				target := int(inst.Arg())
				if target < 0 || target > codeLen {
					return ir.Value{}, ErrInvalidJump
				}
				if target <= ip {
					if gas <= BackwardJumpPenaltyGas {
						return ir.Value{}, ErrGasExhausted
					}
					gas -= BackwardJumpPenaltyGas
				}
				ip = target - 1
			}

		case ir.OpDup:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = stack[sp-1]
			sp++

		case ir.OpNegInt:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1].Num = uint64(-int64(stack[sp-1].Num))

		case ir.OpNegFloat:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1].Num = stack[sp-1].Num ^ (1 << 63)

		// Direct field access opcodes - placeholder implementations
		// These will be wired up when the compiler emits them
		case ir.OpGetBody, ir.OpSetBody, ir.OpGetSeverity, ir.OpSetSeverity, ir.OpGetTimestamp, ir.OpSetTimestamp:
			return ir.Value{}, ErrInvalidOpcode

		default:
			return ir.Value{}, ErrInvalidOpcode
		}
	}
	if sp == 0 {
		return ir.Value{}, ErrEmptyStack
	}
	return stack[sp-1], nil
}
