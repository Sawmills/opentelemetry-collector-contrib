// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/trace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
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
type AttrGetter[K any] func(tCtx K, key string) (ir.Value, error)
type AttrSetter[K any] func(tCtx K, key string, val ir.Value) error

// CallSite represents a callable function with VM value arguments.
type CallSite[K any] struct {
	ArgCount int
	Func     func(ctx context.Context, tCtx K, args []ir.Value) (ir.Value, error)
}

// LogRecordContext exposes a log record for direct field opcodes.
type LogRecordContext interface {
	GetLogRecord() plog.LogRecord
}

// SpanContext exposes a span for direct field opcodes.
type SpanContext interface {
	GetSpan() ptrace.Span
}

// MetricContext exposes a metric for direct field opcodes.
type MetricContext interface {
	GetMetric() pmetric.Metric
}

// ResourceContext exposes a resource for direct field opcodes.
type ResourceContext interface {
	GetResource() pcommon.Resource
}

// ScopeContext exposes an instrumentation scope for direct field opcodes.
type ScopeContext interface {
	GetInstrumentationScope() pcommon.InstrumentationScope
}

// Program is a minimal bytecode program for the micro-VM.
// Generic over K to support typed path accessors without interface{} overhead.
type Program[K any] struct {
	Code                    []ir.Instruction
	Consts                  []ir.Value
	Regexps                 []*regexp.Regexp
	dynamicRegexps          sync.Map
	CallSites               []CallSite[K]
	Accessors               []PathAccessor[K] // cached attribute accessors for OpLoadAttrCached
	Setters                 []PathSetter[K]   // cached attribute setters for OpSetAttrCached
	AttrKeys                []string          // fast attribute keys for OpLoadAttrFast
	AttrGetter              AttrGetter[K]     // fast attribute getter for OpLoadAttrFast
	AttrSetter              AttrSetter[K]     // fast attribute setter for OpSetAttrFast
	LogRecordGetter         func(K) plog.LogRecord
	SpanGetter              func(K) ptrace.Span
	MetricGetter            func(K) pmetric.Metric
	ResourceGetter          func(K) pcommon.Resource
	ResourceSchemaURLGetter func(K) string
	ResourceSchemaURLSetter func(K, string)
	ScopeGetter             func(K) pcommon.InstrumentationScope
	ScopeSchemaURLGetter    func(K) string
	ScopeSchemaURLSetter    func(K, string)
	GasLimit                uint64
}

// ProgramAny is an alias for Program[any] for backward compatibility.
type ProgramAny = Program[any]

var (
	ErrStackUnderflow  = errors.New("stack underflow")
	ErrStackOverflow   = errors.New("stack overflow")
	ErrInvalidOpcode   = errors.New("invalid opcode")
	ErrInvalidConst    = errors.New("invalid const index")
	ErrInvalidRegex    = errors.New("invalid regex index")
	ErrInvalidType     = errors.New("invalid type index")
	ErrInvalidCallSite = errors.New("invalid callsite index")
	ErrInvalidGetter   = errors.New("invalid getter index")
	ErrInvalidAccessor = errors.New("invalid accessor index")
	ErrInvalidSetter   = errors.New("invalid setter index")
	ErrInvalidJump     = errors.New("invalid jump target")
	ErrTypeMismatch    = errors.New("type mismatch")
	ErrEmptyStack      = errors.New("empty stack")
	ErrDivideByZero    = errors.New("divide by zero")
	ErrGasExhausted    = errors.New("gas exhausted")
)

const (
	DefaultGasLimit        uint64 = 10_000
	BackwardJumpPenaltyGas uint64 = 9
)

// StackHolder wraps a reusable stack slice.
type StackHolder struct {
	Stack []ir.Value
}

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
				return &StackHolder{Stack: make([]ir.Value, size)}
			},
		},
	}
}

// GetHolder returns a stack holder from the pool.
func (p *StackPool) GetHolder() *StackHolder {
	return p.pool.Get().(*StackHolder)
}

// PutHolder returns a stack holder to the pool.
func (p *StackPool) PutHolder(holder *StackHolder) {
	if holder == nil || cap(holder.Stack) < p.size {
		return
	}
	holder.Stack = holder.Stack[:p.size]
	p.pool.Put(holder)
}

// Get returns a stack from the pool.
func (p *StackPool) Get() []ir.Value {
	return p.GetHolder().Stack
}

// Put returns a stack to the pool.
func (p *StackPool) Put(stack []ir.Value) {
	if cap(stack) < p.size {
		return
	}
	p.PutHolder(&StackHolder{Stack: stack[:p.size]})
}

// MicroVM executes a tiny subset of the OTTL bytecode for benchmarking.
// For performance-critical paths, use RunWithStackAndContext directly with stack-allocated arrays.
type MicroVM struct {
	stack     []ir.Value
	pool      *StackPool
	stackHold *StackHolder
}

// NewMicroVM creates a VM with a fixed-size stack.
func NewMicroVM(stackSize int) *MicroVM {
	return &MicroVM{stack: make([]ir.Value, stackSize)}
}

// NewMicroVMFromPool creates a VM that borrows its stack from a pool.
func NewMicroVMFromPool(pool *StackPool) *MicroVM {
	holder := pool.GetHolder()
	return &MicroVM{stack: holder.Stack, pool: pool, stackHold: holder}
}

// Release returns the stack to the pool when constructed with NewMicroVMFromPool.
func (m *MicroVM) Release() {
	if m.pool == nil || m.stack == nil {
		return
	}
	if m.stackHold != nil {
		m.pool.PutHolder(m.stackHold)
		m.stackHold = nil
	} else {
		m.pool.Put(m.stack)
	}
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
	if len(p.Code) == 1 {
		inst := p.Code[0]
		op := inst.Op()
		arg := inst.Arg()

		switch op {
	case ir.OpAttrFastEqConstString:
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		return ir.BoolValue(stringsEqual(attrVal, p.Consts[constIdx])), nil

	case ir.OpAttrFastNeConstString:
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		return ir.BoolValue(!stringsEqual(attrVal, p.Consts[constIdx])), nil

	case ir.OpAttrFastEqConst:
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		return ir.BoolValue(attrVal.Type == p.Consts[constIdx].Type && attrVal.Num == p.Consts[constIdx].Num), nil

	case ir.OpAttrFastNeConst:
		keyIdx, constIdx := ir.UnpackAttrConst(arg)
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		return ir.BoolValue(!(attrVal.Type == p.Consts[constIdx].Type && attrVal.Num == p.Consts[constIdx].Num)), nil

		case ir.OpAttrEqConstString:
			attrIdx, constIdx := ir.UnpackAttrConst(arg)
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(stringsEqual(attrVal, p.Consts[constIdx])), nil

		case ir.OpAttrNeConstString:
			attrIdx, constIdx := ir.UnpackAttrConst(arg)
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(!stringsEqual(attrVal, p.Consts[constIdx])), nil

		case ir.OpAttrIsMatchConst:
			attrIdx, regexIdx := ir.UnpackAttrConst(arg)
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(regexIdx) >= len(p.Regexps) || p.Regexps[regexIdx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				return ir.BoolValue(p.Regexps[regexIdx].MatchString(str)), nil
			}
			target, ok, err := stringLikeFromValue(attrVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				return ir.BoolValue(false), nil
			}
			return ir.BoolValue(p.Regexps[regexIdx].MatchString(target)), nil

		case ir.OpAttrFastIsMatchConst:
			keyIdx, regexIdx := ir.UnpackAttrConst(arg)
			if int(keyIdx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(regexIdx) >= len(p.Regexps) || p.Regexps[regexIdx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
			if err != nil {
				return ir.Value{}, err
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				return ir.BoolValue(p.Regexps[regexIdx].MatchString(str)), nil
			}
			target, ok, err := stringLikeFromValue(attrVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				return ir.BoolValue(false), nil
			}
			return ir.BoolValue(p.Regexps[regexIdx].MatchString(target)), nil

		case ir.OpAttrIsNil:
			if int(arg) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[arg]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(attrVal.Type == ir.TypeNone), nil

		case ir.OpAttrIsNotNil:
			if int(arg) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[arg]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(attrVal.Type != ir.TypeNone), nil

		case ir.OpAttrFastIsNil:
			if int(arg) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[arg])
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(attrVal.Type == ir.TypeNone), nil

		case ir.OpAttrFastIsNotNil:
			if int(arg) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[arg])
			if err != nil {
				return ir.Value{}, err
			}
			return ir.BoolValue(attrVal.Type != ir.TypeNone), nil

		case ir.OpLoadConst:
			if int(arg) >= len(p.Consts) {
				return ir.Value{}, ErrInvalidConst
			}
			return p.Consts[arg], nil
		}
	} else if len(p.Code) == 2 {
		inst0 := p.Code[0]
		inst1 := p.Code[1]
		op0, arg0 := inst0.Op(), inst0.Arg()
		op1, arg1 := inst1.Op(), inst1.Arg()

		if op0 == ir.OpLoadAttrCached && op1 == ir.OpEqConst {
			if int(arg0) >= len(p.Accessors) || p.Accessors[arg0] == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(arg1) >= len(p.Consts) {
				return ir.Value{}, ErrInvalidConst
			}
			val, err := p.Accessors[arg0](ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			constVal := p.Consts[arg1]
			if val.Type == ir.TypeInt && constVal.Type == ir.TypeInt {
				return ir.BoolValue(int64(val.Num) == int64(constVal.Num)), nil
			}
			if val.Type == ir.TypeString && constVal.Type == ir.TypeString {
				return ir.BoolValue(stringsEqual(val, constVal)), nil
			}
			return compareOp(ir.OpEq, val, constVal)
		}
	}
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

		case ir.OpEqConst, ir.OpNeConst, ir.OpLtConst, ir.OpLteConst, ir.OpGtConst, ir.OpGteConst:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			idx := inst.Arg()
			if int(idx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			var cmpOp ir.Opcode
			switch op {
			case ir.OpEqConst:
				cmpOp = ir.OpEq
			case ir.OpNeConst:
				cmpOp = ir.OpNe
			case ir.OpLtConst:
				cmpOp = ir.OpLt
			case ir.OpLteConst:
				cmpOp = ir.OpLte
			case ir.OpGtConst:
				cmpOp = ir.OpGt
			case ir.OpGteConst:
				cmpOp = ir.OpGte
			default:
				return ir.Value{}, ErrInvalidOpcode
			}
			// Inline const compare to avoid compareOp dispatch overhead.
			constVal := consts[idx]
			if stack[sp-1].Type == ir.TypeNone || constVal.Type == ir.TypeNone {
				result, err := compareNil(cmpOp, stack[sp-1].Type == ir.TypeNone && constVal.Type == ir.TypeNone)
				if err != nil {
					return ir.Value{}, err
				}
				stack[sp-1] = result
				continue
			}
			var result ir.Value
			var err error
			switch constVal.Type {
			case ir.TypeInt:
				switch stack[sp-1].Type {
				case ir.TypeInt:
					result, err = compareInts(cmpOp, int64(stack[sp-1].Num), int64(constVal.Num))
				case ir.TypeFloat:
					result, err = compareFloats(cmpOp, math.Float64frombits(stack[sp-1].Num), float64(int64(constVal.Num)))
				default:
					return ir.Value{}, ErrTypeMismatch
				}
			case ir.TypeFloat:
				switch stack[sp-1].Type {
				case ir.TypeFloat:
					result, err = compareFloats(cmpOp, math.Float64frombits(stack[sp-1].Num), math.Float64frombits(constVal.Num))
				case ir.TypeInt:
					result, err = compareFloats(cmpOp, float64(int64(stack[sp-1].Num)), math.Float64frombits(constVal.Num))
				default:
					return ir.Value{}, ErrTypeMismatch
				}
			case ir.TypeBool:
				if stack[sp-1].Type != ir.TypeBool {
					return ir.Value{}, ErrTypeMismatch
				}
				result, err = compareBools(cmpOp, stack[sp-1].Num != 0, constVal.Num != 0)
			case ir.TypeString:
				if stack[sp-1].Type != ir.TypeString {
					return ir.Value{}, ErrTypeMismatch
				}
				if cmpOp == ir.OpEq {
					result = ir.BoolValue(stringsEqual(stack[sp-1], constVal))
				} else if cmpOp == ir.OpNe {
					result = ir.BoolValue(!stringsEqual(stack[sp-1], constVal))
				} else {
					result, err = compareStrings(cmpOp, stack[sp-1], constVal)
				}
			case ir.TypeBytes:
				if stack[sp-1].Type != ir.TypeBytes {
					return ir.Value{}, ErrTypeMismatch
				}
				result, err = compareBytes(cmpOp, stack[sp-1], constVal)
			default:
				return ir.Value{}, ErrTypeMismatch
			}
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = result

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

		case ir.OpEqString:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1] = ir.BoolValue(stringsEqual(stack[sp-1], stack[sp]))

		case ir.OpNeString:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1] = ir.BoolValue(!stringsEqual(stack[sp-1], stack[sp]))

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

		case ir.OpInt:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			converted, err := intFromValue(stack[sp-1])
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = converted

		case ir.OpIsNil:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1] = ir.BoolValue(stack[sp-1].Type == ir.TypeNone)

		case ir.OpIsType:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			typ := ir.Type(inst.Arg())
			if typ > ir.TypePSlice {
				return ir.Value{}, ErrInvalidType
			}
			stack[sp-1] = ir.BoolValue(stack[sp-1].Type == typ)

		case ir.OpIsMatch:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			idx := inst.Arg()
			if int(idx) >= len(p.Regexps) || p.Regexps[idx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			// Fast path for TypeString (most common case)
			val := stack[sp-1]
			if val.Type == ir.TypeString {
				str, ok := val.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp-1] = ir.BoolValue(p.Regexps[idx].MatchString(str))
				continue
			}
			// Slow path for other string-like types
			target, ok, err := stringLikeFromValue(val)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp-1] = ir.BoolValue(false)
				continue
			}
			stack[sp-1] = ir.BoolValue(p.Regexps[idx].MatchString(target))

		case ir.OpIsMatchDynamic:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			patternVal := stack[sp-1]
			if patternVal.Type != ir.TypeString {
				return ir.Value{}, ErrTypeMismatch
			}
			pattern, ok := patternVal.String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			r, err := dynamicRegexp(p, pattern)
			if err != nil {
				return ir.Value{}, fmt.Errorf("the regex pattern supplied to IsMatch '%q' is not a valid pattern: %w", pattern, err)
			}
			targetVal := stack[sp-2]
			if targetVal.Type == ir.TypeString {
				str, ok := targetVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp-2] = ir.BoolValue(r.MatchString(str))
				sp--
				continue
			}
			target, ok, err := stringLikeFromValue(targetVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp-2] = ir.BoolValue(false)
				sp--
				continue
			}
			stack[sp-2] = ir.BoolValue(r.MatchString(target))
			sp--

		case ir.OpLoadAttrCached:
			// OpLoadAttrCached requires context; use RunWithStackAndContext instead
			return ir.Value{}, ErrInvalidOpcode
		case ir.OpLoadAttrFast:
			// OpLoadAttrFast requires context; use RunWithStackAndContext instead
			return ir.Value{}, ErrInvalidOpcode
		case ir.OpSetAttrFast:
			// OpSetAttrFast requires context; use RunWithStackAndContext instead
			return ir.Value{}, ErrInvalidOpcode
		case ir.OpCall:
			// OpCall requires context; use RunWithStackAndContext instead
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
	if a.Type == ir.TypeNone || b.Type == ir.TypeNone {
		return compareNil(op, a.Type == ir.TypeNone && b.Type == ir.TypeNone)
	}
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
	case a.Type == ir.TypeBytes && b.Type == ir.TypeBytes:
		return compareBytes(op, a, b)
	case a.Type == ir.TypePMap && b.Type == ir.TypePMap:
		return comparePMaps(op, a, b)
	case a.Type == ir.TypePSlice && b.Type == ir.TypePSlice:
		return comparePSlices(op, a, b)
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func compareNil(op ir.Opcode, bothNil bool) (ir.Value, error) {
	switch op {
	case ir.OpEq:
		return ir.BoolValue(bothNil), nil
	case ir.OpNe:
		return ir.BoolValue(!bothNil), nil
	case ir.OpLt, ir.OpGt:
		return ir.BoolValue(false), nil
	case ir.OpLte, ir.OpGte:
		return ir.BoolValue(bothNil), nil
	default:
		return ir.Value{}, ErrInvalidOpcode
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

func stringsEqual(a, b ir.Value) bool {
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

func compareBytes(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	ba, ok := a.Bytes()
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	bb, ok := b.Bytes()
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	if ba == nil || bb == nil {
		if op == ir.OpNe {
			return ir.BoolValue(true), nil
		}
		return ir.BoolValue(false), nil
	}
	cmp := bytes.Compare(ba, bb)
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

func comparePMaps(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	am, ok := pMapFromValue(a)
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	bm, ok := pMapFromValue(b)
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	switch op {
	case ir.OpEq:
		return ir.BoolValue(am.Equal(bm)), nil
	case ir.OpNe:
		return ir.BoolValue(!am.Equal(bm)), nil
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func comparePSlices(op ir.Opcode, a, b ir.Value) (ir.Value, error) {
	as, ok := pSliceFromValue(a)
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	bs, ok := pSliceFromValue(b)
	if !ok {
		return ir.Value{}, ErrTypeMismatch
	}
	switch op {
	case ir.OpEq:
		return ir.BoolValue(as.Equal(bs)), nil
	case ir.OpNe:
		return ir.BoolValue(!as.Equal(bs)), nil
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func intFromValue(val ir.Value) (ir.Value, error) {
	switch val.Type {
	case ir.TypeNone:
		return ir.Value{Type: ir.TypeNone}, nil
	case ir.TypeInt:
		return val, nil
	case ir.TypeFloat:
		return ir.Int64Value(int64(math.Float64frombits(val.Num))), nil
	case ir.TypeBool:
		return ir.Int64Value(boolToInt(val.Num != 0)), nil
	case ir.TypeString:
		str, ok := val.String()
		if !ok {
			return ir.Value{}, ErrTypeMismatch
		}
		parsed, err := parseIntString(str)
		if err != nil {
			return ir.Value{Type: ir.TypeNone}, nil
		}
		return ir.Int64Value(parsed), nil
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func boolToInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func parseIntString(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func stringLikeFromValue(val ir.Value) (string, bool, error) {
	switch val.Type {
	case ir.TypeNone:
		return "", false, nil
	case ir.TypeString:
		str, ok := val.String()
		if !ok {
			return "", false, ErrTypeMismatch
		}
		return str, true, nil
	case ir.TypeBytes:
		raw, ok := val.Bytes()
		if !ok {
			return "", false, ErrTypeMismatch
		}
		return hex.EncodeToString(raw), true, nil
	case ir.TypeInt:
		return strconv.FormatInt(int64(val.Num), 10), true, nil
	case ir.TypeFloat:
		f := math.Float64frombits(val.Num)
		b, err := json.Marshal(f)
		if err != nil {
			return "", false, err
		}
		return string(b), true, nil
	case ir.TypeBool:
		return strconv.FormatBool(val.Num != 0), true, nil
	default:
		return "", false, ErrTypeMismatch
	}
}

func stringFromValue(val ir.Value) (string, error) {
	if val.Type != ir.TypeString {
		return "", ErrTypeMismatch
	}
	str, ok := val.String()
	if !ok {
		return "", ErrTypeMismatch
	}
	return str, nil
}

func IsMatchValue(val ir.Value, regex *regexp.Regexp) (bool, error) {
	target, ok, err := stringLikeFromValue(val)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return regex.MatchString(target), nil
}

func pcommonValueToVM(val pcommon.Value) (ir.Value, error) {
	switch val.Type() {
	case pcommon.ValueTypeInt:
		return ir.Int64Value(val.Int()), nil
	case pcommon.ValueTypeDouble:
		return ir.Float64Value(val.Double()), nil
	case pcommon.ValueTypeBool:
		return ir.BoolValue(val.Bool()), nil
	case pcommon.ValueTypeStr:
		return ir.StringValue(val.Str()), nil
	case pcommon.ValueTypeBytes:
		return ir.BytesValue(val.Bytes().AsRaw()), nil
	case pcommon.ValueTypeMap:
		return pMapValue(val.Map()), nil
	case pcommon.ValueTypeSlice:
		return pSliceValue(val.Slice()), nil
	default:
		return ir.Value{}, ErrTypeMismatch
	}
}

func pMapValue(val pcommon.Map) ir.Value {
	w := *(*pcommonWrapper)(unsafe.Pointer(&val))
	return ir.Value{Type: ir.TypePMap, Num: uint64(uintptr(w.state)), Ptr: w.orig}
}

func pSliceValue(val pcommon.Slice) ir.Value {
	w := *(*pcommonWrapper)(unsafe.Pointer(&val))
	return ir.Value{Type: ir.TypePSlice, Num: uint64(uintptr(w.state)), Ptr: w.orig}
}

func pMapFromValue(val ir.Value) (pcommon.Map, bool) {
	if val.Type != ir.TypePMap || val.Ptr == nil || val.Num == 0 {
		return pcommon.Map{}, false
	}
	w := pcommonWrapper{orig: val.Ptr, state: unsafe.Pointer(uintptr(val.Num))}
	return *(*pcommon.Map)(unsafe.Pointer(&w)), true
}

func pSliceFromValue(val ir.Value) (pcommon.Slice, bool) {
	if val.Type != ir.TypePSlice || val.Ptr == nil || val.Num == 0 {
		return pcommon.Slice{}, false
	}
	w := pcommonWrapper{orig: val.Ptr, state: unsafe.Pointer(uintptr(val.Num))}
	return *(*pcommon.Slice)(unsafe.Pointer(&w)), true
}

type pcommonWrapper struct {
	orig  unsafe.Pointer
	state unsafe.Pointer
}

func dynamicRegexp[K any](p *Program[K], pattern string) (*regexp.Regexp, error) {
	if p == nil {
		return regexp.Compile(pattern)
	}
	if cached, ok := p.dynamicRegexps.Load(pattern); ok {
		return cached.(*regexp.Regexp), nil
	}
	r, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	actual, _ := p.dynamicRegexps.LoadOrStore(pattern, r)
	return actual.(*regexp.Regexp), nil
}

func setLogBodyFromVM(lr plog.LogRecord, val ir.Value) error {
	body := lr.Body()
	switch val.Type {
	case ir.TypeInt:
		body.SetInt(int64(val.Num))
		return nil
	case ir.TypeFloat:
		body.SetDouble(math.Float64frombits(val.Num))
		return nil
	case ir.TypeBool:
		body.SetBool(val.Num != 0)
		return nil
	case ir.TypeString:
		s, ok := val.String()
		if !ok {
			return ErrTypeMismatch
		}
		body.SetStr(s)
		return nil
	case ir.TypeBytes:
		b, ok := val.Bytes()
		if !ok {
			return ErrTypeMismatch
		}
		body.SetEmptyBytes().FromRaw(b)
		return nil
	case ir.TypePMap:
		m, ok := pMapFromValue(val)
		if !ok {
			return ErrTypeMismatch
		}
		m.CopyTo(body.SetEmptyMap())
		return nil
	case ir.TypePSlice:
		s, ok := pSliceFromValue(val)
		if !ok {
			return ErrTypeMismatch
		}
		s.CopyTo(body.SetEmptySlice())
		return nil
	default:
		return ErrTypeMismatch
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
	logGetter := p.LogRecordGetter
	spanGetter := p.SpanGetter
	metricGetter := p.MetricGetter
	resourceGetter := p.ResourceGetter
	resourceSchemaURLGetter := p.ResourceSchemaURLGetter
	resourceSchemaURLSetter := p.ResourceSchemaURLSetter
	scopeGetter := p.ScopeGetter
	scopeSchemaURLGetter := p.ScopeSchemaURLGetter
	scopeSchemaURLSetter := p.ScopeSchemaURLSetter

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

		case ir.OpEqConst, ir.OpNeConst, ir.OpLtConst, ir.OpLteConst, ir.OpGtConst, ir.OpGteConst:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			idx := inst.Arg()
			if int(idx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			var cmpOp ir.Opcode
			switch op {
			case ir.OpEqConst:
				cmpOp = ir.OpEq
			case ir.OpNeConst:
				cmpOp = ir.OpNe
			case ir.OpLtConst:
				cmpOp = ir.OpLt
			case ir.OpLteConst:
				cmpOp = ir.OpLte
			case ir.OpGtConst:
				cmpOp = ir.OpGt
			case ir.OpGteConst:
				cmpOp = ir.OpGte
			default:
				return ir.Value{}, ErrInvalidOpcode
			}
			// Inline const compare to avoid compareOp dispatch overhead.
			constVal := consts[idx]
			if stack[sp-1].Type == ir.TypeNone || constVal.Type == ir.TypeNone {
				result, err := compareNil(cmpOp, stack[sp-1].Type == ir.TypeNone && constVal.Type == ir.TypeNone)
				if err != nil {
					return ir.Value{}, err
				}
				stack[sp-1] = result
				continue
			}
			var result ir.Value
			var err error
			switch constVal.Type {
			case ir.TypeInt:
				switch stack[sp-1].Type {
				case ir.TypeInt:
					result, err = compareInts(cmpOp, int64(stack[sp-1].Num), int64(constVal.Num))
				case ir.TypeFloat:
					result, err = compareFloats(cmpOp, math.Float64frombits(stack[sp-1].Num), float64(int64(constVal.Num)))
				default:
					return ir.Value{}, ErrTypeMismatch
				}
			case ir.TypeFloat:
				switch stack[sp-1].Type {
				case ir.TypeFloat:
					result, err = compareFloats(cmpOp, math.Float64frombits(stack[sp-1].Num), math.Float64frombits(constVal.Num))
				case ir.TypeInt:
					result, err = compareFloats(cmpOp, float64(int64(stack[sp-1].Num)), math.Float64frombits(constVal.Num))
				default:
					return ir.Value{}, ErrTypeMismatch
				}
			case ir.TypeBool:
				if stack[sp-1].Type != ir.TypeBool {
					return ir.Value{}, ErrTypeMismatch
				}
				result, err = compareBools(cmpOp, stack[sp-1].Num != 0, constVal.Num != 0)
			case ir.TypeString:
				if stack[sp-1].Type != ir.TypeString {
					return ir.Value{}, ErrTypeMismatch
				}
				if cmpOp == ir.OpEq {
					result = ir.BoolValue(stringsEqual(stack[sp-1], constVal))
				} else if cmpOp == ir.OpNe {
					result = ir.BoolValue(!stringsEqual(stack[sp-1], constVal))
				} else {
					result, err = compareStrings(cmpOp, stack[sp-1], constVal)
				}
			case ir.TypeBytes:
				if stack[sp-1].Type != ir.TypeBytes {
					return ir.Value{}, ErrTypeMismatch
				}
				result, err = compareBytes(cmpOp, stack[sp-1], constVal)
			default:
				return ir.Value{}, ErrTypeMismatch
			}
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = result

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

		case ir.OpLoadAttrFast:
			idx := inst.Arg()
			if int(idx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			vmVal, err := p.AttrGetter(tCtx, p.AttrKeys[idx])
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = vmVal
			sp++

		case ir.OpSetAttrFast:
			idx := inst.Arg()
			if int(idx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrSetter == nil {
				return ir.Value{}, ErrInvalidSetter
			}
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			val := stack[sp]
			if err := p.AttrSetter(tCtx, p.AttrKeys[idx], val); err != nil {
				return ir.Value{}, err
			}

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

		case ir.OpEqString:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1] = ir.BoolValue(stringsEqual(stack[sp-1], stack[sp]))

		case ir.OpNeString:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			stack[sp-1] = ir.BoolValue(!stringsEqual(stack[sp-1], stack[sp]))

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

		case ir.OpInt:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			converted, err := intFromValue(stack[sp-1])
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-1] = converted

		case ir.OpIsNil:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			stack[sp-1] = ir.BoolValue(stack[sp-1].Type == ir.TypeNone)

		case ir.OpIsType:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			typ := ir.Type(inst.Arg())
			if typ > ir.TypePSlice {
				return ir.Value{}, ErrInvalidType
			}
			stack[sp-1] = ir.BoolValue(stack[sp-1].Type == typ)

		case ir.OpIsMatch:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			idx := inst.Arg()
			if int(idx) >= len(p.Regexps) || p.Regexps[idx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			// Fast path for TypeString (most common case)
			val := stack[sp-1]
			if val.Type == ir.TypeString {
				str, ok := val.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp-1] = ir.BoolValue(p.Regexps[idx].MatchString(str))
				continue
			}
			// Slow path for other string-like types
			target, ok, err := stringLikeFromValue(val)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp-1] = ir.BoolValue(false)
				continue
			}
			stack[sp-1] = ir.BoolValue(p.Regexps[idx].MatchString(target))

		case ir.OpIsMatchDynamic:
			if sp < 2 {
				return ir.Value{}, ErrStackUnderflow
			}
			patternVal := stack[sp-1]
			if patternVal.Type != ir.TypeString {
				return ir.Value{}, ErrTypeMismatch
			}
			pattern, ok := patternVal.String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			r, err := dynamicRegexp(p, pattern)
			if err != nil {
				return ir.Value{}, fmt.Errorf("the regex pattern supplied to IsMatch '%q' is not a valid pattern: %w", pattern, err)
			}
			targetVal := stack[sp-2]
			if targetVal.Type == ir.TypeString {
				str, ok := targetVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp-2] = ir.BoolValue(r.MatchString(str))
				sp--
				continue
			}
			target, ok, err := stringLikeFromValue(targetVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp-2] = ir.BoolValue(false)
				sp--
				continue
			}
			stack[sp-2] = ir.BoolValue(r.MatchString(target))
			sp--

		case ir.OpCall:
			idx := inst.Arg()
			if int(idx) >= len(p.CallSites) {
				return ir.Value{}, ErrInvalidCallSite
			}
			callsite := p.CallSites[idx]
			if callsite.Func == nil || callsite.ArgCount < 0 {
				return ir.Value{}, ErrInvalidCallSite
			}
			if sp < callsite.ArgCount {
				return ir.Value{}, ErrStackUnderflow
			}
			args := stack[sp-callsite.ArgCount : sp]
			result, err := callsite.Func(ctx, tCtx, args)
			if err != nil {
				return ir.Value{}, err
			}
			stack[sp-callsite.ArgCount] = result
			sp = sp - callsite.ArgCount + 1

		case ir.OpGetBody:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			val, err := pcommonValueToVM(lr.Body())
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = val
			sp++

		case ir.OpSetBody:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if err := setLogBodyFromVM(lr, stack[sp]); err != nil {
				return ir.Value{}, err
			}

		case ir.OpGetSeverity:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(lr.SeverityNumber()))
			sp++

		case ir.OpSetSeverity:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			lr.SetSeverityNumber(plog.SeverityNumber(int64(stack[sp].Num)))

		case ir.OpGetTimestamp:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			ts := lr.Timestamp().AsTime().UnixNano()
			stack[sp] = ir.Int64Value(ts)
			sp++

		case ir.OpSetTimestamp:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(stack[sp].Num))))

		case ir.OpGetSpanName:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(span.Name())
			sp++

		case ir.OpSetSpanName:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			name, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetName(name)

		case ir.OpGetSpanStartTime:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			ts := span.StartTimestamp().AsTime().UnixNano()
			stack[sp] = ir.Int64Value(ts)
			sp++

		case ir.OpSetSpanStartTime:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(stack[sp].Num))))

		case ir.OpGetSpanEndTime:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			ts := span.EndTimestamp().AsTime().UnixNano()
			stack[sp] = ir.Int64Value(ts)
			sp++

		case ir.OpSetSpanEndTime:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(stack[sp].Num))))

		case ir.OpGetSpanKind:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(span.Kind()))
			sp++

		case ir.OpSetSpanKind:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetKind(ptrace.SpanKind(int64(stack[sp].Num)))

		case ir.OpGetSpanStatus:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(span.Status().Code()))
			sp++

		case ir.OpSetSpanStatus:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.Status().SetCode(ptrace.StatusCode(int64(stack[sp].Num)))

		case ir.OpGetMetricName:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(metric.Name())
			sp++

		case ir.OpSetMetricName:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			name, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			metric.SetName(name)

		case ir.OpGetMetricUnit:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(metric.Unit())
			sp++

		case ir.OpSetMetricUnit:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			unit, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			metric.SetUnit(unit)

		case ir.OpGetMetricType:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(metric.Type()))
			sp++

		case ir.OpGetSpanStatusMsg:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(span.Status().Message())
			sp++

		case ir.OpSetSpanStatusMsg:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			msg, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.Status().SetMessage(msg)

		case ir.OpGetResourceDroppedAttributesCount:
			var res pcommon.Resource
			if resourceGetter != nil {
				res = resourceGetter(tCtx)
			} else {
				resourceCtx, ok := any(tCtx).(ResourceContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				res = resourceCtx.GetResource()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(res.DroppedAttributesCount()))
			sp++

		case ir.OpSetResourceDroppedAttributesCount:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var res pcommon.Resource
			if resourceGetter != nil {
				res = resourceGetter(tCtx)
			} else {
				resourceCtx, ok := any(tCtx).(ResourceContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				res = resourceCtx.GetResource()
			}
			res.SetDroppedAttributesCount(uint32(stack[sp].Num))

		case ir.OpGetResourceSchemaURL:
			if resourceSchemaURLGetter == nil {
				return ir.Value{}, ErrTypeMismatch
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(resourceSchemaURLGetter(tCtx))
			sp++

		case ir.OpSetResourceSchemaURL:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			schemaURL, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			if resourceSchemaURLSetter == nil {
				return ir.Value{}, ErrTypeMismatch
			}
			resourceSchemaURLSetter(tCtx, schemaURL)

		case ir.OpGetScopeName:
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(scope.Name())
			sp++

		case ir.OpSetScopeName:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			name, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			scope.SetName(name)

		case ir.OpGetScopeVersion:
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(scope.Version())
			sp++

		case ir.OpSetScopeVersion:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			version, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			scope.SetVersion(version)

		case ir.OpGetScopeDroppedAttributesCount:
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(scope.DroppedAttributesCount()))
			sp++

		case ir.OpSetScopeDroppedAttributesCount:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var scope pcommon.InstrumentationScope
			if scopeGetter != nil {
				scope = scopeGetter(tCtx)
			} else {
				scopeCtx, ok := any(tCtx).(ScopeContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				scope = scopeCtx.GetInstrumentationScope()
			}
			scope.SetDroppedAttributesCount(uint32(stack[sp].Num))

		case ir.OpGetScopeSchemaURL:
			if scopeSchemaURLGetter == nil {
				return ir.Value{}, ErrTypeMismatch
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(scopeSchemaURLGetter(tCtx))
			sp++

		case ir.OpSetScopeSchemaURL:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			schemaURL, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			if scopeSchemaURLSetter == nil {
				return ir.Value{}, ErrTypeMismatch
			}
			scopeSchemaURLSetter(tCtx, schemaURL)

		case ir.OpGetObservedTimestamp:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			ts := lr.ObservedTimestamp().AsTime().UnixNano()
			stack[sp] = ir.Int64Value(ts)
			sp++

		case ir.OpSetObservedTimestamp:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(stack[sp].Num))))

		case ir.OpGetSeverityText:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(lr.SeverityText())
			sp++

		case ir.OpSetSeverityText:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			text, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			lr.SetSeverityText(text)

		case ir.OpGetLogFlags:
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(lr.Flags()))
			sp++

		case ir.OpSetLogFlags:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var lr plog.LogRecord
			if logGetter != nil {
				lr = logGetter(tCtx)
			} else {
				logCtx, ok := any(tCtx).(LogRecordContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				lr = logCtx.GetLogRecord()
			}
			lr.SetFlags(plog.LogRecordFlags(int64(stack[sp].Num)))

		case ir.OpGetSpanTraceID:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			traceID := span.TraceID()
			buf := make([]byte, len(traceID))
			copy(buf, traceID[:])
			stack[sp] = ir.BytesValue(buf)
			sp++

		case ir.OpGetSpanID:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			spanID := span.SpanID()
			buf := make([]byte, len(spanID))
			copy(buf, spanID[:])
			stack[sp] = ir.BytesValue(buf)
			sp++

		case ir.OpGetSpanParentID:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			parentID := span.ParentSpanID()
			buf := make([]byte, len(parentID))
			copy(buf, parentID[:])
			stack[sp] = ir.BytesValue(buf)
			sp++

		case ir.OpGetSpanTraceIDString:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(traceutil.TraceIDToHexOrEmptyString(span.TraceID()))
			sp++

		case ir.OpGetSpanIDString:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(traceutil.SpanIDToHexOrEmptyString(span.SpanID()))
			sp++

		case ir.OpGetSpanParentIDString:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()))
			sp++

		case ir.OpGetSpanTraceState:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(span.TraceState().AsRaw())
			sp++

		case ir.OpSetSpanTraceState:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			raw, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.TraceState().FromRaw(raw)

		case ir.OpGetSpanTraceStateKey:
			idx := inst.Arg()
			if int(idx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			raw := span.TraceState().AsRaw()
			ts, err := trace.ParseTraceState(raw)
			if err != nil {
				if sp >= len(stack) {
					return ir.Value{}, ErrStackOverflow
				}
				stack[sp] = ir.Value{Type: ir.TypeNone}
				sp++
				break
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(ts.Get(p.AttrKeys[idx]))
			sp++

		case ir.OpSetSpanTraceStateKey:
			idx := inst.Arg()
			if int(idx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			value, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			raw := span.TraceState().AsRaw()
			ts, err := trace.ParseTraceState(raw)
			if err != nil {
				break
			}
			if updated, err := ts.Insert(p.AttrKeys[idx], value); err == nil {
				span.TraceState().FromRaw(updated.String())
			}

		case ir.OpGetSpanDroppedAttributesCount:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(span.DroppedAttributesCount()))
			sp++

		case ir.OpSetSpanDroppedAttributesCount:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetDroppedAttributesCount(uint32(stack[sp].Num))

		case ir.OpGetSpanDroppedEventsCount:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(span.DroppedEventsCount()))
			sp++

		case ir.OpSetSpanDroppedEventsCount:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetDroppedEventsCount(uint32(stack[sp].Num))

		case ir.OpGetSpanDroppedLinksCount:
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.Int64Value(int64(span.DroppedLinksCount()))
			sp++

		case ir.OpSetSpanDroppedLinksCount:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var span ptrace.Span
			if spanGetter != nil {
				span = spanGetter(tCtx)
			} else {
				spanCtx, ok := any(tCtx).(SpanContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				span = spanCtx.GetSpan()
			}
			span.SetDroppedLinksCount(uint32(stack[sp].Num))

		case ir.OpGetMetricDescription:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.StringValue(metric.Description())
			sp++

		case ir.OpSetMetricDescription:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			desc, ok := stack[sp].String()
			if !ok {
				return ir.Value{}, ErrTypeMismatch
			}
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			metric.SetDescription(desc)

		case ir.OpGetMetricAggTemporality:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			switch metric.Type() {
			case pmetric.MetricTypeSum:
				stack[sp] = ir.Int64Value(int64(metric.Sum().AggregationTemporality()))
			case pmetric.MetricTypeHistogram:
				stack[sp] = ir.Int64Value(int64(metric.Histogram().AggregationTemporality()))
			case pmetric.MetricTypeExponentialHistogram:
				stack[sp] = ir.Int64Value(int64(metric.ExponentialHistogram().AggregationTemporality()))
			default:
				stack[sp] = ir.Value{Type: ir.TypeNone}
			}
			sp++

		case ir.OpSetMetricAggTemporality:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeInt {
				return ir.Value{}, ErrTypeMismatch
			}
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			agg := pmetric.AggregationTemporality(int64(stack[sp].Num))
			switch metric.Type() {
			case pmetric.MetricTypeSum:
				metric.Sum().SetAggregationTemporality(agg)
			case pmetric.MetricTypeHistogram:
				metric.Histogram().SetAggregationTemporality(agg)
			case pmetric.MetricTypeExponentialHistogram:
				metric.ExponentialHistogram().SetAggregationTemporality(agg)
			}

		case ir.OpGetMetricIsMonotonic:
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			if metric.Type() == pmetric.MetricTypeSum {
				stack[sp] = ir.BoolValue(metric.Sum().IsMonotonic())
			} else {
				stack[sp] = ir.Value{Type: ir.TypeNone}
			}
			sp++

		case ir.OpSetMetricIsMonotonic:
			if sp < 1 {
				return ir.Value{}, ErrStackUnderflow
			}
			sp--
			if stack[sp].Type != ir.TypeBool {
				return ir.Value{}, ErrTypeMismatch
			}
			var metric pmetric.Metric
			if metricGetter != nil {
				metric = metricGetter(tCtx)
			} else {
				metricCtx, ok := any(tCtx).(MetricContext)
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				metric = metricCtx.GetMetric()
			}
			if metric.Type() == pmetric.MetricTypeSum {
				metric.Sum().SetIsMonotonic(stack[sp].Num != 0)
			}

		case ir.OpAttrEqConstString:
			attrIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(constIdx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			constVal := consts[constIdx]
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(stringsEqual(attrVal, constVal))
			sp++

	case ir.OpAttrNeConstString:
		attrIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
		if int(attrIdx) >= len(p.Accessors) {
			return ir.Value{}, ErrInvalidAccessor
		}
			if int(constIdx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			constVal := consts[constIdx]
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
		stack[sp] = ir.BoolValue(!stringsEqual(attrVal, constVal))
		sp++

	case ir.OpAttrEqConst:
		attrIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
		if int(attrIdx) >= len(p.Accessors) {
			return ir.Value{}, ErrInvalidAccessor
		}
		if int(constIdx) >= len(consts) {
			return ir.Value{}, ErrInvalidConst
		}
		accessor := p.Accessors[attrIdx]
		if accessor == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := accessor(ctx, tCtx)
		if err != nil {
			return ir.Value{}, err
		}
		constVal := consts[constIdx]
		if sp >= len(stack) {
			return ir.Value{}, ErrStackOverflow
		}
		stack[sp] = ir.BoolValue(attrVal.Type == constVal.Type && attrVal.Num == constVal.Num)
		sp++

	case ir.OpAttrNeConst:
		attrIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
		if int(attrIdx) >= len(p.Accessors) {
			return ir.Value{}, ErrInvalidAccessor
		}
		if int(constIdx) >= len(consts) {
			return ir.Value{}, ErrInvalidConst
		}
		accessor := p.Accessors[attrIdx]
		if accessor == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := accessor(ctx, tCtx)
		if err != nil {
			return ir.Value{}, err
		}
		constVal := consts[constIdx]
		if sp >= len(stack) {
			return ir.Value{}, ErrStackOverflow
		}
		stack[sp] = ir.BoolValue(!(attrVal.Type == constVal.Type && attrVal.Num == constVal.Num))
		sp++

	case ir.OpAttrFastEqConstString:
		keyIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
		if int(keyIdx) >= len(p.AttrKeys) {
			return ir.Value{}, ErrInvalidAccessor
		}
		if int(constIdx) >= len(consts) {
			return ir.Value{}, ErrInvalidConst
		}
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		constVal := consts[constIdx]
		if sp >= len(stack) {
			return ir.Value{}, ErrStackOverflow
		}
		stack[sp] = ir.BoolValue(stringsEqual(attrVal, constVal))
		sp++

	case ir.OpAttrFastEqConst:
		keyIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
		if int(keyIdx) >= len(p.AttrKeys) {
			return ir.Value{}, ErrInvalidAccessor
		}
		if int(constIdx) >= len(consts) {
			return ir.Value{}, ErrInvalidConst
		}
		if p.AttrGetter == nil {
			return ir.Value{}, ErrInvalidAccessor
		}
		attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
		if err != nil {
			return ir.Value{}, err
		}
		constVal := consts[constIdx]
		if sp >= len(stack) {
			return ir.Value{}, ErrStackOverflow
		}
		stack[sp] = ir.BoolValue(attrVal.Type == constVal.Type && attrVal.Num == constVal.Num)
		sp++

		case ir.OpAttrFastNeConstString:
			keyIdx, constIdx := ir.UnpackAttrConst(inst.Arg())
			if int(keyIdx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(constIdx) >= len(consts) {
				return ir.Value{}, ErrInvalidConst
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
			if err != nil {
				return ir.Value{}, err
			}
			constVal := consts[constIdx]
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(!stringsEqual(attrVal, constVal))
			sp++

		case ir.OpAttrIsMatchConst:
			attrIdx, regexIdx := ir.UnpackAttrConst(inst.Arg())
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(regexIdx) >= len(p.Regexps) || p.Regexps[regexIdx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp] = ir.BoolValue(p.Regexps[regexIdx].MatchString(str))
				sp++
				continue
			}
			target, ok, err := stringLikeFromValue(attrVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp] = ir.BoolValue(false)
				sp++
				continue
			}
			stack[sp] = ir.BoolValue(p.Regexps[regexIdx].MatchString(target))
			sp++

		case ir.OpAttrFastIsMatchConst:
			keyIdx, regexIdx := ir.UnpackAttrConst(inst.Arg())
			if int(keyIdx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if int(regexIdx) >= len(p.Regexps) || p.Regexps[regexIdx] == nil {
				return ir.Value{}, ErrInvalidRegex
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			if attrVal.Type == ir.TypeString {
				str, ok := attrVal.String()
				if !ok {
					return ir.Value{}, ErrTypeMismatch
				}
				stack[sp] = ir.BoolValue(p.Regexps[regexIdx].MatchString(str))
				sp++
				continue
			}
			target, ok, err := stringLikeFromValue(attrVal)
			if err != nil {
				return ir.Value{}, err
			}
			if !ok {
				stack[sp] = ir.BoolValue(false)
				sp++
				continue
			}
			stack[sp] = ir.BoolValue(p.Regexps[regexIdx].MatchString(target))
			sp++

		case ir.OpAttrIsNil:
			attrIdx := inst.Arg()
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(attrVal.Type == ir.TypeNone)
			sp++

		case ir.OpAttrIsNotNil:
			attrIdx := inst.Arg()
			if int(attrIdx) >= len(p.Accessors) {
				return ir.Value{}, ErrInvalidAccessor
			}
			accessor := p.Accessors[attrIdx]
			if accessor == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := accessor(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(attrVal.Type != ir.TypeNone)
			sp++

		case ir.OpAttrFastIsNil:
			keyIdx := inst.Arg()
			if int(keyIdx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(attrVal.Type == ir.TypeNone)
			sp++

		case ir.OpAttrFastIsNotNil:
			keyIdx := inst.Arg()
			if int(keyIdx) >= len(p.AttrKeys) {
				return ir.Value{}, ErrInvalidAccessor
			}
			if p.AttrGetter == nil {
				return ir.Value{}, ErrInvalidAccessor
			}
			attrVal, err := p.AttrGetter(tCtx, p.AttrKeys[keyIdx])
			if err != nil {
				return ir.Value{}, err
			}
			if sp >= len(stack) {
				return ir.Value{}, ErrStackOverflow
			}
			stack[sp] = ir.BoolValue(attrVal.Type != ir.TypeNone)
			sp++

		default:
			return ir.Value{}, ErrInvalidOpcode
		}
	}
	if sp == 0 {
		return ir.Value{}, ErrEmptyStack
	}
	return stack[sp-1], nil
}
