// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"unsafe"

	"github.com/iancoleman/strcase"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type constKey struct {
	typ ir.Type
	num uint64
	str string
}

type valueKind int

const (
	kindUnknown valueKind = iota
	kindInt
	kindFloat
	kindBool
	kindString
	kindBytes
)

const maxInstructionArg = 0x00FFFFFF

type microProgram[K any] struct {
	program      *vm.Program[K]
	getters      []Getter[K]
	vmGetters    []VMGetter[K]
	stack        int
	needsContext bool
	stackPool    *vm.StackPool
}

type microCompiler[K any] struct {
	parser        *Parser[K]
	code          []ir.Instruction
	consts        []ir.Value
	constIndex    map[constKey]uint32
	regexps       []*regexp.Regexp
	regexIndex    map[string]uint32
	callSites     []vm.CallSite[K]
	callSiteIndex map[string]uint32
	getterIndex   map[string]uint32
	getters       []Getter[K]
	vmGetters     []VMGetter[K]
	attrKeys      []string
	attrKeyIndex  map[string]uint32
	attrGetter    vm.AttrGetter[K]
	attrSetter    vm.AttrSetter[K]
	stackDepth    int
	maxStack      int
	needsContext  bool
}

func vmGasLimitFor[K any](p *Parser[K]) uint64 {
	if p != nil && p.vmGasLimit > 0 {
		return p.vmGasLimit
	}
	return vm.DefaultGasLimit
}

func compileMicroMathExpression(expr *mathExpression) (*vm.ProgramAny, error) {
	if _, err := inferMathExpressionKind(expr); err != nil {
		return nil, err
	}
	c := &microCompiler[any]{constIndex: map[constKey]uint32{}}
	if err := c.emitMathExpression(expr); err != nil {
		return nil, err
	}
	if len(c.getters) > 0 {
		return nil, fmt.Errorf("math expression contains paths, use parser.compileMicroMathExpression")
	}
	return &vm.ProgramAny{Code: c.code, Consts: c.consts, GasLimit: vm.DefaultGasLimit}, nil
}

func (p *Parser[K]) compileMicroComparisonVM(cmp *comparison) (*microProgram[K], error) {
	if cmp == nil {
		return nil, fmt.Errorf("comparison is nil")
	}

	leftKind, err := inferValueKind(cmp.Left)
	if err != nil {
		return nil, err
	}
	rightKind, err := inferValueKind(cmp.Right)
	if err != nil {
		return nil, err
	}
	if !supportsComparison(cmp.Op, leftKind, rightKind) {
		return nil, fmt.Errorf("unsupported comparison op/type combination")
	}

	c := &microCompiler[K]{
		parser:        p,
		constIndex:    map[constKey]uint32{},
		regexIndex:    map[string]uint32{},
		callSiteIndex: map[string]uint32{},
		getterIndex:   map[string]uint32{},
		attrKeyIndex:  map[string]uint32{},
		attrGetter:    p.vmAttrGetter,
		attrSetter:    p.vmAttrSetter,
	}
	if err := c.emitComparison(cmp); err != nil {
		return nil, err
	}
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program[K]{
			Code:                    c.code,
			Consts:                  c.consts,
			Regexps:                 c.regexps,
			CallSites:               c.callSites,
			Accessors:               c.buildAccessors(),
			AttrKeys:                c.attrKeys,
			AttrGetter:              c.attrGetter,
			AttrSetter:              c.attrSetter,
			LogRecordGetter:         p.vmLogRecordGetter,
			SpanGetter:              p.vmSpanGetter,
			MetricGetter:            p.vmMetricGetter,
			ResourceGetter:          p.vmResourceGetter,
			ResourceSchemaURLGetter: p.vmResourceSchemaURLGetter,
			ResourceSchemaURLSetter: p.vmResourceSchemaURLSetter,
			ScopeGetter:             p.vmScopeGetter,
			ScopeSchemaURLGetter:    p.vmScopeSchemaURLGetter,
			ScopeSchemaURLSetter:    p.vmScopeSchemaURLSetter,
			GasLimit:                vmGasLimitFor(p),
		},
		getters:      c.getters,
		vmGetters:    c.vmGetters,
		stack:        c.maxStack,
		needsContext: c.needsContext,
		stackPool:    p.vmStackPool,
	}, nil
}

func (p *Parser[K]) compileMicroBoolExpression(expr *booleanExpression) (*microProgram[K], error) {
	if expr == nil {
		return nil, fmt.Errorf("boolean expression is nil")
	}
	c := &microCompiler[K]{
		parser:        p,
		constIndex:    map[constKey]uint32{},
		regexIndex:    map[string]uint32{},
		callSiteIndex: map[string]uint32{},
		getterIndex:   map[string]uint32{},
		attrKeyIndex:  map[string]uint32{},
		attrGetter:    p.vmAttrGetter,
		attrSetter:    p.vmAttrSetter,
	}
	if err := c.emitBooleanExpression(expr); err != nil {
		return nil, err
	}
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program[K]{
			Code:                    c.code,
			Consts:                  c.consts,
			Regexps:                 c.regexps,
			CallSites:               c.callSites,
			Accessors:               c.buildAccessors(),
			AttrKeys:                c.attrKeys,
			AttrGetter:              c.attrGetter,
			AttrSetter:              c.attrSetter,
			LogRecordGetter:         p.vmLogRecordGetter,
			SpanGetter:              p.vmSpanGetter,
			MetricGetter:            p.vmMetricGetter,
			ResourceGetter:          p.vmResourceGetter,
			ResourceSchemaURLGetter: p.vmResourceSchemaURLGetter,
			ResourceSchemaURLSetter: p.vmResourceSchemaURLSetter,
			ScopeGetter:             p.vmScopeGetter,
			ScopeSchemaURLGetter:    p.vmScopeSchemaURLGetter,
			ScopeSchemaURLSetter:    p.vmScopeSchemaURLSetter,
			GasLimit:                vmGasLimitFor(p),
		},
		getters:      c.getters,
		vmGetters:    c.vmGetters,
		stack:        c.maxStack,
		needsContext: c.needsContext,
		stackPool:    p.vmStackPool,
	}, nil
}

func compareOpcode(op compareOp) (ir.Opcode, error) {
	switch op {
	case eq:
		return ir.OpEq, nil
	case ne:
		return ir.OpNe, nil
	case lt:
		return ir.OpLt, nil
	case lte:
		return ir.OpLte, nil
	case gt:
		return ir.OpGt, nil
	case gte:
		return ir.OpGte, nil
	default:
		return 0, fmt.Errorf("unsupported comparison op: %v", op)
	}
}

// compareOpcodeTyped returns a specialized comparison opcode when both operands have the same known type.
func compareOpcodeTyped(op compareOp, kind valueKind) (ir.Opcode, error) {
	switch kind {
	case kindInt:
		switch op {
		case eq:
			return ir.OpEqInt, nil
		case ne:
			return ir.OpNeInt, nil
		case lt:
			return ir.OpLtInt, nil
		case lte:
			return ir.OpLteInt, nil
		case gt:
			return ir.OpGtInt, nil
		case gte:
			return ir.OpGteInt, nil
		}
	case kindFloat:
		switch op {
		case eq:
			return ir.OpEqFloat, nil
		case ne:
			return ir.OpNeFloat, nil
		case lt:
			return ir.OpLtFloat, nil
		case lte:
			return ir.OpLteFloat, nil
		case gt:
			return ir.OpGtFloat, nil
		case gte:
			return ir.OpGteFloat, nil
		}
	}
	// Fall back to generic opcode
	return compareOpcode(op)
}

// mathOpcodeTyped returns a specialized math opcode when both operands have the same known type.
func mathOpcodeTyped(op mathOp, kind valueKind) ir.Opcode {
	switch kind {
	case kindInt:
		switch op {
		case add:
			return ir.OpAddInt
		case sub:
			return ir.OpSubInt
		case mult:
			return ir.OpMulInt
		case div:
			return ir.OpDivInt
		}
	case kindFloat:
		switch op {
		case add:
			return ir.OpAddFloat
		case sub:
			return ir.OpSubFloat
		case mult:
			return ir.OpMulFloat
		case div:
			return ir.OpDivFloat
		}
	}
	// Fall back to generic opcode
	switch op {
	case add:
		return ir.OpAdd
	case sub:
		return ir.OpSub
	case mult:
		return ir.OpMul
	case div:
		return ir.OpDiv
	default:
		return ir.OpAdd // should not happen
	}
}

func supportsComparison(op compareOp, left, right valueKind) bool {
	if left == kindUnknown || right == kindUnknown {
		return op == eq || op == ne
	}
	if left == kindInt || left == kindFloat {
		return right == kindInt || right == kindFloat
	}
	if left == kindBool || left == kindString {
		if left != right {
			return false
		}
		return op == eq || op == ne
	}
	if left == kindBytes {
		return right == kindBytes
	}
	return false
}

func inferValueKind(val value) (valueKind, error) {
	switch {
	case val.Literal != nil && val.Literal.Path != nil:
		return kindUnknown, nil
	case val.MathExpression != nil:
		return inferMathExpressionKind(val.MathExpression)
	case val.Literal != nil:
		return inferMathExprLiteralKind(val.Literal)
	case val.Bool != nil:
		return kindBool, nil
	case val.String != nil:
		return kindString, nil
	case val.IsNil != nil:
		return kindUnknown, nil
	case val.Bytes != nil:
		return kindBytes, nil
	case val.Enum != nil:
		return kindUnknown, fmt.Errorf("enum literals unsupported in micro compiler")
	case val.Map != nil:
		return kindUnknown, fmt.Errorf("map literals unsupported in micro compiler")
	case val.List != nil:
		return kindUnknown, fmt.Errorf("list literals unsupported in micro compiler")
	default:
		return kindUnknown, fmt.Errorf("unsupported value in micro compiler")
	}
}

func inferMathExpressionKind(expr *mathExpression) (valueKind, error) {
	if expr == nil {
		return kindUnknown, fmt.Errorf("math expression is nil")
	}
	kind, err := inferAddSubTermKind(expr.Left)
	if err != nil {
		return kindUnknown, err
	}
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return kindUnknown, fmt.Errorf("invalid add/sub term")
		}
		k, err := inferAddSubTermKind(rhs.Term)
		if err != nil {
			return kindUnknown, err
		}
		kind, err = mergeKinds(kind, k)
		if err != nil {
			return kindUnknown, err
		}
	}
	return kind, nil
}

func inferAddSubTermKind(term *addSubTerm) (valueKind, error) {
	if term == nil {
		return kindUnknown, fmt.Errorf("add/sub term is nil")
	}
	kind, err := inferMathValueKind(term.Left)
	if err != nil {
		return kindUnknown, err
	}
	for _, rhs := range term.Right {
		if rhs == nil || rhs.Value == nil {
			return kindUnknown, fmt.Errorf("invalid mult/div value")
		}
		k, err := inferMathValueKind(rhs.Value)
		if err != nil {
			return kindUnknown, err
		}
		kind, err = mergeKinds(kind, k)
		if err != nil {
			return kindUnknown, err
		}
	}
	return kind, nil
}

func inferMathValueKind(val *mathValue) (valueKind, error) {
	switch {
	case val == nil:
		return kindUnknown, fmt.Errorf("math value is nil")
	case val.Literal != nil:
		return inferMathExprLiteralKind(val.Literal)
	case val.SubExpression != nil:
		return inferMathExpressionKind(val.SubExpression)
	default:
		return kindUnknown, fmt.Errorf("unsupported math value")
	}
}

func inferMathExprLiteralKind(lit *mathExprLiteral) (valueKind, error) {
	if lit == nil {
		return kindUnknown, fmt.Errorf("math literal is nil")
	}
	if lit.Int != nil {
		return kindInt, nil
	}
	if lit.Float != nil {
		return kindFloat, nil
	}
	if lit.Path != nil || lit.Converter != nil {
		return kindUnknown, nil
	}
	if lit.Editor != nil {
		return kindUnknown, fmt.Errorf("editor literals unsupported in micro compiler")
	}
	return kindUnknown, fmt.Errorf("unsupported math literal")
}

func mergeKinds(a, b valueKind) (valueKind, error) {
	if a == kindUnknown {
		return b, nil
	}
	if b == kindUnknown {
		return a, nil
	}
	if a != b {
		return kindUnknown, fmt.Errorf("mixed numeric types unsupported in micro compiler")
	}
	return a, nil
}

type foldedNumber struct {
	kind  valueKind
	int64 int64
	float float64
}

func foldValueConst(val value) (ir.Value, bool, error) {
	switch {
	case val.Bool != nil:
		return ir.BoolValue(bool(*val.Bool)), true, nil
	case val.String != nil:
		return ir.StringValue(*val.String), true, nil
	case val.Bytes != nil:
		return ir.BytesValue([]byte(*val.Bytes)), true, nil
	case val.IsNil != nil:
		return ir.Value{Type: ir.TypeNone}, true, nil
	case val.Literal != nil:
		return foldMathExprLiteralConst(val.Literal)
	case val.MathExpression != nil:
		return foldMathExpressionConst(val.MathExpression)
	default:
		return ir.Value{}, false, nil
	}
}

func foldMathExprLiteralConst(lit *mathExprLiteral) (ir.Value, bool, error) {
	if lit == nil {
		return ir.Value{}, false, nil
	}
	switch {
	case lit.Int != nil:
		return ir.Int64Value(*lit.Int), true, nil
	case lit.Float != nil:
		return ir.Float64Value(*lit.Float), true, nil
	default:
		return ir.Value{}, false, nil
	}
}

func foldMathExpressionConst(expr *mathExpression) (ir.Value, bool, error) {
	num, ok, err := foldMathExpressionNumber(expr)
	if !ok || err != nil {
		return ir.Value{}, ok, err
	}
	switch num.kind {
	case kindInt:
		return ir.Int64Value(num.int64), true, nil
	case kindFloat:
		return ir.Float64Value(num.float), true, nil
	default:
		return ir.Value{}, false, nil
	}
}

func foldMathExpressionNumber(expr *mathExpression) (foldedNumber, bool, error) {
	if expr == nil {
		return foldedNumber{}, false, nil
	}
	left, ok, err := foldAddSubTermNumber(expr.Left)
	if !ok || err != nil {
		return foldedNumber{}, ok, err
	}
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return foldedNumber{}, false, fmt.Errorf("invalid add/sub term")
		}
		right, ok, err := foldAddSubTermNumber(rhs.Term)
		if !ok || err != nil {
			return foldedNumber{}, ok, err
		}
		left, ok, err = applyMathOp(rhs.Operator, left, right)
		if !ok || err != nil {
			return foldedNumber{}, ok, err
		}
	}
	return left, true, nil
}

func foldAddSubTermNumber(term *addSubTerm) (foldedNumber, bool, error) {
	if term == nil || term.Left == nil {
		return foldedNumber{}, false, nil
	}
	left, ok, err := foldMathValueNumber(term.Left)
	if !ok || err != nil {
		return foldedNumber{}, ok, err
	}
	for _, rhs := range term.Right {
		if rhs == nil || rhs.Value == nil {
			return foldedNumber{}, false, fmt.Errorf("invalid mult/div value")
		}
		right, ok, err := foldMathValueNumber(rhs.Value)
		if !ok || err != nil {
			return foldedNumber{}, ok, err
		}
		left, ok, err = applyMathOp(rhs.Operator, left, right)
		if !ok || err != nil {
			return foldedNumber{}, ok, err
		}
	}
	return left, true, nil
}

func foldMathValueNumber(val *mathValue) (foldedNumber, bool, error) {
	switch {
	case val == nil:
		return foldedNumber{}, false, nil
	case val.Literal != nil:
		return foldMathLiteralNumber(val.Literal)
	case val.SubExpression != nil:
		return foldMathExpressionNumber(val.SubExpression)
	default:
		return foldedNumber{}, false, nil
	}
}

func foldMathLiteralNumber(lit *mathExprLiteral) (foldedNumber, bool, error) {
	if lit == nil {
		return foldedNumber{}, false, nil
	}
	switch {
	case lit.Int != nil:
		return foldedNumber{kind: kindInt, int64: *lit.Int}, true, nil
	case lit.Float != nil:
		return foldedNumber{kind: kindFloat, float: *lit.Float}, true, nil
	default:
		return foldedNumber{}, false, nil
	}
}

func applyMathOp(op mathOp, left, right foldedNumber) (foldedNumber, bool, error) {
	if left.kind != right.kind {
		if left.kind == kindUnknown || right.kind == kindUnknown {
			return foldedNumber{}, false, nil
		}
		return foldedNumber{}, false, fmt.Errorf("mixed numeric types unsupported in micro compiler")
	}
	switch left.kind {
	case kindInt:
		switch op {
		case add:
			return foldedNumber{kind: kindInt, int64: left.int64 + right.int64}, true, nil
		case sub:
			return foldedNumber{kind: kindInt, int64: left.int64 - right.int64}, true, nil
		case mult:
			return foldedNumber{kind: kindInt, int64: left.int64 * right.int64}, true, nil
		case div:
			if right.int64 == 0 {
				return foldedNumber{}, false, nil
			}
			return foldedNumber{kind: kindInt, int64: left.int64 / right.int64}, true, nil
		default:
			return foldedNumber{}, false, fmt.Errorf("unsupported math op")
		}
	case kindFloat:
		switch op {
		case add:
			return foldedNumber{kind: kindFloat, float: left.float + right.float}, true, nil
		case sub:
			return foldedNumber{kind: kindFloat, float: left.float - right.float}, true, nil
		case mult:
			return foldedNumber{kind: kindFloat, float: left.float * right.float}, true, nil
		case div:
			return foldedNumber{kind: kindFloat, float: left.float / right.float}, true, nil
		default:
			return foldedNumber{}, false, fmt.Errorf("unsupported math op")
		}
	default:
		return foldedNumber{}, false, nil
	}
}

func foldComparisonConst(cmp *comparison) (ir.Value, bool, error) {
	if cmp == nil {
		return ir.Value{}, false, nil
	}
	left, ok, err := foldValueConst(cmp.Left)
	if !ok || err != nil {
		return ir.Value{}, ok, err
	}
	right, ok, err := foldValueConst(cmp.Right)
	if !ok || err != nil {
		return ir.Value{}, ok, err
	}
	leftKind := kindFromValue(left)
	rightKind := kindFromValue(right)
	if !supportsComparison(cmp.Op, leftKind, rightKind) {
		return ir.Value{}, false, fmt.Errorf("unsupported comparison op/type combination")
	}
	result, err := compareConst(cmp.Op, left, right)
	if err != nil {
		return ir.Value{}, false, err
	}
	return result, true, nil
}

func kindFromValue(val ir.Value) valueKind {
	switch val.Type {
	case ir.TypeInt:
		return kindInt
	case ir.TypeFloat:
		return kindFloat
	case ir.TypeBool:
		return kindBool
	case ir.TypeString:
		return kindString
	case ir.TypeBytes:
		return kindBytes
	default:
		return kindUnknown
	}
}

func compareConst(op compareOp, left, right ir.Value) (ir.Value, error) {
	if left.Type == ir.TypeNone || right.Type == ir.TypeNone {
		return compareNil(op, left.Type == ir.TypeNone && right.Type == ir.TypeNone)
	}
	switch {
	case (left.Type == ir.TypeInt || left.Type == ir.TypeFloat) && (right.Type == ir.TypeInt || right.Type == ir.TypeFloat):
		if left.Type == ir.TypeFloat || right.Type == ir.TypeFloat {
			lf := math.Float64frombits(left.Num)
			if left.Type == ir.TypeInt {
				lf = float64(int64(left.Num))
			}
			rf := math.Float64frombits(right.Num)
			if right.Type == ir.TypeInt {
				rf = float64(int64(right.Num))
			}
			return compareFloatConst(op, lf, rf)
		}
		return compareIntConst(op, int64(left.Num), int64(right.Num))
	case left.Type == ir.TypeBool && right.Type == ir.TypeBool:
		return compareBoolConst(op, left.Num != 0, right.Num != 0)
	case left.Type == ir.TypeString && right.Type == ir.TypeString:
		ls, ok := left.String()
		if !ok {
			return ir.Value{}, fmt.Errorf("invalid string constant")
		}
		rs, ok := right.String()
		if !ok {
			return ir.Value{}, fmt.Errorf("invalid string constant")
		}
		return compareStringConst(op, ls, rs)
	case left.Type == ir.TypeBytes && right.Type == ir.TypeBytes:
		ls, ok := left.Bytes()
		if !ok {
			return ir.Value{}, fmt.Errorf("invalid bytes constant")
		}
		rs, ok := right.Bytes()
		if !ok {
			return ir.Value{}, fmt.Errorf("invalid bytes constant")
		}
		return compareBytesConst(op, ls, rs)
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison types")
	}
}

func compareNil(op compareOp, bothNil bool) (ir.Value, error) {
	switch op {
	case eq:
		return ir.BoolValue(bothNil), nil
	case ne:
		return ir.BoolValue(!bothNil), nil
	case lt, gt:
		return ir.BoolValue(false), nil
	case lte, gte:
		return ir.BoolValue(bothNil), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func compareIntConst(op compareOp, a, b int64) (ir.Value, error) {
	switch op {
	case eq:
		return ir.BoolValue(a == b), nil
	case ne:
		return ir.BoolValue(a != b), nil
	case lt:
		return ir.BoolValue(a < b), nil
	case lte:
		return ir.BoolValue(a <= b), nil
	case gt:
		return ir.BoolValue(a > b), nil
	case gte:
		return ir.BoolValue(a >= b), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func compareFloatConst(op compareOp, a, b float64) (ir.Value, error) {
	switch op {
	case eq:
		return ir.BoolValue(a == b), nil
	case ne:
		return ir.BoolValue(a != b), nil
	case lt:
		return ir.BoolValue(a < b), nil
	case lte:
		return ir.BoolValue(a <= b), nil
	case gt:
		return ir.BoolValue(a > b), nil
	case gte:
		return ir.BoolValue(a >= b), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func compareBoolConst(op compareOp, a, b bool) (ir.Value, error) {
	switch op {
	case eq:
		return ir.BoolValue(a == b), nil
	case ne:
		return ir.BoolValue(a != b), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func compareStringConst(op compareOp, a, b string) (ir.Value, error) {
	switch op {
	case eq:
		return ir.BoolValue(a == b), nil
	case ne:
		return ir.BoolValue(a != b), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func compareBytesConst(op compareOp, a, b []byte) (ir.Value, error) {
	cmp := bytes.Compare(a, b)
	switch op {
	case eq:
		return ir.BoolValue(cmp == 0), nil
	case ne:
		return ir.BoolValue(cmp != 0), nil
	case lt:
		return ir.BoolValue(cmp < 0), nil
	case lte:
		return ir.BoolValue(cmp <= 0), nil
	case gt:
		return ir.BoolValue(cmp > 0), nil
	case gte:
		return ir.BoolValue(cmp >= 0), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison op")
	}
}

func foldBooleanExpressionConst(expr *booleanExpression) (bool, bool, error) {
	if expr == nil || expr.Left == nil {
		return false, false, nil
	}
	left, ok, err := foldTermConst(expr.Left)
	if !ok || err != nil {
		return false, ok, err
	}
	result := left
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return false, false, fmt.Errorf("invalid or term")
		}
		val, ok, err := foldTermConst(rhs.Term)
		if !ok || err != nil {
			return false, ok, err
		}
		result = result || val
	}
	return result, true, nil
}

func foldTermConst(term *term) (bool, bool, error) {
	if term == nil || term.Left == nil {
		return false, false, nil
	}
	left, ok, err := foldBooleanValueConst(term.Left)
	if !ok || err != nil {
		return false, ok, err
	}
	result := left
	for _, rhs := range term.Right {
		if rhs == nil || rhs.Value == nil {
			return false, false, fmt.Errorf("invalid and term")
		}
		val, ok, err := foldBooleanValueConst(rhs.Value)
		if !ok || err != nil {
			return false, ok, err
		}
		result = result && val
	}
	return result, true, nil
}

func foldBooleanValueConst(val *booleanValue) (bool, bool, error) {
	if val == nil {
		return false, false, nil
	}
	var (
		result bool
		ok     bool
		err    error
	)
	switch {
	case val.Comparison != nil:
		var folded ir.Value
		folded, ok, err = foldComparisonConst(val.Comparison)
		if !ok || err != nil {
			return false, ok, err
		}
		var boolOk bool
		result, boolOk = folded.Bool()
		if !boolOk {
			return false, false, nil
		}
	case val.ConstExpr != nil:
		if val.ConstExpr.Boolean != nil {
			result = bool(*val.ConstExpr.Boolean)
			ok = true
			break
		}
		if val.ConstExpr.Converter != nil {
			return false, false, nil
		}
		return false, false, fmt.Errorf("boolean converter unsupported in micro compiler")
	case val.SubExpr != nil:
		result, ok, err = foldBooleanExpressionConst(val.SubExpr)
		if !ok || err != nil {
			return false, ok, err
		}
	default:
		return false, false, nil
	}
	if val.Negation != nil {
		result = !result
	}
	return result, ok, nil
}

func (c *microCompiler[K]) emitValue(val value) error {
	if folded, ok, err := foldValueConst(val); err != nil {
		return err
	} else if ok {
		c.emitLoadConst(folded)
		return nil
	}
	switch {
	case val.Literal != nil && val.Literal.Path != nil:
		return c.emitPath(val.Literal.Path)
	case val.MathExpression != nil:
		return c.emitMathExpression(val.MathExpression)
	case val.Literal != nil:
		return c.emitMathExprLiteral(val.Literal)
	case val.Bool != nil:
		c.emitLoadConst(ir.BoolValue(bool(*val.Bool)))
		return nil
	case val.String != nil:
		c.emitLoadConst(ir.StringValue(*val.String))
		return nil
	case val.IsNil != nil:
		c.emitLoadConst(ir.Value{Type: ir.TypeNone})
		return nil
	case val.Bytes != nil:
		c.emitLoadConst(ir.BytesValue([]byte(*val.Bytes)))
		return nil
	case val.Enum != nil:
		return fmt.Errorf("enum literals unsupported in micro compiler")
	case val.Map != nil:
		return fmt.Errorf("map literals unsupported in micro compiler")
	case val.List != nil:
		return fmt.Errorf("list literals unsupported in micro compiler")
	default:
		return fmt.Errorf("unsupported value in micro compiler")
	}
}

func (c *microCompiler[K]) emitBooleanExpression(expr *booleanExpression) error {
	if expr == nil || expr.Left == nil {
		return fmt.Errorf("boolean expression is nil")
	}
	if folded, ok, err := foldBooleanExpressionConst(expr); err != nil {
		return err
	} else if ok {
		c.emitLoadConst(ir.BoolValue(folded))
		return nil
	}
	if err := c.emitTerm(expr.Left); err != nil {
		return err
	}
	var jumps []int
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return fmt.Errorf("invalid or term")
		}
		jumpIdx := c.emitJumpIfTrue()
		jumps = append(jumps, jumpIdx)
		c.emitPop()
		if err := c.emitTerm(rhs.Term); err != nil {
			return err
		}
	}
	return c.patchJumps(jumps, len(c.code))
}

func (c *microCompiler[K]) emitTerm(term *term) error {
	if term == nil || term.Left == nil {
		return fmt.Errorf("term is nil")
	}
	if err := c.emitBooleanValue(term.Left); err != nil {
		return err
	}
	var jumps []int
	for _, rhs := range term.Right {
		if rhs == nil || rhs.Value == nil {
			return fmt.Errorf("invalid and term")
		}
		jumpIdx := c.emitJumpIfFalse()
		jumps = append(jumps, jumpIdx)
		c.emitPop()
		if err := c.emitBooleanValue(rhs.Value); err != nil {
			return err
		}
	}
	return c.patchJumps(jumps, len(c.code))
}

func (c *microCompiler[K]) emitBooleanValue(val *booleanValue) error {
	if val == nil {
		return fmt.Errorf("boolean value is nil")
	}
	switch {
	case val.Comparison != nil:
		if err := c.emitComparison(val.Comparison); err != nil {
			return err
		}
	case val.ConstExpr != nil:
		if val.ConstExpr.Boolean != nil {
			c.emitLoadConst(ir.BoolValue(bool(*val.ConstExpr.Boolean)))
		} else if val.ConstExpr.Converter != nil {
			return c.emitConverter(val.ConstExpr.Converter)
		} else {
			return fmt.Errorf("boolean converter unsupported in micro compiler")
		}
	case val.SubExpr != nil:
		if err := c.emitBooleanExpression(val.SubExpr); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported boolean value in micro compiler")
	}
	if val.Negation != nil {
		c.code = append(c.code, ir.Encode(ir.OpNot, 0))
	}
	return nil
}

func (c *microCompiler[K]) emitComparison(cmp *comparison) error {
	if cmp == nil {
		return fmt.Errorf("comparison is nil")
	}
	leftKind, err := inferValueKind(cmp.Left)
	if err != nil {
		return err
	}
	rightKind, err := inferValueKind(cmp.Right)
	if err != nil {
		return err
	}
	if !supportsComparison(cmp.Op, leftKind, rightKind) {
		return fmt.Errorf("unsupported comparison op/type combination")
	}
	if folded, ok, err := foldComparisonConst(cmp); err != nil {
		return err
	} else if ok {
		c.emitLoadConst(folded)
		return nil
	}
	if ok, err := c.emitNilComparison(cmp); ok || err != nil {
		return err
	}
	if ok, err := c.emitCompareConst(cmp.Left, cmp.Right, cmp.Op); ok || err != nil {
		return err
	}
	if swapOp, ok := invertCompareOpForSwap(cmp.Op); ok {
		if ok, err := c.emitCompareConst(cmp.Right, cmp.Left, swapOp); ok || err != nil {
			return err
		}
	}
	if err := c.emitValue(cmp.Left); err != nil {
		return err
	}
	if err := c.emitValue(cmp.Right); err != nil {
		return err
	}

	// Use specialized opcode when both operands have the same known numeric type
	var op ir.Opcode
	if leftKind == rightKind && (leftKind == kindInt || leftKind == kindFloat) {
		op, err = compareOpcodeTyped(cmp.Op, leftKind)
	} else {
		op, err = compareOpcode(cmp.Op)
	}
	if err != nil {
		return err
	}
	c.code = append(c.code, ir.Encode(op, 0))
	c.onBinaryOp()
	return nil
}

func (c *microCompiler[K]) emitNilComparison(cmp *comparison) (bool, error) {
	if cmp == nil {
		return false, nil
	}
	if cmp.Op != eq && cmp.Op != ne {
		return false, nil
	}
	leftNil := cmp.Left.IsNil != nil
	rightNil := cmp.Right.IsNil != nil
	if leftNil == rightNil {
		return false, nil
	}
	var other value
	if leftNil {
		other = cmp.Right
	} else {
		other = cmp.Left
	}
	if err := c.emitValue(other); err != nil {
		return true, err
	}
	c.code = append(c.code, ir.Encode(ir.OpIsNil, 0))
	if cmp.Op == ne {
		c.code = append(c.code, ir.Encode(ir.OpNot, 0))
	}
	return true, nil
}

func invertCompareOpForSwap(op compareOp) (compareOp, bool) {
	switch op {
	case eq, ne:
		return op, true
	case lt:
		return gt, true
	case lte:
		return gte, true
	case gt:
		return lt, true
	case gte:
		return lte, true
	default:
		return 0, false
	}
}

func (c *microCompiler[K]) emitCompareConst(left value, constVal value, op compareOp) (bool, error) {
	var opcode ir.Opcode
	switch op {
	case eq:
		opcode = ir.OpEqConst
	case ne:
		opcode = ir.OpNeConst
	case lt:
		opcode = ir.OpLtConst
	case lte:
		opcode = ir.OpLteConst
	case gt:
		opcode = ir.OpGtConst
	case gte:
		opcode = ir.OpGteConst
	default:
		return false, nil
	}
	parsed, ok, err := foldValueConst(constVal)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := c.emitValue(left); err != nil {
		return false, err
	}
	idx := c.addConst(parsed)
	c.code = append(c.code, ir.Encode(opcode, idx))
	return true, nil
}

func (c *microCompiler[K]) emitPath(path *path) error {
	if c.parser == nil {
		return fmt.Errorf("path literals unsupported in micro compiler")
	}
	if ok, err := c.emitFastAttr(path); ok || err != nil {
		return err
	}
	if ok, err := c.emitDirectField(path); ok || err != nil {
		return err
	}
	bp, err := c.parser.newPath(path)
	if err != nil {
		return err
	}
	gs, err := c.parser.parsePath(bp)
	if err != nil {
		return err
	}
	var vmGetter VMGetter[K]
	if provider, ok := gs.(VMGetterProvider[K]); ok {
		vmGetter = provider.VMGetter()
	}
	idx := c.addGetter(buildOriginalText(path), gs, vmGetter)
	// Emit OpLoadAttrCached for context-aware execution path
	c.code = append(c.code, ir.Encode(ir.OpLoadAttrCached, idx))
	c.onPush()
	return nil
}

func (c *microCompiler[K]) emitConverter(conv *converter) error {
	if conv == nil {
		return fmt.Errorf("converter is nil")
	}
	if c.parser == nil {
		return fmt.Errorf("converter literals unsupported in micro compiler")
	}
	if ok, err := c.emitNativeConverter(conv); ok || err != nil {
		return err
	}
	if ok, err := c.emitCallSiteConverter(conv); ok || err != nil {
		return err
	}
	gs, err := c.parser.newGetterFromConverter(*conv)
	if err != nil {
		return err
	}
	var vmGetter VMGetter[K]
	if provider, ok := gs.(VMGetterProvider[K]); ok {
		vmGetter = provider.VMGetter()
	}
	key := fmt.Sprintf("converter:%s@%p", conv.Function, conv)
	idx := c.addGetter(key, gs, vmGetter)
	c.code = append(c.code, ir.Encode(ir.OpLoadAttrCached, idx))
	c.onPush()
	return nil
}

type callFrame struct {
	values []ir.Value
}

type vmArgGetter[K any] struct {
	frame *callFrame
	idx   int
}

func (g vmArgGetter[K]) Get(context.Context, K) (any, error) {
	if g.frame == nil || g.idx < 0 || g.idx >= len(g.frame.values) {
		return nil, fmt.Errorf("invalid call argument index")
	}
	return vmValueToAny(g.frame.values[g.idx])
}

func vmValueToAny(val ir.Value) (any, error) {
	switch val.Type {
	case ir.TypeNone:
		return nil, nil
	case ir.TypeInt:
		return int64(val.Num), nil
	case ir.TypeFloat:
		return math.Float64frombits(val.Num), nil
	case ir.TypeBool:
		return val.Num != 0, nil
	case ir.TypeString:
		str, ok := val.String()
		if !ok {
			return nil, fmt.Errorf("invalid string value")
		}
		return str, nil
	case ir.TypeBytes:
		b, ok := val.Bytes()
		if !ok {
			return nil, fmt.Errorf("invalid bytes value")
		}
		return b, nil
	case ir.TypePMap:
		pm, ok := pMapFromValue(val)
		if !ok {
			return nil, fmt.Errorf("invalid map value")
		}
		return pm, nil
	case ir.TypePSlice:
		ps, ok := pSliceFromValue(val)
		if !ok {
			return nil, fmt.Errorf("invalid slice value")
		}
		return ps, nil
	default:
		return nil, fmt.Errorf("unsupported vm value type %v", val.Type)
	}
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

func (c *microCompiler[K]) emitCallSiteConverter(conv *converter) (bool, error) {
	if c.parser == nil {
		return false, nil
	}
	for _, arg := range conv.Arguments {
		if arg.FunctionName != nil {
			return false, nil
		}
		if arg.Value.Map != nil || arg.Value.Enum != nil {
			return false, nil
		}
	}
	callsite, argCount, ok, err := c.buildCallSite(conv)
	if err != nil {
		return true, err
	}
	if !ok {
		return false, nil
	}
	for _, arg := range conv.Arguments {
		if arg.Value.List != nil {
			continue
		}
		if err := c.emitValue(arg.Value); err != nil {
			return true, err
		}
	}
	idx := c.addCallSite(callsite, conv)
	c.code = append(c.code, ir.Encode(ir.OpCall, idx))
	c.needsContext = true
	c.onCall(argCount)
	return true, nil
}

func (c *microCompiler[K]) buildCallSite(conv *converter) (vm.CallSite[K], int, bool, error) {
	f, ok := c.parser.functions[conv.Function]
	if !ok || f == nil {
		return vm.CallSite[K]{}, 0, false, fmt.Errorf("undefined function %q", conv.Function)
	}
	args := f.CreateDefaultArguments()
	if args == nil {
		callsite := vm.CallSite[K]{
			ArgCount: 0,
			Func: func(ctx context.Context, tCtx K, _ []ir.Value) (ir.Value, error) {
				fn, err := f.CreateFunction(FunctionContext{Set: c.parser.telemetrySettings}, nil)
				if err != nil {
					return ir.Value{}, err
				}
				raw, err := fn(ctx, tCtx)
				if err != nil {
					return ir.Value{}, err
				}
				if raw == nil {
					return ir.Value{Type: ir.TypeNone}, nil
				}
				return valueToVM(raw)
			},
		}
		return callsite, 0, true, nil
	}
	if reflect.TypeOf(args).Kind() != reflect.Pointer {
		return vm.CallSite[K]{}, 0, false, fmt.Errorf("factory for %q must return a pointer to an Arguments value", f.Name())
	}
	argsType := reflect.TypeOf(args).Elem()
	argIndexByField := make(map[int]int)
	required := 0
	seenNamed := false
	for i := 0; i < len(conv.Arguments); i++ {
		if !seenNamed && conv.Arguments[i].Name != "" {
			seenNamed = true
		} else if seenNamed && conv.Arguments[i].Name == "" {
			return vm.CallSite[K]{}, 0, false, fmt.Errorf("unnamed argument used after named argument")
		}
	}
	for i := 0; i < argsType.NumField(); i++ {
		if !strings.HasPrefix(argsType.Field(i).Type.Name(), "Optional") {
			required++
		}
	}
	if len(conv.Arguments) < required || len(conv.Arguments) > argsType.NumField() {
		return vm.CallSite[K]{}, 0, false, fmt.Errorf("incorrect number of arguments. Expected: %d Received: %d", argsType.NumField(), len(conv.Arguments))
	}
	if seenNamed {
		for i, arg := range conv.Arguments {
			field, ok := argsType.FieldByName(strcase.ToCamel(arg.Name))
			if !ok {
				return vm.CallSite[K]{}, 0, false, fmt.Errorf("no such parameter: %s", arg.Name)
			}
			argIndexByField[field.Index[0]] = i
		}
	} else {
		for i := range conv.Arguments {
			argIndexByField[i] = i
		}
	}

	for i := 0; i < argsType.NumField(); i++ {
		argIdx, ok := argIndexByField[i]
		if !ok {
			if strings.HasPrefix(argsType.Field(i).Type.Name(), "Optional") {
				continue
			}
			return vm.CallSite[K]{}, 0, false, fmt.Errorf("missing required argument %q", argsType.Field(i).Name)
		}
		if !c.supportsCallSiteArg(argsType.Field(i).Type, conv.Arguments[argIdx].Value) {
			return vm.CallSite[K]{}, 0, false, nil
		}
	}

	vmIndexByArg := make(map[int]int, len(conv.Arguments))
	staticByArg := make(map[int]reflect.Value)
	vmArgCount := 0
	for argIdx := range conv.Arguments {
		if conv.Arguments[argIdx].Value.List != nil {
			continue
		}
		vmIndexByArg[argIdx] = vmArgCount
		vmArgCount++
	}
	for i := 0; i < argsType.NumField(); i++ {
		argIdx, ok := argIndexByField[i]
		if !ok {
			continue
		}
		argVal := conv.Arguments[argIdx].Value
		if argVal.List == nil {
			continue
		}
		field := argsType.Field(i)
		isOptional := strings.HasPrefix(field.Type.Name(), "Optional")
		t := field.Type
		var manager optionalManager
		if isOptional {
			var ok bool
			manager, ok = reflect.New(t).Elem().Interface().(optionalManager)
			if !ok {
				return vm.CallSite[K]{}, 0, false, fmt.Errorf("optional type is not manageable by the OTTL parser")
			}
			t = manager.get().Type()
		}
		if t.Kind() != reflect.Slice {
			return vm.CallSite[K]{}, 0, false, fmt.Errorf("list argument provided for non-slice parameter %q", field.Name)
		}
		staticVal, err := c.parser.buildSliceArg(argVal, t)
		if err != nil {
			return vm.CallSite[K]{}, 0, false, err
		}
		if isOptional {
			staticByArg[argIdx] = manager.set(staticVal)
			continue
		}
		staticByArg[argIdx] = reflect.ValueOf(staticVal)
	}

	callsite := vm.CallSite[K]{
		ArgCount: vmArgCount,
		Func: func(ctx context.Context, tCtx K, values []ir.Value) (ir.Value, error) {
			frame := &callFrame{values: values}
			argsVal := reflect.New(argsType)
			for i := 0; i < argsType.NumField(); i++ {
				argIdx, ok := argIndexByField[i]
				if !ok {
					continue
				}
				field := argsType.Field(i)
				fieldVal := argsVal.Elem().Field(i)
				if staticVal, ok := staticByArg[argIdx]; ok {
					fieldVal.Set(staticVal)
					continue
				}
				isOptional := strings.HasPrefix(field.Type.Name(), "Optional")
				var manager optionalManager
				if isOptional {
					var ok bool
					manager, ok = fieldVal.Interface().(optionalManager)
					if !ok {
						return ir.Value{}, fmt.Errorf("optional type is not manageable by the OTTL parser")
					}
				}
				vmArgIdx, ok := vmIndexByArg[argIdx]
				if !ok {
					return ir.Value{}, fmt.Errorf("missing callsite argument index")
				}
				val, err := c.buildCallSiteArgValue(field.Type, frame, vmArgIdx, isOptional, manager)
				if err != nil {
					return ir.Value{}, err
				}
				if !val.IsValid() {
					continue
				}
				fieldVal.Set(val)
			}
			fn, err := f.CreateFunction(FunctionContext{Set: c.parser.telemetrySettings}, argsVal.Interface())
			if err != nil {
				return ir.Value{}, err
			}
			raw, err := fn(ctx, tCtx)
			if err != nil {
				return ir.Value{}, err
			}
			if raw == nil {
				return ir.Value{Type: ir.TypeNone}, nil
			}
			return valueToVM(raw)
		},
	}
	return callsite, vmArgCount, true, nil
}

func (c *microCompiler[K]) supportsCallSiteArg(argType reflect.Type, argVal value) bool {
	t := argType
	if strings.HasPrefix(t.Name(), "Optional") {
		manager, ok := reflect.New(t).Elem().Interface().(optionalManager)
		if !ok {
			return false
		}
		t = manager.get().Type()
	}
	if argVal.List != nil {
		if t.Kind() != reflect.Slice {
			return false
		}
		return supportsCallSiteSliceElem(t.Elem())
	}
	name := t.Name()
	switch {
	case strings.HasPrefix(name, "Getter"),
		strings.HasPrefix(name, "PMapGetter"),
		strings.HasPrefix(name, "PSliceGetter"),
		strings.HasPrefix(name, "StringGetter"),
		strings.HasPrefix(name, "StringLikeGetter"),
		strings.HasPrefix(name, "FloatGetter"),
		strings.HasPrefix(name, "FloatLikeGetter"),
		strings.HasPrefix(name, "IntGetter"),
		strings.HasPrefix(name, "IntLikeGetter"),
		strings.HasPrefix(name, "BoolGetter"),
		strings.HasPrefix(name, "BoolLikeGetter"),
		strings.HasPrefix(name, "ByteSliceLikeGetter"),
		name == reflect.String.String(),
		name == reflect.Float64.String(),
		name == reflect.Int64.String(),
		name == reflect.Bool.String():
		return argVal.Map == nil && argVal.Enum == nil
	default:
		return false
	}
}

func supportsCallSiteSliceElem(elemType reflect.Type) bool {
	name := elemType.Name()
	switch {
	case strings.HasPrefix(name, "Getter"),
		strings.HasPrefix(name, "PMapGetter"),
		strings.HasPrefix(name, "PSliceGetter"),
		strings.HasPrefix(name, "StringGetter"),
		strings.HasPrefix(name, "StringLikeGetter"),
		strings.HasPrefix(name, "FloatGetter"),
		strings.HasPrefix(name, "FloatLikeGetter"),
		strings.HasPrefix(name, "IntGetter"),
		strings.HasPrefix(name, "IntLikeGetter"),
		strings.HasPrefix(name, "DurationGetter"),
		strings.HasPrefix(name, "TimeGetter"),
		strings.HasPrefix(name, "BoolGetter"),
		strings.HasPrefix(name, "BoolLikeGetter"),
		strings.HasPrefix(name, "ByteSliceLikeGetter"),
		name == reflect.String.String(),
		name == reflect.Float64.String(),
		name == reflect.Int64.String(),
		name == reflect.Bool.String():
		return true
	default:
		return false
	}
}

func (c *microCompiler[K]) buildCallSiteArgValue(fieldType reflect.Type, frame *callFrame, argIdx int, isOptional bool, manager optionalManager) (reflect.Value, error) {
	t := fieldType
	if isOptional {
		t = manager.get().Type()
	}
	name := t.Name()
	getter := vmArgGetter[K]{frame: frame, idx: argIdx}
	switch {
	case strings.HasPrefix(name, "Getter"):
		val := any(getter)
		if isOptional {
			return manager.set(val), nil
		}
		return reflect.ValueOf(val), nil
	case strings.HasPrefix(name, "PMapGetter"):
		arg, err := newStandardPMapGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "PSliceGetter"):
		arg, err := newStandardPSliceGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "StringGetter"):
		arg, err := newStandardStringGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "StringLikeGetter"):
		arg, err := newStandardStringLikeGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "FloatGetter"):
		arg, err := newStandardFloatGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "FloatLikeGetter"):
		arg, err := newStandardFloatLikeGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "IntGetter"):
		arg, err := newStandardIntGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "IntLikeGetter"):
		arg, err := newStandardIntLikeGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "BoolGetter"):
		arg, err := newStandardBoolGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "BoolLikeGetter"):
		arg, err := newStandardBoolLikeGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case strings.HasPrefix(name, "ByteSliceLikeGetter"):
		arg, err := newStandardByteSliceLikeGetter[K](getter)
		if err != nil {
			return reflect.Value{}, err
		}
		if isOptional {
			return manager.set(arg), nil
		}
		return reflect.ValueOf(arg), nil
	case name == reflect.String.String(),
		name == reflect.Float64.String(),
		name == reflect.Int64.String(),
		name == reflect.Bool.String():
		raw, err := vmValueToAny(frame.values[argIdx])
		if err != nil {
			return reflect.Value{}, err
		}
		if raw == nil {
			return reflect.Value{}, fmt.Errorf("nil not supported for %s", name)
		}
		val := reflect.ValueOf(raw)
		if !val.Type().AssignableTo(t) {
			return reflect.Value{}, fmt.Errorf("invalid argument type %T for %s", raw, name)
		}
		if isOptional {
			return manager.set(raw), nil
		}
		return val, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported argument type: %s", name)
	}
}

func (c *microCompiler[K]) emitNativeConverter(conv *converter) (bool, error) {
	if len(conv.Arguments) == 0 {
		return false, nil
	}
	if hasFunctionNameArg(conv.Arguments) {
		return false, nil
	}
	switch conv.Function {
	case "Int":
		arg, ok := converterArg(conv.Arguments, "target", 0)
		if !ok {
			return false, nil
		}
		if err := c.emitValue(arg); err != nil {
			return true, err
		}
		c.code = append(c.code, ir.Encode(ir.OpInt, 0))
		return true, nil
	case "IsInt":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypeInt)
	case "IsDouble":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypeFloat)
	case "IsBool":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypeBool)
	case "IsString":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypeString)
	case "IsMap":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypePMap)
	case "IsList":
		return c.emitIsTypeConverter(conv.Arguments, ir.TypePSlice)
	case "IsMatch":
		target, pattern, ok := converterArgs(conv.Arguments, "target", "pattern")
		if !ok {
			return false, nil
		}
		patternConst, ok, err := foldValueConst(pattern)
		if err != nil {
			return true, err
		}
		if ok && patternConst.Type == ir.TypeString {
			patternStr, ok := patternConst.String()
			if !ok {
				return true, fmt.Errorf("invalid IsMatch pattern")
			}
			idx, err := c.addRegexp(patternStr)
			if err != nil {
				return true, err
			}
			if err := c.emitValue(target); err != nil {
				return true, err
			}
			c.code = append(c.code, ir.Encode(ir.OpIsMatch, idx))
			return true, nil
		}
		if err := c.emitValue(target); err != nil {
			return true, err
		}
		if err := c.emitValue(pattern); err != nil {
			return true, err
		}
		c.code = append(c.code, ir.Encode(ir.OpIsMatchDynamic, 0))
		return true, nil
	default:
		return false, nil
	}
}

func (c *microCompiler[K]) emitIsTypeConverter(args []argument, typ ir.Type) (bool, error) {
	arg, ok := converterArg(args, "target", 0)
	if !ok {
		return false, nil
	}
	if err := c.emitValue(arg); err != nil {
		return true, err
	}
	c.code = append(c.code, ir.Encode(ir.OpIsType, uint32(typ)))
	return true, nil
}

func converterArg(args []argument, name string, idx int) (value, bool) {
	if len(args) == 0 {
		return value{}, false
	}
	if usesNamedArgs(args) {
		for _, arg := range args {
			if arg.Name == name {
				return arg.Value, true
			}
		}
		return value{}, false
	}
	if idx >= len(args) {
		return value{}, false
	}
	return args[idx].Value, true
}

func converterArgs(args []argument, firstName, secondName string) (value, value, bool) {
	first, ok := converterArg(args, firstName, 0)
	if !ok {
		return value{}, value{}, false
	}
	second, ok := converterArg(args, secondName, 1)
	if !ok {
		return value{}, value{}, false
	}
	return first, second, true
}

func usesNamedArgs(args []argument) bool {
	for _, arg := range args {
		if arg.Name != "" {
			return true
		}
	}
	return false
}

func hasFunctionNameArg(args []argument) bool {
	for _, arg := range args {
		if arg.FunctionName != nil {
			return true
		}
	}
	return false
}

func (c *microCompiler[K]) emitFastAttr(path *path) (bool, error) {
	if path == nil || len(path.Fields) != 1 {
		return false, nil
	}
	if c.attrGetter == nil {
		return false, nil
	}
	if c.parser != nil && len(c.parser.vmAttrContextNames) > 0 {
		if _, ok := c.parser.vmAttrContextNames[path.Context]; !ok {
			return false, nil
		}
	}
	field := path.Fields[0]
	if field.Name != "attributes" || len(field.Keys) != 1 {
		return false, nil
	}
	if field.Keys[0].String == nil {
		return false, nil
	}
	idx := c.addAttrKey(*field.Keys[0].String)
	c.code = append(c.code, ir.Encode(ir.OpLoadAttrFast, idx))
	c.onPush()
	return true, nil
}

func (c *microCompiler[K]) emitDirectField(path *path) (bool, error) {
	if path == nil {
		return false, nil
	}
	hasContext := func(name string) bool {
		if c.parser == nil {
			return false
		}
		if c.parser.pathContextNames != nil {
			if _, ok := c.parser.pathContextNames[name]; ok {
				return true
			}
		}
		if c.parser.vmAttrContextNames != nil {
			if _, ok := c.parser.vmAttrContextNames[name]; ok {
				return true
			}
		}
		return false
	}
	contextAllowed := func(name string, conflicts ...string) bool {
		if path.Context == name {
			return true
		}
		if path.Context != "" {
			return false
		}
		if !hasContext(name) {
			return false
		}
		for _, conflict := range conflicts {
			if hasContext(conflict) {
				return false
			}
		}
		return true
	}
	spanAllowed := contextAllowed("span", "metric", "scope")
	metricAllowed := contextAllowed("metric", "span", "scope")
	resourceAllowed := contextAllowed("resource", "scope")
	scopeAllowed := contextAllowed("scope", "resource") || path.Context == "instrumentation_scope"
	emit := func(op ir.Opcode) {
		c.code = append(c.code, ir.Encode(op, 0))
		c.needsContext = true
		c.onPush()
	}

	switch len(path.Fields) {
	case 1:
		field := path.Fields[0]
		if field.Name == "trace_state" && len(field.Keys) == 1 && spanAllowed {
			if field.Keys[0].String == nil {
				return false, nil
			}
			idx := c.addAttrKey(*field.Keys[0].String)
			c.code = append(c.code, ir.Encode(ir.OpGetSpanTraceStateKey, idx))
			c.needsContext = true
			c.onPush()
			return true, nil
		}
		if len(field.Keys) != 0 {
			return false, nil
		}
		if path.Context == "" || path.Context == "log" {
			switch field.Name {
			case "body":
				emit(ir.OpGetBody)
				return true, nil
			case "severity_number":
				emit(ir.OpGetSeverity)
				return true, nil
			case "time_unix_nano":
				emit(ir.OpGetTimestamp)
				return true, nil
			case "observed_time_unix_nano":
				emit(ir.OpGetObservedTimestamp)
				return true, nil
			case "severity_text":
				emit(ir.OpGetSeverityText)
				return true, nil
			case "flags":
				emit(ir.OpGetLogFlags)
				return true, nil
			}
		}
		if spanAllowed {
			switch field.Name {
			case "name":
				emit(ir.OpGetSpanName)
				return true, nil
			case "start_time_unix_nano":
				emit(ir.OpGetSpanStartTime)
				return true, nil
			case "end_time_unix_nano":
				emit(ir.OpGetSpanEndTime)
				return true, nil
			case "kind":
				emit(ir.OpGetSpanKind)
				return true, nil
			case "trace_id":
				emit(ir.OpGetSpanTraceID)
				return true, nil
			case "span_id":
				emit(ir.OpGetSpanID)
				return true, nil
			case "parent_span_id":
				emit(ir.OpGetSpanParentID)
				return true, nil
			case "trace_state":
				emit(ir.OpGetSpanTraceState)
				return true, nil
			case "dropped_attributes_count":
				emit(ir.OpGetSpanDroppedAttributesCount)
				return true, nil
			case "dropped_events_count":
				emit(ir.OpGetSpanDroppedEventsCount)
				return true, nil
			case "dropped_links_count":
				emit(ir.OpGetSpanDroppedLinksCount)
				return true, nil
			}
		}
		if metricAllowed {
			switch field.Name {
			case "name":
				emit(ir.OpGetMetricName)
				return true, nil
			case "description":
				emit(ir.OpGetMetricDescription)
				return true, nil
			case "unit":
				emit(ir.OpGetMetricUnit)
				return true, nil
			case "type":
				emit(ir.OpGetMetricType)
				return true, nil
			case "aggregation_temporality":
				emit(ir.OpGetMetricAggTemporality)
				return true, nil
			case "is_monotonic":
				emit(ir.OpGetMetricIsMonotonic)
				return true, nil
			}
		}
		if resourceAllowed {
			switch field.Name {
			case "dropped_attributes_count":
				emit(ir.OpGetResourceDroppedAttributesCount)
				return true, nil
			case "schema_url":
				emit(ir.OpGetResourceSchemaURL)
				return true, nil
			}
		}
		if scopeAllowed {
			switch field.Name {
			case "name":
				emit(ir.OpGetScopeName)
				return true, nil
			case "version":
				emit(ir.OpGetScopeVersion)
				return true, nil
			case "dropped_attributes_count":
				emit(ir.OpGetScopeDroppedAttributesCount)
				return true, nil
			case "schema_url":
				emit(ir.OpGetScopeSchemaURL)
				return true, nil
			}
		}
	case 2:
		field := path.Fields[0]
		next := path.Fields[1]
		if len(field.Keys) != 0 || len(next.Keys) != 0 {
			return false, nil
		}
		if field.Name == "status" && spanAllowed {
			switch next.Name {
			case "code":
				emit(ir.OpGetSpanStatus)
				return true, nil
			case "message":
				emit(ir.OpGetSpanStatusMsg)
				return true, nil
			}
		}
		if spanAllowed {
			switch field.Name {
			case "trace_id":
				if next.Name == "string" {
					emit(ir.OpGetSpanTraceIDString)
					return true, nil
				}
			case "span_id":
				if next.Name == "string" {
					emit(ir.OpGetSpanIDString)
					return true, nil
				}
			case "parent_span_id":
				if next.Name == "string" {
					emit(ir.OpGetSpanParentIDString)
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (c *microCompiler[K]) emitMathExpression(expr *mathExpression) error {
	if expr == nil {
		return fmt.Errorf("math expression is nil")
	}
	if folded, ok, err := foldMathExpressionConst(expr); err != nil {
		return err
	} else if ok {
		c.emitLoadConst(folded)
		return nil
	}

	// Infer overall expression type for specialized opcodes
	exprKind, _ := inferMathExpressionKind(expr)

	if err := c.emitAddSubTermTyped(expr.Left, exprKind); err != nil {
		return err
	}
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return fmt.Errorf("invalid add/sub term")
		}
		if err := c.emitAddSubTermTyped(rhs.Term, exprKind); err != nil {
			return err
		}
		op := mathOpcodeTyped(rhs.Operator, exprKind)
		c.code = append(c.code, ir.Encode(op, 0))
		c.onBinaryOp()
	}
	return nil
}

func (c *microCompiler[K]) emitAddSubTerm(term *addSubTerm) error {
	return c.emitAddSubTermTyped(term, kindUnknown)
}

func (c *microCompiler[K]) emitAddSubTermTyped(term *addSubTerm, kind valueKind) error {
	if term == nil {
		return fmt.Errorf("add/sub term is nil")
	}
	if err := c.emitMathValue(term.Left); err != nil {
		return err
	}
	for _, rhs := range term.Right {
		if rhs == nil || rhs.Value == nil {
			return fmt.Errorf("invalid mult/div value")
		}
		if err := c.emitMathValue(rhs.Value); err != nil {
			return err
		}
		op := mathOpcodeTyped(rhs.Operator, kind)
		c.code = append(c.code, ir.Encode(op, 0))
		c.onBinaryOp()
	}
	return nil
}

func (c *microCompiler[K]) emitMathValue(val *mathValue) error {
	switch {
	case val == nil:
		return fmt.Errorf("math value is nil")
	case val.Literal != nil:
		return c.emitMathExprLiteral(val.Literal)
	case val.SubExpression != nil:
		return c.emitMathExpression(val.SubExpression)
	default:
		return fmt.Errorf("unsupported math value")
	}
}

func (c *microCompiler[K]) emitMathExprLiteral(lit *mathExprLiteral) error {
	if lit == nil {
		return fmt.Errorf("math literal is nil")
	}
	if lit.Path != nil {
		return c.emitPath(lit.Path)
	}
	if lit.Converter != nil {
		return c.emitConverter(lit.Converter)
	}
	if lit.Editor != nil {
		return fmt.Errorf("editor literals unsupported in micro compiler")
	}
	if lit.Int != nil {
		c.emitLoadConst(ir.Int64Value(*lit.Int))
		return nil
	}
	if lit.Float != nil {
		c.emitLoadConst(ir.Float64Value(*lit.Float))
		return nil
	}
	return fmt.Errorf("unsupported math literal")
}

func (c *microCompiler[K]) emitLoadConst(val ir.Value) {
	idx := c.addConst(val)
	c.code = append(c.code, ir.Encode(ir.OpLoadConst, idx))
	c.onPush()
}

func (c *microCompiler[K]) emitPop() {
	c.code = append(c.code, ir.Encode(ir.OpPop, 0))
	c.onPop()
}

func (c *microCompiler[K]) emitJumpIfTrue() int {
	c.code = append(c.code, ir.Encode(ir.OpJumpIfTrue, 0))
	return len(c.code) - 1
}

func (c *microCompiler[K]) emitJumpIfFalse() int {
	c.code = append(c.code, ir.Encode(ir.OpJumpIfFalse, 0))
	return len(c.code) - 1
}

func (c *microCompiler[K]) patchJumps(jumps []int, target int) error {
	if len(jumps) == 0 {
		return nil
	}
	if target < 0 || target > maxInstructionArg {
		return fmt.Errorf("jump target out of range: %d", target)
	}
	for _, idx := range jumps {
		if idx < 0 || idx >= len(c.code) {
			return fmt.Errorf("jump index out of range")
		}
		op := c.code[idx].Op()
		c.code[idx] = ir.Encode(op, uint32(target))
	}
	return nil
}

func (c *microCompiler[K]) addConst(val ir.Value) uint32 {
	key := constKey{typ: val.Type, num: val.Num}
	if val.Type == ir.TypeString {
		if s, ok := val.String(); ok {
			key.str = s
		}
	}
	if val.Type == ir.TypeBytes {
		if b, ok := val.Bytes(); ok {
			key.str = string(b)
		}
	}
	if idx, ok := c.constIndex[key]; ok {
		return idx
	}
	idx := uint32(len(c.consts))
	c.consts = append(c.consts, val)
	c.constIndex[key] = idx
	return idx
}

func (c *microCompiler[K]) addRegexp(pattern string) (uint32, error) {
	if c.regexIndex == nil {
		c.regexIndex = map[string]uint32{}
	}
	if idx, ok := c.regexIndex[pattern]; ok {
		return idx, nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return 0, fmt.Errorf("the regex pattern supplied to IsMatch '%q' is not a valid pattern: %w", pattern, err)
	}
	idx := uint32(len(c.regexps))
	c.regexps = append(c.regexps, compiled)
	c.regexIndex[pattern] = idx
	return idx, nil
}

func (c *microCompiler[K]) addCallSite(site vm.CallSite[K], conv *converter) uint32 {
	if c.callSiteIndex == nil {
		c.callSiteIndex = map[string]uint32{}
	}
	key := fmt.Sprintf("callsite:%s@%p", conv.Function, conv)
	if idx, ok := c.callSiteIndex[key]; ok {
		return idx
	}
	idx := uint32(len(c.callSites))
	c.callSites = append(c.callSites, site)
	c.callSiteIndex[key] = idx
	return idx
}

func (c *microCompiler[K]) addGetter(key string, getter Getter[K], vmGetter VMGetter[K]) uint32 {
	if idx, ok := c.getterIndex[key]; ok {
		if vmGetter != nil && int(idx) < len(c.vmGetters) && c.vmGetters[idx] == nil {
			c.vmGetters[idx] = vmGetter
		}
		return idx
	}
	idx := uint32(len(c.getters))
	c.getters = append(c.getters, getter)
	c.vmGetters = append(c.vmGetters, vmGetter)
	c.getterIndex[key] = idx
	return idx
}

func (c *microCompiler[K]) addAttrKey(key string) uint32 {
	if idx, ok := c.attrKeyIndex[key]; ok {
		return idx
	}
	idx := uint32(len(c.attrKeys))
	c.attrKeys = append(c.attrKeys, key)
	c.attrKeyIndex[key] = idx
	return idx
}

func (c *microCompiler[K]) onPush() {
	c.stackDepth++
	if c.stackDepth > c.maxStack {
		c.maxStack = c.stackDepth
	}
}

func (c *microCompiler[K]) onBinaryOp() {
	if c.stackDepth >= 2 {
		c.stackDepth--
	}
}

func (c *microCompiler[K]) onCall(argCount int) {
	if argCount <= 0 {
		c.stackDepth++
		if c.stackDepth > c.maxStack {
			c.maxStack = c.stackDepth
		}
		return
	}
	if c.stackDepth >= argCount {
		c.stackDepth = c.stackDepth - argCount + 1
		if c.stackDepth > c.maxStack {
			c.maxStack = c.stackDepth
		}
	}
}

func (c *microCompiler[K]) onPop() {
	if c.stackDepth >= 1 {
		c.stackDepth--
	}
}

// buildAccessors creates PathAccessors from the compiled getters.
// These accessors are compiled once and stored in Program.Accessors,
// receiving ctx/tCtx as arguments to avoid per-run closure allocations.
// Generic over K to eliminate interface{} conversions entirely.
func (c *microCompiler[K]) buildAccessors() []vm.PathAccessor[K] {
	if len(c.getters) == 0 {
		return nil
	}
	accessors := make([]vm.PathAccessor[K], len(c.getters))
	for i := range c.getters {
		getter := c.getters[i]
		var vmGetter VMGetter[K]
		if i < len(c.vmGetters) {
			vmGetter = c.vmGetters[i]
		}
		// No type assertion needed - K is concrete at compile time
		if vmGetter != nil {
			accessors[i] = func(ctx context.Context, tCtx K) (ir.Value, error) {
				return vmGetter.GetVM(ctx, tCtx)
			}
		} else {
			accessors[i] = func(ctx context.Context, tCtx K) (ir.Value, error) {
				raw, err := getter.Get(ctx, tCtx)
				if err != nil {
					return ir.Value{}, err
				}
				return valueToVM(raw)
			}
		}
	}
	return accessors
}
