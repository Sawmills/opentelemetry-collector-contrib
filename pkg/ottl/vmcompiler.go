// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"fmt"
	"math"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
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
)

const maxInstructionArg = 0x00FFFFFF

type microProgram[K any] struct {
	program   *vm.Program[K]
	getters   []Getter[K]
	vmGetters []VMGetter[K]
	stack     int
}

type microCompiler[K any] struct {
	parser       *Parser[K]
	code         []ir.Instruction
	consts       []ir.Value
	constIndex   map[constKey]uint32
	getterIndex  map[string]uint32
	getters      []Getter[K]
	vmGetters    []VMGetter[K]
	attrKeys     []string
	attrKeyIndex map[string]uint32
	attrGetter   vm.AttrGetter[K]
	attrSetter   vm.AttrSetter[K]
	stackDepth   int
	maxStack     int
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
		parser:       p,
		constIndex:   map[constKey]uint32{},
		getterIndex:  map[string]uint32{},
		attrKeyIndex: map[string]uint32{},
		attrGetter:   p.vmAttrGetter,
		attrSetter:   p.vmAttrSetter,
	}
	if err := c.emitComparison(cmp); err != nil {
		return nil, err
	}
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program[K]{
			Code:       c.code,
			Consts:     c.consts,
			Accessors:  c.buildAccessors(),
			AttrKeys:   c.attrKeys,
			AttrGetter: c.attrGetter,
			AttrSetter: c.attrSetter,
			GasLimit:   vmGasLimitFor(p),
		},
		getters:   c.getters,
		vmGetters: c.vmGetters,
		stack:     c.maxStack,
	}, nil
}

func (p *Parser[K]) compileMicroBoolExpression(expr *booleanExpression) (*microProgram[K], error) {
	if expr == nil {
		return nil, fmt.Errorf("boolean expression is nil")
	}
	c := &microCompiler[K]{
		parser:       p,
		constIndex:   map[constKey]uint32{},
		getterIndex:  map[string]uint32{},
		attrKeyIndex: map[string]uint32{},
		attrGetter:   p.vmAttrGetter,
		attrSetter:   p.vmAttrSetter,
	}
	if err := c.emitBooleanExpression(expr); err != nil {
		return nil, err
	}
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program[K]{
			Code:       c.code,
			Consts:     c.consts,
			Accessors:  c.buildAccessors(),
			AttrKeys:   c.attrKeys,
			AttrGetter: c.attrGetter,
			AttrSetter: c.attrSetter,
			GasLimit:   vmGasLimitFor(p),
		},
		getters:   c.getters,
		vmGetters: c.vmGetters,
		stack:     c.maxStack,
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
		return kindUnknown, fmt.Errorf("nil literals unsupported in micro compiler")
	case val.Bytes != nil:
		return kindUnknown, fmt.Errorf("byte literals unsupported in micro compiler")
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
	default:
		return kindUnknown
	}
}

func compareConst(op compareOp, left, right ir.Value) (ir.Value, error) {
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
	default:
		return ir.Value{}, fmt.Errorf("unsupported comparison types")
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
		if val.ConstExpr.Boolean == nil {
			return false, false, fmt.Errorf("boolean converter unsupported in micro compiler")
		}
		result = bool(*val.ConstExpr.Boolean)
		ok = true
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
		return fmt.Errorf("nil literals unsupported in micro compiler")
	case val.Bytes != nil:
		return fmt.Errorf("byte literals unsupported in micro compiler")
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
	switch len(path.Fields) {
	case 1:
		field := path.Fields[0]
		if len(field.Keys) != 0 {
			return false, nil
		}
		switch path.Context {
		case "", "log":
			switch field.Name {
			case "body":
				c.code = append(c.code, ir.Encode(ir.OpGetBody, 0))
				c.onPush()
				return true, nil
			case "severity_number":
				c.code = append(c.code, ir.Encode(ir.OpGetSeverity, 0))
				c.onPush()
				return true, nil
			case "time_unix_nano":
				c.code = append(c.code, ir.Encode(ir.OpGetTimestamp, 0))
				c.onPush()
				return true, nil
			}
		case "span":
			switch field.Name {
			case "name":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanName, 0))
				c.onPush()
				return true, nil
			case "start_time_unix_nano":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanStartTime, 0))
				c.onPush()
				return true, nil
			case "end_time_unix_nano":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanEndTime, 0))
				c.onPush()
				return true, nil
			case "kind":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanKind, 0))
				c.onPush()
				return true, nil
			}
		}
		if path.Context == "" {
			switch field.Name {
			case "name":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanName, 0))
				c.onPush()
				return true, nil
			case "start_time_unix_nano":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanStartTime, 0))
				c.onPush()
				return true, nil
			case "end_time_unix_nano":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanEndTime, 0))
				c.onPush()
				return true, nil
			case "kind":
				c.code = append(c.code, ir.Encode(ir.OpGetSpanKind, 0))
				c.onPush()
				return true, nil
			}
		}
	case 2:
		field := path.Fields[0]
		next := path.Fields[1]
		if len(field.Keys) != 0 || len(next.Keys) != 0 {
			return false, nil
		}
		if field.Name == "status" && next.Name == "code" {
			if path.Context == "" || path.Context == "span" {
				c.code = append(c.code, ir.Encode(ir.OpGetSpanStatus, 0))
				c.onPush()
				return true, nil
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
	if idx, ok := c.constIndex[key]; ok {
		return idx
	}
	idx := uint32(len(c.consts))
	c.consts = append(c.consts, val)
	c.constIndex[key] = idx
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
