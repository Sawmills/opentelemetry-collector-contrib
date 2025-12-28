// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"fmt"

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
	program *vm.Program
	getters []Getter[K]
	stack   int
}

type microCompiler[K any] struct {
	parser      *Parser[K]
	code        []ir.Instruction
	consts      []ir.Value
	constIndex  map[constKey]uint32
	getterIndex map[string]uint32
	getters     []Getter[K]
	stackDepth  int
	maxStack    int
}

func compileMicroMathExpression(expr *mathExpression) (*vm.Program, error) {
	if _, err := inferMathExpressionKind(expr); err != nil {
		return nil, err
	}
	c := &microCompiler[any]{constIndex: map[constKey]uint32{}}
	if err := c.emitMathExpression(expr); err != nil {
		return nil, err
	}
	return &vm.Program{Code: c.code, Consts: c.consts}, nil
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
		parser:      p,
		constIndex:  map[constKey]uint32{},
		getterIndex: map[string]uint32{},
	}
	if err := c.emitValue(cmp.Left); err != nil {
		return nil, err
	}
	if err := c.emitValue(cmp.Right); err != nil {
		return nil, err
	}

	op, err := compareOpcode(cmp.Op)
	if err != nil {
		return nil, err
	}
	c.code = append(c.code, ir.Encode(op, 0))
	c.onBinaryOp()
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program{Code: c.code, Consts: c.consts},
		getters: c.getters,
		stack:   c.maxStack,
	}, nil
}

func (p *Parser[K]) compileMicroBoolExpression(expr *booleanExpression) (*microProgram[K], error) {
	if expr == nil {
		return nil, fmt.Errorf("boolean expression is nil")
	}
	c := &microCompiler[K]{
		parser:      p,
		constIndex:  map[constKey]uint32{},
		getterIndex: map[string]uint32{},
	}
	if err := c.emitBooleanExpression(expr); err != nil {
		return nil, err
	}
	if c.maxStack > defaultMicroVMStackSize {
		return nil, fmt.Errorf("vm stack depth %d exceeds max %d", c.maxStack, defaultMicroVMStackSize)
	}
	return &microProgram[K]{
		program: &vm.Program{Code: c.code, Consts: c.consts},
		getters: c.getters,
		stack:   c.maxStack,
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

func (c *microCompiler[K]) emitValue(val value) error {
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
	if err := c.emitValue(cmp.Left); err != nil {
		return err
	}
	if err := c.emitValue(cmp.Right); err != nil {
		return err
	}
	op, err := compareOpcode(cmp.Op)
	if err != nil {
		return err
	}
	c.code = append(c.code, ir.Encode(op, 0))
	c.onBinaryOp()
	return nil
}

func (c *microCompiler[K]) emitPath(path *path) error {
	if c.parser == nil {
		return fmt.Errorf("path literals unsupported in micro compiler")
	}
	bp, err := c.parser.newPath(path)
	if err != nil {
		return err
	}
	gs, err := c.parser.parsePath(bp)
	if err != nil {
		return err
	}
	idx := c.addGetter(buildOriginalText(path), gs)
	c.code = append(c.code, ir.Encode(ir.OpLoadGetter, idx))
	c.onPush()
	return nil
}

func (c *microCompiler[K]) emitMathExpression(expr *mathExpression) error {
	if expr == nil {
		return fmt.Errorf("math expression is nil")
	}
	if err := c.emitAddSubTerm(expr.Left); err != nil {
		return err
	}
	for _, rhs := range expr.Right {
		if rhs == nil || rhs.Term == nil {
			return fmt.Errorf("invalid add/sub term")
		}
		if err := c.emitAddSubTerm(rhs.Term); err != nil {
			return err
		}
		switch rhs.Operator {
		case add:
			c.code = append(c.code, ir.Encode(ir.OpAdd, 0))
			c.onBinaryOp()
		case sub:
			c.code = append(c.code, ir.Encode(ir.OpSub, 0))
			c.onBinaryOp()
		default:
			return fmt.Errorf("unsupported add/sub operator: %v", rhs.Operator)
		}
	}
	return nil
}

func (c *microCompiler[K]) emitAddSubTerm(term *addSubTerm) error {
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
		switch rhs.Operator {
		case mult:
			c.code = append(c.code, ir.Encode(ir.OpMul, 0))
			c.onBinaryOp()
		case div:
			c.code = append(c.code, ir.Encode(ir.OpDiv, 0))
			c.onBinaryOp()
		default:
			return fmt.Errorf("unsupported mult/div operator: %v", rhs.Operator)
		}
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

func (c *microCompiler[K]) addGetter(key string, getter Getter[K]) uint32 {
	if idx, ok := c.getterIndex[key]; ok {
		return idx
	}
	idx := uint32(len(c.getters))
	c.getters = append(c.getters, getter)
	c.getterIndex[key] = idx
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
