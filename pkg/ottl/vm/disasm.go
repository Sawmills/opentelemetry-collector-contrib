// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"

import (
	"fmt"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

// Disassemble renders bytecode into a human-readable format.
func Disassemble[K any](p *Program[K]) string {
	if p == nil {
		return ""
	}
	var b strings.Builder
	for i, inst := range p.Code {
		op := inst.Op()
		name := opcodeName(op)
		if op == ir.OpLoadConst {
			idx := inst.Arg()
			fmt.Fprintf(&b, "%03d %-12s %d", i, name, idx)
			if int(idx) < len(p.Consts) {
				b.WriteString(" ; ")
				b.WriteString(formatConst(p.Consts[idx]))
			}
		} else if op == ir.OpLoadGetter {
			fmt.Fprintf(&b, "%03d %-12s %d", i, name, inst.Arg())
		} else {
			fmt.Fprintf(&b, "%03d %-12s %d", i, name, inst.Arg())
		}
		if i < len(p.Code)-1 {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func opcodeName(op ir.Opcode) string {
	switch op {
	case ir.OpLoadConst:
		return "LOAD_CONST"
	case ir.OpLoadGetter:
		return "LOAD_GETTER"
	case ir.OpAdd:
		return "ADD"
	case ir.OpSub:
		return "SUB"
	case ir.OpMul:
		return "MUL"
	case ir.OpDiv:
		return "DIV"
	case ir.OpEq:
		return "EQ"
	case ir.OpNe:
		return "NE"
	case ir.OpLt:
		return "LT"
	case ir.OpLte:
		return "LTE"
	case ir.OpGt:
		return "GT"
	case ir.OpGte:
		return "GTE"
	case ir.OpAddInt:
		return "ADD_INT"
	case ir.OpSubInt:
		return "SUB_INT"
	case ir.OpMulInt:
		return "MUL_INT"
	case ir.OpDivInt:
		return "DIV_INT"
	case ir.OpEqInt:
		return "EQ_INT"
	case ir.OpNeInt:
		return "NE_INT"
	case ir.OpLtInt:
		return "LT_INT"
	case ir.OpLteInt:
		return "LTE_INT"
	case ir.OpGtInt:
		return "GT_INT"
	case ir.OpGteInt:
		return "GTE_INT"
	case ir.OpAddFloat:
		return "ADD_FLOAT"
	case ir.OpSubFloat:
		return "SUB_FLOAT"
	case ir.OpMulFloat:
		return "MUL_FLOAT"
	case ir.OpDivFloat:
		return "DIV_FLOAT"
	case ir.OpEqFloat:
		return "EQ_FLOAT"
	case ir.OpNeFloat:
		return "NE_FLOAT"
	case ir.OpLtFloat:
		return "LT_FLOAT"
	case ir.OpLteFloat:
		return "LTE_FLOAT"
	case ir.OpGtFloat:
		return "GT_FLOAT"
	case ir.OpGteFloat:
		return "GTE_FLOAT"
	case ir.OpJump:
		return "JUMP"
	case ir.OpJumpIfTrue:
		return "JUMP_IF_TRUE"
	case ir.OpJumpIfFalse:
		return "JUMP_IF_FALSE"
	case ir.OpPop:
		return "POP"
	case ir.OpNot:
		return "NOT"
	case ir.OpDup:
		return "DUP"
	case ir.OpNegInt:
		return "NEG_INT"
	case ir.OpNegFloat:
		return "NEG_FLOAT"
	case ir.OpLoadAttrCached:
		return "LOAD_ATTR_CACHED"
	case ir.OpSetAttrCached:
		return "SET_ATTR_CACHED"
	case ir.OpLoadAttrFast:
		return "LOAD_ATTR_FAST"
	case ir.OpSetAttrFast:
		return "SET_ATTR_FAST"
	case ir.OpGetBody:
		return "GET_BODY"
	case ir.OpSetBody:
		return "SET_BODY"
	case ir.OpGetSeverity:
		return "GET_SEVERITY"
	case ir.OpSetSeverity:
		return "SET_SEVERITY"
	case ir.OpGetTimestamp:
		return "GET_TIMESTAMP"
	case ir.OpSetTimestamp:
		return "SET_TIMESTAMP"
	case ir.OpEqConst:
		return "EQ_CONST"
	case ir.OpNeConst:
		return "NE_CONST"
	case ir.OpLtConst:
		return "LT_CONST"
	case ir.OpLteConst:
		return "LTE_CONST"
	case ir.OpGtConst:
		return "GT_CONST"
	case ir.OpGteConst:
		return "GTE_CONST"
	case ir.OpGetSpanName:
		return "GET_SPAN_NAME"
	case ir.OpSetSpanName:
		return "SET_SPAN_NAME"
	case ir.OpGetSpanStartTime:
		return "GET_SPAN_START_TIME"
	case ir.OpSetSpanStartTime:
		return "SET_SPAN_START_TIME"
	case ir.OpGetSpanEndTime:
		return "GET_SPAN_END_TIME"
	case ir.OpSetSpanEndTime:
		return "SET_SPAN_END_TIME"
	default:
		return fmt.Sprintf("OP_%d", op)
	}
}

func formatConst(val ir.Value) string {
	switch val.Type {
	case ir.TypeInt:
		return fmt.Sprintf("%d", int64(val.Num))
	case ir.TypeFloat:
		return fmt.Sprintf("%g", mathFloat(val))
	case ir.TypeBool:
		return fmt.Sprintf("%t", val.Num != 0)
	case ir.TypeString:
		if s, ok := val.String(); ok {
			return fmt.Sprintf("%q", s)
		}
		return "<string>"
	default:
		return "<?>"
	}
}

func mathFloat(val ir.Value) float64 {
	f, _ := val.Float64()
	return f
}
