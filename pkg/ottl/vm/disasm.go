// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"

import (
	"fmt"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

// Disassemble renders bytecode into a human-readable format.
func Disassemble(p *Program) string {
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
