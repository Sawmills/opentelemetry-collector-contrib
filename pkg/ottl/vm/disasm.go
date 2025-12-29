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
	case ir.OpGetSpanKind:
		return "GET_SPAN_KIND"
	case ir.OpSetSpanKind:
		return "SET_SPAN_KIND"
	case ir.OpGetSpanStatus:
		return "GET_SPAN_STATUS"
	case ir.OpSetSpanStatus:
		return "SET_SPAN_STATUS"
	case ir.OpGetMetricName:
		return "GET_METRIC_NAME"
	case ir.OpSetMetricName:
		return "SET_METRIC_NAME"
	case ir.OpGetMetricUnit:
		return "GET_METRIC_UNIT"
	case ir.OpSetMetricUnit:
		return "SET_METRIC_UNIT"
	case ir.OpGetMetricType:
		return "GET_METRIC_TYPE"
	case ir.OpGetSpanStatusMsg:
		return "GET_SPAN_STATUS_MSG"
	case ir.OpSetSpanStatusMsg:
		return "SET_SPAN_STATUS_MSG"
	case ir.OpGetResourceDroppedAttributesCount:
		return "GET_RESOURCE_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpSetResourceDroppedAttributesCount:
		return "SET_RESOURCE_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpGetResourceSchemaURL:
		return "GET_RESOURCE_SCHEMA_URL"
	case ir.OpSetResourceSchemaURL:
		return "SET_RESOURCE_SCHEMA_URL"
	case ir.OpGetScopeName:
		return "GET_SCOPE_NAME"
	case ir.OpSetScopeName:
		return "SET_SCOPE_NAME"
	case ir.OpGetScopeVersion:
		return "GET_SCOPE_VERSION"
	case ir.OpSetScopeVersion:
		return "SET_SCOPE_VERSION"
	case ir.OpGetScopeDroppedAttributesCount:
		return "GET_SCOPE_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpSetScopeDroppedAttributesCount:
		return "SET_SCOPE_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpGetScopeSchemaURL:
		return "GET_SCOPE_SCHEMA_URL"
	case ir.OpSetScopeSchemaURL:
		return "SET_SCOPE_SCHEMA_URL"
	case ir.OpGetObservedTimestamp:
		return "GET_OBSERVED_TIMESTAMP"
	case ir.OpSetObservedTimestamp:
		return "SET_OBSERVED_TIMESTAMP"
	case ir.OpGetSeverityText:
		return "GET_SEVERITY_TEXT"
	case ir.OpSetSeverityText:
		return "SET_SEVERITY_TEXT"
	case ir.OpGetLogFlags:
		return "GET_LOG_FLAGS"
	case ir.OpSetLogFlags:
		return "SET_LOG_FLAGS"
	case ir.OpGetSpanTraceID:
		return "GET_SPAN_TRACE_ID"
	case ir.OpGetSpanID:
		return "GET_SPAN_ID"
	case ir.OpGetSpanParentID:
		return "GET_SPAN_PARENT_ID"
	case ir.OpGetSpanTraceIDString:
		return "GET_SPAN_TRACE_ID_STRING"
	case ir.OpGetSpanIDString:
		return "GET_SPAN_ID_STRING"
	case ir.OpGetSpanParentIDString:
		return "GET_SPAN_PARENT_ID_STRING"
	case ir.OpGetSpanTraceState:
		return "GET_SPAN_TRACE_STATE"
	case ir.OpSetSpanTraceState:
		return "SET_SPAN_TRACE_STATE"
	case ir.OpGetSpanTraceStateKey:
		return "GET_SPAN_TRACE_STATE_KEY"
	case ir.OpSetSpanTraceStateKey:
		return "SET_SPAN_TRACE_STATE_KEY"
	case ir.OpGetSpanDroppedAttributesCount:
		return "GET_SPAN_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpSetSpanDroppedAttributesCount:
		return "SET_SPAN_DROPPED_ATTRIBUTES_COUNT"
	case ir.OpGetSpanDroppedEventsCount:
		return "GET_SPAN_DROPPED_EVENTS_COUNT"
	case ir.OpSetSpanDroppedEventsCount:
		return "SET_SPAN_DROPPED_EVENTS_COUNT"
	case ir.OpGetSpanDroppedLinksCount:
		return "GET_SPAN_DROPPED_LINKS_COUNT"
	case ir.OpSetSpanDroppedLinksCount:
		return "SET_SPAN_DROPPED_LINKS_COUNT"
	case ir.OpGetMetricDescription:
		return "GET_METRIC_DESCRIPTION"
	case ir.OpSetMetricDescription:
		return "SET_METRIC_DESCRIPTION"
	case ir.OpGetMetricAggTemporality:
		return "GET_METRIC_AGG_TEMPORALITY"
	case ir.OpSetMetricAggTemporality:
		return "SET_METRIC_AGG_TEMPORALITY"
	case ir.OpGetMetricIsMonotonic:
		return "GET_METRIC_IS_MONOTONIC"
	case ir.OpSetMetricIsMonotonic:
		return "SET_METRIC_IS_MONOTONIC"
	case ir.OpInt:
		return "INT"
	case ir.OpIsNil:
		return "IS_NIL"
	case ir.OpIsType:
		return "IS_TYPE"
	case ir.OpIsMatch:
		return "IS_MATCH"
	case ir.OpIsMatchDynamic:
		return "IS_MATCH_DYNAMIC"
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
	case ir.TypeNone:
		return "nil"
	default:
		return "<?>"
	}
}

func mathFloat(val ir.Value) float64 {
	f, _ := val.Float64()
	return f
}
