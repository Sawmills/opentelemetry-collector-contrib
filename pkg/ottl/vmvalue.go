// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func valueToVM(val any) (ir.Value, error) {
	switch v := val.(type) {
	case int64:
		return ir.Int64Value(v), nil
	case float64:
		return ir.Float64Value(v), nil
	case bool:
		return ir.BoolValue(v), nil
	case string:
		return ir.StringValue(v), nil
	case []byte:
		return ir.BytesValue(v), nil
	case pcommon.TraceID:
		b := make([]byte, len(v))
		copy(b, v[:])
		return ir.BytesValue(b), nil
	case pcommon.SpanID:
		b := make([]byte, len(v))
		copy(b, v[:])
		return ir.BytesValue(b), nil
	case pcommon.Value:
		switch v.Type() {
		case pcommon.ValueTypeInt:
			return ir.Int64Value(v.Int()), nil
		case pcommon.ValueTypeDouble:
			return ir.Float64Value(v.Double()), nil
		case pcommon.ValueTypeBool:
			return ir.BoolValue(v.Bool()), nil
		case pcommon.ValueTypeStr:
			return ir.StringValue(v.Str()), nil
		case pcommon.ValueTypeBytes:
			return ir.BytesValue(v.Bytes().AsRaw()), nil
		case pcommon.ValueTypeMap:
			return pMapValue(v.Map()), nil
		case pcommon.ValueTypeSlice:
			return pSliceValue(v.Slice()), nil
		default:
			return ir.Value{}, fmt.Errorf("unsupported pcommon.Value type: %v", v.Type())
		}
	case pcommon.Map:
		return pMapValue(v), nil
	case pcommon.Slice:
		return pSliceValue(v), nil
	default:
		return ir.Value{}, fmt.Errorf("unsupported value type: %T", val)
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

type pcommonWrapper struct {
	orig  unsafe.Pointer
	state unsafe.Pointer
}

func VMValueToAny(val ir.Value) (any, error) {
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
