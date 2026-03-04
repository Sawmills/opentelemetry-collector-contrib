// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxutil // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxutil"

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

// VMGetterForMapLiteralKey returns a VM getter when the path is a single literal string key.
func VMGetterForMapLiteralKey[K any](keys []ottl.Key[K], getMap func(K) pcommon.Map) (ottl.VMGetterFunc[K], bool) {
	if len(keys) != 1 {
		return nil, false
	}
	literal, ok := keys[0].(ottl.LiteralStringKey)
	if !ok {
		return nil, false
	}
	key, ok := literal.LiteralString()
	if !ok {
		return nil, false
	}
	return func(_ context.Context, tCtx K) (ir.Value, error) {
		val, ok := getMap(tCtx).Get(key)
		if !ok {
			return ir.Value{}, fmt.Errorf("unsupported value type: %T", nil)
		}
		return valueToVM(val)
	}, true
}

func valueToVM(val pcommon.Value) (ir.Value, error) {
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
		return ir.Value{}, fmt.Errorf("unsupported pcommon.Value type: %v", val.Type())
	}
}

// ValueToVM converts a pcommon.Value to an ir.Value for VM fast paths.
func ValueToVM(val pcommon.Value) (ir.Value, error) {
	return valueToVM(val)
}

// SetMapValueFromVM writes a VM value into a pdata map for the given key.
func SetMapValueFromVM(m pcommon.Map, key string, val ir.Value) error {
	switch val.Type {
	case ir.TypeInt:
		m.PutInt(key, int64(val.Num))
		return nil
	case ir.TypeFloat:
		m.PutDouble(key, math.Float64frombits(val.Num))
		return nil
	case ir.TypeBool:
		m.PutBool(key, val.Num != 0)
		return nil
	case ir.TypeString:
		s, ok := val.String()
		if !ok {
			return fmt.Errorf("invalid string value")
		}
		m.PutStr(key, s)
		return nil
	case ir.TypeBytes:
		bytesVal, ok := val.Bytes()
		if !ok {
			return fmt.Errorf("invalid bytes value")
		}
		m.PutEmptyBytes(key).FromRaw(bytesVal)
		return nil
	case ir.TypePMap:
		pm, ok := pMapFromValue(val)
		if !ok {
			return fmt.Errorf("invalid map value")
		}
		pm.CopyTo(m.PutEmptyMap(key))
		return nil
	case ir.TypePSlice:
		ps, ok := pSliceFromValue(val)
		if !ok {
			return fmt.Errorf("invalid slice value")
		}
		ps.CopyTo(m.PutEmptySlice(key))
		return nil
	default:
		return fmt.Errorf("unsupported VM value type: %v", val.Type)
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
