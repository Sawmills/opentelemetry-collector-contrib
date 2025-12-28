// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxutil // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxutil"

import (
	"context"
	"fmt"
	"math"

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
	default:
		return ir.Value{}, fmt.Errorf("unsupported pcommon.Value type: %v", val.Type())
	}
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
	default:
		return fmt.Errorf("unsupported VM value type: %v", val.Type)
	}
}
