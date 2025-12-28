// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxutil // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxutil"

import (
	"context"
	"fmt"

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
