// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"fmt"

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
		default:
			return ir.Value{}, fmt.Errorf("unsupported pcommon.Value type: %v", v.Type())
		}
	default:
		return ir.Value{}, fmt.Errorf("unsupported value type: %T", val)
	}
}
