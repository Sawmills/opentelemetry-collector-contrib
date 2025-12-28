// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ctxutil

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

type literalKeyVM struct {
	s string
}

func (k literalKeyVM) String(context.Context, any) (*string, error) { return &k.s, nil }
func (k literalKeyVM) Int(context.Context, any) (*int64, error)     { return nil, nil }
func (k literalKeyVM) ExpressionGetter(context.Context, any) (ottl.Getter[any], error) {
	return nil, nil
}
func (k literalKeyVM) LiteralString() (string, bool) { return k.s, true }

func TestVMGetterForMapLiteralKey(t *testing.T) {
	m := pcommon.NewMap()
	m.PutInt("foo", 7)
	keys := []ottl.Key[any]{literalKeyVM{s: "foo"}}

	vmGetter, ok := VMGetterForMapLiteralKey(keys, func(any) pcommon.Map {
		return m
	})
	if !ok {
		t.Fatalf("expected vm getter")
	}

	val, err := vmGetter.GetVM(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != ir.Int64Value(7) {
		t.Fatalf("unexpected value: %v", val)
	}
}

func TestVMGetterForMapLiteralKey_Missing(t *testing.T) {
	m := pcommon.NewMap()
	keys := []ottl.Key[any]{literalKeyVM{s: "missing"}}

	vmGetter, ok := VMGetterForMapLiteralKey(keys, func(any) pcommon.Map {
		return m
	})
	if !ok {
		t.Fatalf("expected vm getter")
	}

	_, err := vmGetter.GetVM(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error for missing key")
	}
}
