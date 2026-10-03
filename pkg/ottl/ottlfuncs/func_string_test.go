// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
)

func Test_String(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected any
		err      bool
	}{
		{
			name:     "string",
			value:    "test",
			expected: string("test"),
		},
		{
			name:     "empty string",
			value:    "",
			expected: string(""),
		},
		{
			name:     "a number string",
			value:    "333",
			expected: string("333"),
		},
		{
			name:     "int64",
			value:    int64(333),
			expected: string("333"),
		},
		{
			name:     "float64",
			value:    float64(2.7),
			expected: string("2.7"),
		},
		{
			name:     "float64 without decimal",
			value:    float64(55),
			expected: string("55"),
		},
		{
			name:     "true",
			value:    true,
			expected: string("true"),
		},
		{
			name:     "false",
			value:    false,
			expected: string("false"),
		},
		{
			name:     "nil",
			value:    nil,
			expected: nil,
		},
		{
			name:     "byte",
			value:    []byte{123},
			expected: string("7b"),
		},
		{
			name:     "map",
			value:    map[int]bool{1: true},
			expected: string("{\"1\":true}"),
		},
		{
			name:     "slice",
			value:    []int{1, 2, 3},
			expected: string("[1,2,3]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprFunc := stringFunc(&ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return tt.value, nil
				},
			})
			result, err := exprFunc(nil, nil)
			if tt.err {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}

// The input is already an interface value, as returned by an OTTL path getter.
func TestStringPreservesBoxedStringWithoutAllocations(t *testing.T) {
	var input any = "already a string"
	fn := stringFunc(ottl.StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) { return input, nil }})
	var got any
	allocations := testing.AllocsPerRun(100, func() { got, _ = fn(t.Context(), nil) })
	require.Equal(t, input, got)
	require.Zero(t, allocations)
}

type stringTestCustomGetter struct {
	value *string
	err   error
	calls int
}

func (g *stringTestCustomGetter) Get(context.Context, any) (*string, error) {
	g.calls++
	return g.value, g.err
}

func TestStringCustomGetter(t *testing.T) {
	value := "custom conversion"
	sentinel := errors.New("getter failed")
	for _, tc := range []struct {
		value *string
		err   error
	}{{&value, nil}, {nil, nil}, {nil, sentinel}} {
		getter := &stringTestCustomGetter{value: tc.value, err: tc.err}
		got, err := stringFunc[any](getter)(t.Context(), nil)
		require.ErrorIs(t, err, tc.err)
		require.Equal(t, 1, getter.calls)
		if tc.value == nil {
			require.Nil(t, got)
		} else {
			require.Equal(t, *tc.value, got)
		}
	}
}

func TestStringGetterErrorAndCallCount(t *testing.T) {
	sentinel := errors.New("getter failed")
	calls := 0
	getter := ottl.StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) { calls++; return nil, sentinel }}
	_, want := getter.Get(t.Context(), nil)
	calls = 0
	got, err := stringFunc[any](getter)(t.Context(), nil)
	require.Nil(t, got)
	require.EqualError(t, err, want.Error())
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, 1, calls)
}

func BenchmarkStringValue(b *testing.B) {
	for _, value := range []any{"scanner completed", int64(333), nil, []byte{123}} {
		b.Run(fmt.Sprintf("%T", value), func(b *testing.B) {
			fn := stringFunc(ottl.StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) { return value, nil }})
			b.ReportAllocs()
			for b.Loop() {
				_, err := fn(b.Context(), nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestStringStandardConversionParity(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("key", "value")
	sl := pcommon.NewSlice()
	sl.AppendEmpty().SetStr("item")
	var ptr *int
	values := []any{m, sl, pcommon.NewValueStr("pdata"), pcommon.NewValueInt(42), pcommon.NewValueEmpty(), ptr, make(chan int), map[string]any{"invalid": make(chan int)}}
	for _, value := range values {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			calls := 0
			getter := ottl.StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) { calls++; return value, nil }}
			expected, wantErr := getter.Get(t.Context(), nil)
			calls = 0
			got, err := stringFunc[any](getter)(t.Context(), nil)
			require.Equal(t, 1, calls)
			if wantErr != nil {
				require.EqualError(t, err, wantErr.Error())
				var wantType, gotType ottl.TypeError
				require.Equal(t, errors.As(wantErr, &wantType), errors.As(err, &gotType))
				return
			}
			require.NoError(t, err)
			if expected == nil {
				require.Nil(t, got)
			} else {
				require.Equal(t, *expected, got)
			}
		})
	}
}

type stringEmbeddedGetter struct {
	ottl.StandardStringLikeGetter[any]
}

func (stringEmbeddedGetter) Get(context.Context, any) (*string, error) {
	value := "overridden"
	return &value, nil
}

func TestStringEmbeddedGetterOverride(t *testing.T) {
	getter := stringEmbeddedGetter{ottl.StandardStringLikeGetter[any]{Getter: func(context.Context, any) (any, error) {
		t.Fatal("underlying getter bypassed override")
		return nil, nil
	}}}
	got, err := stringFunc[any](getter)(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, "overridden", got)
}

func BenchmarkStringLogAttribute(b *testing.B) {
	parser, err := ottllog.NewParser(StandardConverters[*ottllog.TransformContext](), componenttest.NewNopTelemetrySettings())
	require.NoError(b, err)
	condition, err := parser.ParseCondition(`String(attributes["status"]) == "completed"`)
	require.NoError(b, err)
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("status", "completed")
	tc := ottllog.NewTransformContextPtr(rl, sl, lr)
	defer tc.Close()
	b.ReportAllocs()
	for b.Loop() {
		got, err := condition.Eval(b.Context(), tc)
		if err != nil || !got {
			b.Fatalf("got %v: %v", got, err)
		}
	}
}

func TestStringPointerGetterUpdate(t *testing.T) {
	getter := &ottl.StandardStringLikeGetter[any]{}
	fn := stringFunc[any](getter)
	getter.Getter = func(context.Context, any) (any, error) { return "first", nil }
	got, err := fn(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, "first", got)
	getter.Getter = func(context.Context, any) (any, error) { return "second", nil }
	got, err = fn(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, "second", got)
}
