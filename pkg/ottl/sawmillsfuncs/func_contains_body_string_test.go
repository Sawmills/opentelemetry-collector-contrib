// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
)

func parseBodyStringCondition(tb testing.TB, path, pattern string) *ottl.Condition[*ottllog.TransformContext] {
	tb.Helper()
	parser, err := ottllog.NewParser(map[string]ottl.Factory[*ottllog.TransformContext]{"Contains": NewContainsFactory[*ottllog.TransformContext]()}, componenttest.NewNopTelemetrySettings())
	require.NoError(tb, err)
	condition, err := parser.ParseCondition(fmt.Sprintf("Contains(%s, [%q], true)", path, pattern))
	require.NoError(tb, err)
	return condition
}

func TestContainsBodyStringValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    any
		pattern string
		want    bool
	}{
		{name: "string", body: "hello", pattern: "ell", want: true},
		{name: "absent substring", body: "hello", pattern: "bye"},
		{name: "empty string", body: "", pattern: "", want: true},
		{name: "integer", body: int64(123), pattern: "123", want: true},
		{name: "double", body: 123.5, pattern: "123.5", want: true},
		{name: "boolean", body: true, pattern: "true", want: true},
		{name: "map", body: map[string]any{"key": "value"}, pattern: "value", want: true},
		{name: "slice", body: []any{"value"}, pattern: "value", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			context, _, _ := newContainsLogContext("")
			defer context.Close()
			require.NoError(t, context.GetLogRecord().Body().FromRaw(tc.body))
			condition := parseBodyStringCondition(t, "body.string", tc.pattern)

			got, err := condition.Eval(t.Context(), context)

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

var bodyStringConditionResult bool

func BenchmarkContainsBodyStringGetter(b *testing.B) {
	for _, path := range []string{"body.string", "body", `attributes["value"]`} {
		b.Run(path, func(b *testing.B) {
			condition := parseBodyStringCondition(b, path, "missing")
			logs := plog.NewLogs()
			rl := logs.ResourceLogs().AppendEmpty()
			sl := rl.ScopeLogs().AppendEmpty()
			lr := sl.LogRecords().AppendEmpty()
			value := strings.Repeat("x", 1024)
			lr.Body().SetStr(value)
			lr.Attributes().PutStr("value", value)
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				tc := ottllog.NewTransformContextPtr(rl, sl, lr)
				var err error
				bodyStringConditionResult, err = condition.Eval(ctx, tc)
				tc.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
