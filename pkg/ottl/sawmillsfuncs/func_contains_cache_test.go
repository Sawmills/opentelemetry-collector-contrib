// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
)

func newContainsLogContext(value string) (*ottllog.TransformContext, ottl.StringGetter[*ottllog.TransformContext], plog.Logs) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr(value)
	tCtx := ottllog.NewTransformContextPtr(rl, sl, lr)
	getter := ottl.StandardStringGetter[*ottllog.TransformContext]{Getter: func(_ context.Context, tc *ottllog.TransformContext) (any, error) {
		return tc.GetLogRecord().Body().Str(), nil
	}}
	return tCtx, getter, logs
}

func requireContainsResult(t *testing.T, fn ottl.ExprFunc[*ottllog.TransformContext], tc *ottllog.TransformContext, want bool) {
	t.Helper()
	got, err := fn(t.Context(), tc)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestContainsUnicodeAfterBodyChanges(t *testing.T) {
	tc, getter, _ := newContainsLogContext("Ω")
	defer tc.Close()
	fn := contains(getter, []string{"ω"}, false)
	requireContainsResult(t, fn, tc, true)
	tc.GetLogRecord().Body().SetStr("Β")
	requireContainsResult(t, fn, tc, false)
	tc.GetLogRecord().Body().SetStr("Ω")
	requireContainsResult(t, fn, tc, true)
}

func TestContainsUnicodeAcrossDifferentPatterns(t *testing.T) {
	tc, getter, _ := newContainsLogContext("GREETING Ω")
	defer tc.Close()
	omega := contains(getter, []string{"ω"}, false)
	beta := contains(getter, []string{"β"}, false)
	requireContainsResult(t, omega, tc, true)
	requireContainsResult(t, beta, tc, false)
	requireContainsResult(t, omega, tc, true)
}

func TestContainsUnicodeAfterASCIIUppercase(t *testing.T) {
	tc, getter, _ := newContainsLogContext("Service=" + strings.Repeat("x", 1024) + "Ω")
	defer tc.Close()
	fn := contains(getter, []string{"ω"}, false)
	requireContainsResult(t, fn, tc, true)
	requireContainsResult(t, fn, tc, true)
}

func TestContainsUnicodePreservesInvalidUTF8Conversion(t *testing.T) {
	tc, getter, _ := newContainsLogContext("Service=\xff")
	defer tc.Close()
	fn := contains(getter, []string{"\ufffd"}, false)
	requireContainsResult(t, fn, tc, true)
	requireContainsResult(t, fn, tc, true)
}

func TestContainsUnicodePreservesNilLogContext(t *testing.T) {
	getter := ottl.StandardStringGetter[*ottllog.TransformContext]{Getter: func(context.Context, *ottllog.TransformContext) (any, error) { return "Ω", nil }}
	fn := contains(getter, []string{"ω"}, false)
	requireContainsResult(t, fn, nil, true)
}

func BenchmarkContainsUnicodePerRecord(b *testing.B) {
	for _, sample := range []struct {
		name  string
		value string
	}{
		{"unicode", strings.Repeat("level=info status=ok ", 204) + "Ω"},
		{"late_unicode", "Service=payments " + strings.Repeat("level=info status=ok ", 204) + "Ω"},
		{"ascii", "Service=payments " + strings.Repeat("level=info status=ok ", 204)},
	} {
		b.Run(sample.name, func(b *testing.B) {
			for _, count := range []int{1, 16} {
				name := "one"
				if count == 16 {
					name = "sixteen"
				}
				b.Run(name, func(b *testing.B) {
					tc, getter, logs := newContainsLogContext(sample.value)
					rl := logs.ResourceLogs().At(0)
					sl := rl.ScopeLogs().At(0)
					lr := sl.LogRecords().At(0)
					tc.Close()
					fn := contains(getter, []string{"not-present"}, false)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tc = ottllog.NewTransformContextPtr(rl, sl, lr)
						for range count {
							_, _ = fn(b.Context(), tc)
						}
						tc.Close()
					}
				})
			}
		})
	}
}
