// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func TestContains(t *testing.T) {
	type testCase struct {
		name          string
		value         any
		getterErr     error
		patterns      []string
		caseSensitive bool
		want          bool
		expectErr     bool
	}

	emptyStr := ""
	testStr := "this is a test string"
	tests := []testCase{
		{name: "nil value", value: nil, patterns: []string{"test"}, caseSensitive: true, want: false},
		{name: "empty string", value: emptyStr, patterns: []string{"test"}, caseSensitive: true, want: false},
		{name: "empty string with empty pattern", value: emptyStr, patterns: []string{""}, caseSensitive: true, want: true},
		{name: "empty patterns", value: testStr, patterns: []string{}, caseSensitive: true, want: false},
		{name: "single val match", value: testStr, patterns: []string{"test"}, caseSensitive: true, want: true},
		{name: "single val no match", value: testStr, patterns: []string{"xyz"}, caseSensitive: true, want: false},
		{name: "two patterns first match", value: testStr, patterns: []string{"test", "xyz"}, caseSensitive: true, want: true},
		{name: "two patterns second match", value: testStr, patterns: []string{"xyz", "test"}, caseSensitive: true, want: true},
		{name: "two patterns no match", value: testStr, patterns: []string{"xyz", "abc"}, caseSensitive: true, want: false},
		{name: "three patterns first match", value: testStr, patterns: []string{"test", "xyz", "abc"}, caseSensitive: true, want: true},
		{name: "three patterns second match", value: testStr, patterns: []string{"xyz", "test", "abc"}, caseSensitive: true, want: true},
		{name: "three patterns third match", value: testStr, patterns: []string{"xyz", "abc", "test"}, caseSensitive: true, want: true},
		{name: "three patterns no match", value: testStr, patterns: []string{"xyz", "abc", "def"}, caseSensitive: true, want: false},
		{name: "case sensitivity default", value: testStr, patterns: []string{"TEST"}, caseSensitive: true, want: false},
		{name: "partial match at beginning", value: testStr, patterns: []string{"this"}, caseSensitive: true, want: true},
		{name: "partial match in middle", value: testStr, patterns: []string{"is a"}, caseSensitive: true, want: true},
		{name: "partial match at end", value: testStr, patterns: []string{"string"}, caseSensitive: true, want: true},
		{name: "multiple matches", value: testStr, patterns: []string{"this", "test", "string"}, caseSensitive: true, want: true},
		{name: "case insensitive match uppercase pattern", value: testStr, patterns: []string{"TEST"}, caseSensitive: false, want: true},
		{name: "case insensitive match mixed case pattern", value: testStr, patterns: []string{"TeSt"}, caseSensitive: false, want: true},
		{name: "case insensitive no match", value: testStr, patterns: []string{"XYZ"}, caseSensitive: false, want: false},
		{name: "case insensitive multiple patterns", value: testStr, patterns: []string{"ABC", "TEST", "XYZ"}, caseSensitive: false, want: true},
		{name: "case insensitive beginning match", value: testStr, patterns: []string{"THIS"}, caseSensitive: false, want: true},
		{name: "case insensitive middle match", value: testStr, patterns: []string{"IS A"}, caseSensitive: false, want: true},
		{name: "case insensitive end match", value: testStr, patterns: []string{"STRING"}, caseSensitive: false, want: true},
		{name: "uppercase string with lowercase patterns", value: "THIS IS A TEST STRING", patterns: []string{"test"}, caseSensitive: false, want: true},
		{name: "getter error bubbles up", getterErr: errors.New("boom"), patterns: []string{"test"}, caseSensitive: true, expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expressionFunc, err := createContainsFunction[any](
				ottl.FunctionContext{},
				&ContainsArguments[any]{
					Target: &ottl.StandardStringGetter[any]{
						Getter: func(context.Context, any) (any, error) {
							return tt.value, tt.getterErr
						},
					},
					Patterns:      tt.patterns,
					CaseSensitive: tt.caseSensitive,
				},
			)

			require.NoError(t, err)

			result, err := expressionFunc(t.Context(), nil)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestContainsDoesNotMutatePatterns(t *testing.T) {
	patterns := []string{"TEST"}

	_, err := createContainsFunction[any](
		ottl.FunctionContext{},
		&ContainsArguments[any]{
			Target: &ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "test", nil
				},
			},
			Patterns:      patterns,
			CaseSensitive: false,
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"TEST"}, patterns)
}

// TestContainsCaseInsensitiveUnicode pins the SAW-7559 fix's Unicode
// fallback: the new ASCII-only fast path doesn't change matching semantics
// for non-ASCII haystacks (architect catch on PR #75). For any value with
// a byte ≥ 0x80, contains() must fall back to `strings.ToLower(val)` /
// `unicode.ToLower` so case-folding matches the original behavior.
func TestContainsCaseInsensitiveUnicode(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		pattern string
		want    bool
	}{
		// Cyrillic small/capital — Unicode case-folding required.
		{"cyrillic upper haystack lower pattern", "Привет МИР", "мир", true},
		// Greek omega case pair.
		{"greek omega upper → lower", "GREETING Ω", "ω", true},
		// Latin-1 with diacritic — ÷ is not a letter, ensure it doesn't match.
		{"diacritic case", "Ëxample log line", "ëxample", true},
		// ASCII haystack must still match (regression guard for fast path).
		{"ascii haystack case-folded match", "MIXED Body Line", "body", true},
		{"ascii haystack no-match stays false", "MIXED Body Line", "missing", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := contains[any](
				&ottl.StandardStringGetter[any]{
					Getter: func(context.Context, any) (any, error) { return tc.value, nil },
				},
				[]string{tc.pattern},
				false,
			)
			got, err := fn(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestContainsCaseInsensitiveSlowPathConstantAlloc pins the SAW-7559 fix:
// the pool-backed fold path must allocate a *constant* (small, body-size-
// independent) number of objects per call. Before the fix, the slow path
// called `strings.ToLower(val)` which allocated len(val) bytes per record
// × per filter rule — on customer's pipeline that was the dominant heap
// pressure source (178 such calls per record at ~4 kB bodies).
//
// We measure with a small body and a 16 kB body. With the pool the alloc
// count must NOT scale with body size — the only allocations left are
// per-call constants (interface boxes around the bool return + getter
// value), which the test pins below 5.
func TestContainsCaseInsensitiveSlowPathConstantAlloc(t *testing.T) {
	runWith := func(body string) float64 {
		fn := contains(
			&ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) { return body, nil },
			},
			[]string{"xyz-no-such-thing"}, // forces fold-path miss
			false,
		)
		// Warm the pool — first call may allocate the buffer.
		for i := 0; i < 16; i++ {
			_, _ = fn(context.Background(), nil)
		}
		return testing.AllocsPerRun(1000, func() {
			_, _ = fn(context.Background(), nil)
		})
	}

	short := "Some MIXED-Case Body Line"
	long := short + " " + strings.Repeat("Padding ", 2000) // ~16 kB

	shortAllocs := runWith(short)
	longAllocs := runWith(long)

	// Body-size-independent: allocations on the long body must not exceed
	// the short body's count + a tiny constant. The old code would have
	// allocated ~one 16 kB byte slice per call here (a proportional shift
	// in alloc count and bytes).
	require.LessOrEqualf(t, longAllocs, shortAllocs+1,
		"slow-path allocations scaled with body size: %.1f (short) → %.1f (long); pool isn't keeping the buffer reused",
		shortAllocs, longAllocs)
	// Hard ceiling — interface boxing of bool + getter value should be
	// well under 5.
	require.LessOrEqualf(t, longAllocs, 5.0,
		"slow path allocated %.1f objects per run on a 16 kB body — pool is likely missing", longAllocs)
}
