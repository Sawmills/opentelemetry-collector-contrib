// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func TestStartsWith(t *testing.T) {
	type testCase struct {
		name          string
		value         any
		prefixes      []string
		caseSensitive bool
		want          bool
		expectErr     bool
	}

	emptyStr := ""
	testStr := "this is a test string"
	tests := []testCase{
		{name: "nil value", value: nil, prefixes: []string{"this"}, caseSensitive: true, want: false},
		{name: "empty string", value: emptyStr, prefixes: []string{"this"}, caseSensitive: true, want: false},
		{name: "empty string with empty prefix", value: emptyStr, prefixes: []string{""}, caseSensitive: true, want: true},
		{name: "empty prefixes", value: testStr, prefixes: []string{}, caseSensitive: true, want: false},
		{name: "single val match", value: testStr, prefixes: []string{"this"}, caseSensitive: true, want: true},
		{name: "single val no match", value: testStr, prefixes: []string{"xyz"}, caseSensitive: true, want: false},
		{name: "two prefixes first match", value: testStr, prefixes: []string{"this", "xyz"}, caseSensitive: true, want: true},
		{name: "two prefixes second match", value: testStr, prefixes: []string{"xyz", "this"}, caseSensitive: true, want: true},
		{name: "two prefixes no match", value: testStr, prefixes: []string{"xyz", "abc"}, caseSensitive: true, want: false},
		{name: "case sensitivity default", value: testStr, prefixes: []string{"THIS"}, caseSensitive: true, want: false},
		{name: "partial match", value: testStr, prefixes: []string{"th"}, caseSensitive: true, want: true},
		{name: "prefix longer than string", value: testStr, prefixes: []string{"this is a test string with extra words"}, caseSensitive: true, want: false},
		{name: "not a prefix", value: testStr, prefixes: []string{"test"}, caseSensitive: true, want: false},
		{name: "case insensitive match uppercase prefix", value: testStr, prefixes: []string{"THIS"}, caseSensitive: false, want: true},
		{name: "case insensitive match mixed case prefix", value: testStr, prefixes: []string{"ThIs"}, caseSensitive: false, want: true},
		{name: "case insensitive no match", value: testStr, prefixes: []string{"XYZ"}, caseSensitive: false, want: false},
		{name: "case insensitive multiple prefixes", value: testStr, prefixes: []string{"abc", "THIS", "xyz"}, caseSensitive: false, want: true},
		{name: "case insensitive partial match", value: testStr, prefixes: []string{"TH"}, caseSensitive: false, want: true},
		{name: "uppercase string with lowercase prefix", value: "THIS IS A TEST STRING", prefixes: []string{"this"}, caseSensitive: false, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expressionFunc, err := createStartsWithFunction[any](
				ottl.FunctionContext{},
				&StartsWithArguments[any]{
					Target: &ottl.StandardStringGetter[any]{
						Getter: func(context.Context, any) (any, error) {
							return tt.value, nil
						},
					},
					Prefixes:      tt.prefixes,
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

func TestStartsWithDoesNotMutatePrefixes(t *testing.T) {
	prefixes := []string{"THIS"}

	_, err := createStartsWithFunction[any](
		ottl.FunctionContext{},
		&StartsWithArguments[any]{
			Target: &ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "this", nil
				},
			},
			Prefixes:      prefixes,
			CaseSensitive: false,
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"THIS"}, prefixes)
}

func TestStartsWithCaseInsensitiveUnicode(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		prefix string
		want   bool
	}{
		{"cyrillic upper haystack lower prefix", "Привет мир", "привет", true},
		{"greek omega upper lower prefix", "Ωmega", "ω", true},
		{"diacritic case", "Ëxample log line", "ëxample", true},
		{"unicode fallback for ascii prefix", "\u212aelvin log line", "kelvin", true},
		{"ascii haystack case-folded match", "MIXED Body Line", "mixed", true},
		{"ascii haystack no-match stays false", "MIXED Body Line", "body", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := startsWith[any](
				&ottl.StandardStringGetter[any]{
					Getter: func(context.Context, any) (any, error) { return tc.value, nil },
				},
				[]string{tc.prefix},
				false,
			)
			got, err := fn(t.Context(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStartsWithCaseInsensitiveMixedASCIIPrefixes(t *testing.T) {
	fn := startsWith[any](
		&ottl.StandardStringGetter[any]{
			Getter: func(context.Context, any) (any, error) { return "MIXED Body Line", nil },
		},
		[]string{"ω", "mixed"},
		false,
	)
	got, err := fn(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, true, got)
}

func TestStartsWithCaseInsensitiveASCIIFoldDoesNotAllocateByBodySize(t *testing.T) {
	run := func(body string) (allocsPerOp, bytesPerOp float64) {
		fn := startsWith(
			&ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) { return body, nil },
			},
			[]string{"xyz-no-such-thing"},
			false,
		)
		br := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, _ = fn(t.Context(), nil)
			}
		})
		return float64(br.AllocsPerOp()), float64(br.AllocedBytesPerOp())
	}

	short := "Some MIXED-Case Body Line"
	long := short + " " + strings.Repeat("Padding ", 2000)

	shortAllocs, shortBytes := run(short)
	longAllocs, longBytes := run(long)

	require.LessOrEqualf(t, longAllocs, shortAllocs+1,
		"startsWith allocations scaled with body size: %.1f short -> %.1f long", shortAllocs, longAllocs)
	require.LessOrEqualf(t, longAllocs, 5.0,
		"startsWith allocated %.1f objects per run on a 16 kB body", longAllocs)
	require.LessOrEqualf(t, longBytes, shortBytes+512,
		"startsWith bytes/op scaled with body size: %.1f short -> %.1f long", shortBytes, longBytes)
}
