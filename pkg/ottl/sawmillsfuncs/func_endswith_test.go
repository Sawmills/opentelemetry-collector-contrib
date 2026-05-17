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

func TestEndsWith(t *testing.T) {
	type testCase struct {
		name          string
		value         any
		suffixes      []string
		caseSensitive bool
		want          bool
		expectErr     bool
	}

	emptyStr := ""
	testStr := "this is a test string"
	tests := []testCase{
		{name: "nil value", value: nil, suffixes: []string{"string"}, caseSensitive: true, want: false},
		{name: "empty string", value: emptyStr, suffixes: []string{"string"}, caseSensitive: true, want: false},
		{name: "empty string with empty suffix", value: emptyStr, suffixes: []string{""}, caseSensitive: true, want: true},
		{name: "empty suffixes", value: testStr, suffixes: []string{}, caseSensitive: true, want: false},
		{name: "single val match", value: testStr, suffixes: []string{"string"}, caseSensitive: true, want: true},
		{name: "single val no match", value: testStr, suffixes: []string{"xyz"}, caseSensitive: true, want: false},
		{name: "two suffixes first match", value: testStr, suffixes: []string{"string", "xyz"}, caseSensitive: true, want: true},
		{name: "two suffixes second match", value: testStr, suffixes: []string{"xyz", "string"}, caseSensitive: true, want: true},
		{name: "two suffixes no match", value: testStr, suffixes: []string{"xyz", "abc"}, caseSensitive: true, want: false},
		{name: "case sensitivity default", value: testStr, suffixes: []string{"STRING"}, caseSensitive: true, want: false},
		{name: "partial match", value: testStr, suffixes: []string{"ing"}, caseSensitive: true, want: true},
		{name: "suffix longer than string", value: testStr, suffixes: []string{"extra words this is a test string"}, caseSensitive: true, want: false},
		{name: "not a suffix", value: testStr, suffixes: []string{"this"}, caseSensitive: true, want: false},
		{name: "case insensitive match uppercase suffix", value: testStr, suffixes: []string{"STRING"}, caseSensitive: false, want: true},
		{name: "case insensitive match mixed case suffix", value: testStr, suffixes: []string{"StRiNg"}, caseSensitive: false, want: true},
		{name: "case insensitive no match", value: testStr, suffixes: []string{"XYZ"}, caseSensitive: false, want: false},
		{name: "case insensitive multiple suffixes", value: testStr, suffixes: []string{"abc", "STRING", "xyz"}, caseSensitive: false, want: true},
		{name: "case insensitive partial match", value: testStr, suffixes: []string{"ING"}, caseSensitive: false, want: true},
		{name: "uppercase string with lowercase suffix", value: "THIS IS A TEST STRING", suffixes: []string{"string"}, caseSensitive: false, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expressionFunc, err := createEndsWithFunction[any](
				ottl.FunctionContext{},
				&EndsWithArguments[any]{
					Target: &ottl.StandardStringGetter[any]{
						Getter: func(context.Context, any) (any, error) {
							return tt.value, nil
						},
					},
					Suffixes:      tt.suffixes,
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

func TestEndsWithDoesNotMutateSuffixes(t *testing.T) {
	suffixes := []string{"STRING"}

	_, err := createEndsWithFunction[any](
		ottl.FunctionContext{},
		&EndsWithArguments[any]{
			Target: &ottl.StandardStringGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "string", nil
				},
			},
			Suffixes:      suffixes,
			CaseSensitive: false,
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"STRING"}, suffixes)
}

func TestEndsWithCaseInsensitiveUnicode(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		suffix string
		want   bool
	}{
		{"cyrillic upper haystack lower suffix", "hello МИР", "мир", true},
		{"greek omega upper lower suffix", "hello Ω", "ω", true},
		{"diacritic case", "log linË", "linë", true},
		{"ascii haystack case-folded match", "MIXED Body Line", "line", true},
		{"ascii haystack no-match stays false", "MIXED Body Line", "body", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := endsWith[any](
				&ottl.StandardStringGetter[any]{
					Getter: func(context.Context, any) (any, error) { return tc.value, nil },
				},
				[]string{tc.suffix},
				false,
			)
			got, err := fn(t.Context(), nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEndsWithCaseInsensitiveASCIIFoldDoesNotAllocateByBodySize(t *testing.T) {
	run := func(body string) (allocsPerOp, bytesPerOp float64) {
		fn := endsWith(
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
		"endsWith allocations scaled with body size: %.1f short -> %.1f long", shortAllocs, longAllocs)
	require.LessOrEqualf(t, longAllocs, 5.0,
		"endsWith allocated %.1f objects per run on a 16 kB body", longAllocs)
	require.LessOrEqualf(t, longBytes, shortBytes+512,
		"endsWith bytes/op scaled with body size: %.1f short -> %.1f long", shortBytes, longBytes)
}

func TestEndsWithCaseInsensitiveSuffixWindowDoesNotAllocateByBodySize(t *testing.T) {
	run := func(body string) (allocsPerOp, bytesPerOp float64) {
		fn := endsWith(
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

	short := "Привет Some MIXED-Case Body Line"
	long := "Привет " + strings.Repeat("Padding ", 2000) + "Some MIXED-Case Body Line"

	shortAllocs, shortBytes := run(short)
	longAllocs, longBytes := run(long)

	require.LessOrEqualf(t, longAllocs, shortAllocs+1,
		"endsWith allocations scaled with body size: %.1f short -> %.1f long", shortAllocs, longAllocs)
	require.LessOrEqualf(t, longAllocs, 5.0,
		"endsWith allocated %.1f objects per run on a body with non-ASCII outside the suffix window", longAllocs)
	require.LessOrEqualf(t, longBytes, shortBytes+512,
		"endsWith bytes/op scaled with body size when non-ASCII was outside the suffix window: %.1f short -> %.1f long", shortBytes, longBytes)
}
