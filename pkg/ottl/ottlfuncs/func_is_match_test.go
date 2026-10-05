// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	ottlregexp "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/regexp"
)

func TestIsMatchLiteralParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		target  any
	}{
		{"required literal miss", `scanner_log\s+-`, "a long scanner message without the marker"},
		{"required literal match", `scanner_log\s+-`, "scanner_log - message"},
		{"leading wildcard miss", `.*ecs_scaler`, "a long ordinary service log"},
		{"leading wildcard match", `.*ecs_scaler`, "service ecs_scaler"},
		{"leading wildcard newline", `.*ecs_scaler`, "first line\necs_scaler"},
		{"leading wildcard invalid utf8", `.*ecs_scaler`, "\xffecs_scaler"},
		{"trailing wildcard", `ecs_scaler.*`, "ecs_scaler\nnext line"},
		{"wrapped wildcard", `.*ecs_scaler.*`, "first line\necs_scaler\nlast line"},
		{"long literal go-re2", strings.Repeat("x", 210), strings.Repeat("x", 210)},
		{"long wildcard go-re2 invalid utf8", `.*` + strings.Repeat("x", 210), "\xff" + strings.Repeat("x", 210)},
		{"anchored miss", `^scanner_log\s+-$`, "prefix scanner_log -"},
		{"anchored match", `^scanner_log\s+-$`, "scanner_log -"},
		{"case folded", `(?i)health`, "HEALTH"},
		{"folded character class", `[Aa]bc`, "Abc"},
		{"dot all", `(?s).*scanner_log.*`, "line\nscanner_log\nline"},
		{"multiline", `(?m)^scanner_log`, "line\nscanner_log"},
		{"alternation", `scanner_log|ecs_scaler`, "ecs_scaler"},
		{"one or more prefix", `.+ecs_scaler`, "ecs_scaler"},
		{"repeated literal", `ecs{2}`, "ecs"},
		{"unicode", `café\s+-`, "café -"},
		{"invalid utf8", `�\s+-`, "\xff -"},
		{"escaped replacement rune", `\x{FFFD}\s+-`, "\xff -"},
		{"replacement rune after wildcard", `.*�`, "\xff"},
		{"empty target", `scanner_log`, ""},
		{"nil target", `scanner_log`, nil},
		{"non-string target", `123\s+-`, 123},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := ottl.NewParser[any](
				StandardConverters[any](),
				func(path ottl.Path[any]) (ottl.GetSetter[any], error) {
					require.Equal(t, "body", path.Name())
					return &ottl.StandardGetSetter[any]{
						Getter: func(context.Context, any) (any, error) { return tt.target, nil },
						Setter: func(context.Context, any, any) error { return nil },
					}, nil
				},
				componenttest.NewNopTelemetrySettings(),
			)
			require.NoError(t, err)
			expr, err := parser.ParseValueExpression(fmt.Sprintf("IsMatch(body, %q)", tt.pattern))
			require.NoError(t, err)
			got, err := expr.Eval(t.Context(), nil)
			require.NoError(t, err)

			matcher, err := ottlregexp.Compile(tt.pattern)
			require.NoError(t, err)
			var want bool
			if tt.target != nil {
				target := fmt.Sprint(tt.target)
				want = matcher.MatchString(target)
			}
			require.Equal(t, want, got)
		})
	}
}

func TestIsMatchDynamicPatternParity(t *testing.T) {
	t.Parallel()
	pattern := `.*ecs_scaler`
	parser, err := ottl.NewParser[any](
		StandardConverters[any](),
		func(path ottl.Path[any]) (ottl.GetSetter[any], error) {
			var value string
			switch path.Name() {
			case "body":
				value = "service ecs_scaler"
			case "pattern":
				value = pattern
			default:
				t.Fatalf("unexpected path %q", path.Name())
			}
			return &ottl.StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) { return value, nil },
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		componenttest.NewNopTelemetrySettings(),
	)
	require.NoError(t, err)
	expr, err := parser.ParseValueExpression("IsMatch(body, pattern)")
	require.NoError(t, err)
	got, err := expr.Eval(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, true, got)
}

func TestIsMatchPublicErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		expression  string
		pattern     string
		targetError bool
		parseError  bool
		errorText   string
	}{
		{"invalid literal pattern", `IsMatch(body, "\\K")`, "", false, true, "not a valid pattern"},
		{"invalid dynamic pattern", "IsMatch(body, pattern)", `\K`, false, false, "invalid escape sequence"},
		{"target getter failure", `IsMatch(body, ".*ecs_scaler")`, "", true, false, "target failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			targetCalls := 0
			parser, err := ottl.NewParser[any](
				StandardConverters[any](),
				func(path ottl.Path[any]) (ottl.GetSetter[any], error) {
					return &ottl.StandardGetSetter[any]{
						Getter: func(context.Context, any) (any, error) {
							switch path.Name() {
							case "pattern":
								return tt.pattern, nil
							case "body":
								targetCalls++
								if tt.targetError {
									return nil, errors.New("target failed")
								}
								return "service ecs_scaler", nil
							default:
								return nil, fmt.Errorf("unexpected path %q", path.Name())
							}
						},
						Setter: func(context.Context, any, any) error { return nil },
					}, nil
				},
				componenttest.NewNopTelemetrySettings(),
			)
			require.NoError(t, err)
			expr, err := parser.ParseValueExpression(tt.expression)
			if tt.parseError {
				require.ErrorContains(t, err, tt.errorText)
				require.Zero(t, targetCalls)
				return
			}
			require.NoError(t, err)
			result, err := expr.Eval(t.Context(), nil)
			require.Nil(t, result)
			require.ErrorContains(t, err, tt.errorText)
			if tt.targetError {
				require.Equal(t, 1, targetCalls)
			} else {
				require.Zero(t, targetCalls)
			}
		})
	}
}

func BenchmarkIsMatchLiteral(b *testing.B) {
	tests := []struct {
		name    string
		pattern string
		target  string
	}{
		{"substring_miss", `.*ecs_scaler`, strings.Repeat("ordinary message ", 16)},
		{"substring_match", `.*ecs_scaler`, strings.Repeat("ordinary message ", 16) + "ecs_scaler"},
		{"miss", `scanner_log\s+-`, strings.Repeat("ordinary message ", 16)},
		{"match", `scanner_log\s+-`, strings.Repeat("ordinary message ", 16) + "scanner_log -"},
		{"folded_control", `(?i)scanner_log\s+-`, strings.Repeat("ordinary message ", 16)},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			parser, err := ottl.NewParser[any](
				StandardConverters[any](),
				func(ottl.Path[any]) (ottl.GetSetter[any], error) {
					return &ottl.StandardGetSetter[any]{
						Getter: func(context.Context, any) (any, error) { return tt.target, nil },
						Setter: func(context.Context, any, any) error { return nil },
					}, nil
				},
				componenttest.NewNopTelemetrySettings(),
			)
			if err != nil {
				b.Fatal(err)
			}
			expr, err := parser.ParseValueExpression(fmt.Sprintf("IsMatch(body, %q)", tt.pattern))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := expr.Eval(b.Context(), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_isMatch(t *testing.T) {
	tests := []struct {
		name     string
		target   ottl.StringLikeGetter[any]
		pattern  ottl.StringGetter[any]
		expected bool
	}{
		{
			name: "replace match true",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "hello world", nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return "hello.*", nil
				},
			},
			expected: true,
		},
		{
			name: "replace match false",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "goodbye world", nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return "hello.*", nil
				},
			},
			expected: false,
		},
		{
			name: "replace match complex",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return "-12.001", nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return "[-+]?\\d*\\.\\d+([eE][-+]?\\d+)?", nil
				},
			},
			expected: true,
		},
		{
			name: "target bool",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return true, nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return "true", nil
				},
			},
			expected: true,
		},
		{
			name: "target int",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return int64(1), nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return `\d`, nil
				},
			},
			expected: true,
		},
		{
			name: "target float",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return 1.1, nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return `\d\.\d`, nil
				},
			},
			expected: true,
		},
		{
			name: "target pcommon.Value",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					v := pcommon.NewValueEmpty()
					v.SetStr("test")
					return v, nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return `test`, nil
				},
			},
			expected: true,
		},
		{
			name: "nil target",
			target: &ottl.StandardStringLikeGetter[any]{
				Getter: func(context.Context, any) (any, error) {
					return nil, nil
				},
			},
			pattern: &ottl.StandardStringGetter[any]{
				Getter: func(_ context.Context, _ any) (any, error) {
					return "impossible to match", nil
				},
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprFunc, err := isMatch(tt.target, tt.pattern)
			require.NoError(t, err)
			result, err := exprFunc(t.Context(), nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_isMatch_validation(t *testing.T) {
	target := &ottl.StandardStringLikeGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return "anything", nil
		},
	}
	invalidRegexPattern := ottl.StandardStringGetter[any]{
		Getter: func(_ context.Context, _ any) (any, error) {
			return "\\K", nil
		},
	}
	exprFunc, err := isMatch[any](target, invalidRegexPattern)
	require.NoError(t, err)
	_, err = exprFunc(t.Context(), nil)
	require.Error(t, err)
}

func Test_isMatch_error(t *testing.T) {
	target := &ottl.StandardStringLikeGetter[any]{
		Getter: func(context.Context, any) (any, error) {
			return make(chan int), nil
		},
	}
	regexPattern := ottl.StandardStringGetter[any]{
		Getter: func(_ context.Context, _ any) (any, error) {
			return "test", nil
		},
	}
	exprFunc, err := isMatch[any](target, regexPattern)
	require.NoError(t, err)
	_, err = exprFunc(t.Context(), nil)
	require.Error(t, err)
}
