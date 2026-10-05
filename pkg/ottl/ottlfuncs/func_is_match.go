// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"

import (
	"context"
	"errors"
	"regexp/syntax"
	"strings"
	"unicode/utf8"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type IsMatchArguments[K any] struct {
	Target  ottl.StringLikeGetter[K]
	Pattern ottl.StringGetter[K]
}

func NewIsMatchFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("IsMatch", &IsMatchArguments[K]{}, createIsMatchFunction[K])
}

func createIsMatchFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*IsMatchArguments[K])

	if !ok {
		return nil, errors.New("IsMatchFactory args must be of type *IsMatchArguments[K]")
	}

	return isMatch(args.Target, args.Pattern)
}

func isMatch[K any](target ottl.StringLikeGetter[K], pattern ottl.StringGetter[K]) (ottl.ExprFunc[K], error) {
	var substring string
	if literalPattern, isLiteral := ottl.GetLiteralValue(pattern); isLiteral {
		substring = isMatchLiteralSubstring(literalPattern)
	}

	compiledPattern, err := newDynamicRegex("IsMatch", pattern)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		cp, err := compiledPattern.compile(ctx, tCtx)
		if err != nil {
			return nil, err
		}
		val, err := target.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return false, nil
		}
		if substring != "" {
			return strings.Contains(*val, substring), nil
		}
		return cp.MatchString(*val), nil
	}, nil
}

// isMatchLiteralSubstring admits only patterns whose match is exactly a
// case-sensitive substring search. Unanchored any-character stars can be
// skipped because the regex may start at the literal itself.
func isMatchLiteralSubstring(pattern string) string {
	// Leave inline flags and named groups on the canonical regex path.
	if strings.Contains(pattern, "(?") {
		return ""
	}
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return ""
	}
	re = re.Simplify()
	parts := []*syntax.Regexp{re}
	if re.Op == syntax.OpConcat {
		parts = re.Sub
	}

	var literal string
	for _, part := range parts {
		for part.Op == syntax.OpCapture {
			part = part.Sub[0]
		}
		switch part.Op {
		case syntax.OpLiteral:
			if literal != "" || part.Flags&syntax.FoldCase != 0 {
				return ""
			}
			literal = string(part.Rune)
		case syntax.OpStar:
			child := part.Sub[0]
			for child.Op == syntax.OpCapture {
				child = child.Sub[0]
			}
			if child.Op != syntax.OpAnyChar && child.Op != syntax.OpAnyCharNotNL {
				return ""
			}
		default:
			return ""
		}
	}
	// Regexp treats malformed UTF-8 as RuneError; byte searches do not.
	if strings.ContainsRune(literal, utf8.RuneError) {
		return ""
	}
	return literal
}
