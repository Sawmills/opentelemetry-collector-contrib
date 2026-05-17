// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/sawmillsfuncs"

import (
	"context"
	"errors"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type StartsWithArguments[K any] struct {
	Target        ottl.StringGetter[K]
	Prefixes      []string
	CaseSensitive bool
}

func NewStartsWithFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory(
		"StartsWith",
		&StartsWithArguments[K]{},
		createStartsWithFunction[K],
	)
}

func createStartsWithFunction[K any](
	_ ottl.FunctionContext,
	oArgs ottl.Arguments,
) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*StartsWithArguments[K])

	if !ok {
		return nil, errors.New(
			"NewStartsWithFactory args must be of type *StartsWithArguments[K]",
		)
	}

	return startsWith(args.Target, args.Prefixes, args.CaseSensitive), nil
}

func startsWithAny(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func startsWithAnyFoldASCII(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if len(prefix) > len(s) {
			continue
		}
		matches := true
		for i := 0; i < len(prefix); i++ {
			c := s[i]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != prefix[i] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func maxPrefixLen(prefixes []string) int {
	maxLen := 0
	for _, prefix := range prefixes {
		if len(prefix) > maxLen {
			maxLen = len(prefix)
		}
	}
	return maxLen
}

func hasNonASCIIInPrefixWindow(s string, maxLen int) bool {
	if maxLen > len(s) {
		maxLen = len(s)
	}
	for i := 0; i < maxLen; i++ {
		if s[i] >= 0x80 {
			return true
		}
	}
	return false
}

func startsWith[K any](
	target ottl.StringGetter[K],
	prefixes []string,
	caseSensitive bool,
) ottl.ExprFunc[K] {
	longestPrefix := 0
	if !caseSensitive {
		lowerPrefixes := make([]string, len(prefixes))
		for i, prefix := range prefixes {
			lowerPrefixes[i] = strings.ToLower(prefix)
		}
		prefixes = lowerPrefixes
		longestPrefix = maxPrefixLen(prefixes)
	}
	return func(ctx context.Context, tCtx K) (any, error) {
		val, err := target.Get(ctx, tCtx)
		if err != nil {
			var typeError ottl.TypeError
			switch {
			case errors.As(err, &typeError):
				return false, nil
			default:
				return false, err
			}
		}
		if !caseSensitive {
			// Avoid lowercasing already-lowercase hot-path values.
			if startsWithAny(val, prefixes) {
				return true, nil
			}
			if startsWithAnyFoldASCII(val, prefixes) {
				return true, nil
			}
			if !hasNonASCIIInPrefixWindow(val, longestPrefix) {
				return false, nil
			}
			val = strings.ToLower(val)
		}
		return startsWithAny(val, prefixes), nil
	}
}
