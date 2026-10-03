// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sawmillsfuncs // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/sawmillsfuncs"

import (
	"context"
	"errors"
	"strings"
	"sync"
	"unsafe"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type ContainsArguments[K any] struct {
	Target        ottl.StringGetter[K]
	Patterns      []string
	CaseSensitive bool
}

func NewContainsFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory(
		"Contains",
		&ContainsArguments[K]{},
		createContainsFunction[K],
	)
}

func createContainsFunction[K any](
	_ ottl.FunctionContext,
	oArgs ottl.Arguments,
) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*ContainsArguments[K])

	if !ok {
		return nil, errors.New(
			"NewContainsFactory args must be of type *ContainsArguments[K]",
		)
	}

	return contains(args.Target, args.Patterns, args.CaseSensitive), nil
}

func containsAny(s string, substrings []string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// lowerBufPool hands out reusable byte slices for the case-insensitive
// substring search. The previous fallback was `val = strings.ToLower(val)`
// which allocates len(val) bytes per record × per filter rule. On customer's
// Datadog pipeline that fired 178 times per record (~700 kB of garbage per
// log at 4 kB bodies) and was the dominant heap allocator under load
// (SAW-7559). Pooling the buffer keeps the SIMD-accelerated strings.Contains
// path and amortizes the allocation across records.
//
// The pool holds *[]byte pointers (not []byte directly): sync.Pool.Get/Put
// take `any`, and a bare slice header would box on every call, which would
// reintroduce the per-record allocations we're trying to remove. A pointer
// passes through `any` without boxing.
var lowerBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// asciiUpperIndex finds the first uppercase ASCII byte, stopping if the
// prefix contains a non-ASCII byte. The fold checks the remaining suffix.
func asciiUpperIndex(s string) (int, bool) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 {
			return -1, false
		}
		if c >= 'A' && c <= 'Z' {
			return i, true
		}
	}
	return -1, true
}

// lowerASCIIInto folds A-Z to a-z in-place into the pointed-to buffer and
// returns a string view of the result. firstUpper must identify the first
// uppercase byte, and asciiUpperIndex must have confirmed the preceding prefix.
// A non-ASCII suffix returns false so the caller uses Unicode case-folding.
// If the buffer needs to grow the caller's pointer
// is updated to point at the resized slice so the new backing memory goes
// back into the pool. The returned string aliases `*bufPtr`'s backing
// memory — caller must keep using it only until the pointer is returned
// to the pool.
func lowerASCIIInto(bufPtr *[]byte, s string, firstUpper int) (string, bool) {
	if cap(*bufPtr) < len(s) {
		*bufPtr = make([]byte, len(s))
	} else {
		*bufPtr = (*bufPtr)[:len(s)]
	}
	buf := *bufPtr
	copy(buf, s[:firstUpper])
	for i := firstUpper; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 {
			return "", false
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		buf[i] = c
	}
	// `string(buf)` would copy; unsafe.String reuses the buffer's backing
	// memory. Safe here because strings.Contains is read-only.
	return unsafe.String(unsafe.SliceData(buf), len(buf)), true
}

func contains[K any](
	target ottl.StringGetter[K],
	patterns []string,
	caseSensitive bool,
) ottl.ExprFunc[K] {
	if !caseSensitive {
		lowerPatterns := make([]string, len(patterns))
		for i, pattern := range patterns {
			lowerPatterns[i] = strings.ToLower(pattern)
		}
		patterns = lowerPatterns
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
		if caseSensitive {
			return containsAny(val, patterns), nil
		}
		// Fast path: lowered pattern already sits in val verbatim (val is
		// lowercase, or the substring lives in a lowercase region).
		// strings.Contains uses SIMD so this stays cheap.
		if containsAny(val, patterns) {
			return true, nil
		}
		// Non-ASCII fallback: ASCII case-folding (A-Z → +32) doesn't cover
		// Unicode case pairs (Ëë, Ωω, etc.), so for non-ASCII haystacks we
		// take the original allocating path. `strings.ToLower` uses
		// `unicode.ToLower` which handles all case-folding the original
		// behavior promised. Customer's logs are ASCII so this branch is cold
		// on the hot pipeline.
		firstUpper, ascii := asciiUpperIndex(val)
		if !ascii {
			return containsAny(strings.ToLower(val), patterns), nil
		}
		if firstUpper < 0 {
			return false, nil
		}
		// ASCII fast slow path: fold val into a pooled buffer and run
		// strings.Contains. The pool replaces the per-record
		// strings.ToLower allocation that was the dominant heap pressure
		// source on the filter-sampling pipeline (SAW-7559). Pool holds
		// *[]byte to avoid slice-header boxing on Pool.Get/Put.
		bufPtr := lowerBufPool.Get().(*[]byte)
		lowered, ascii := lowerASCIIInto(bufPtr, val, firstUpper)
		hit := ascii && containsAny(lowered, patterns)
		// Reset length before returning to pool — keep the backing array.
		*bufPtr = (*bufPtr)[:0]
		lowerBufPool.Put(bufPtr)
		if !ascii {
			return containsAny(strings.ToLower(val), patterns), nil
		}
		return hit, nil
	}
}
