// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package regexp

import (
	stdregexp "regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wasilibs/go-re2"
)

func TestCompileSelectsEngineByPatternLength(t *testing.T) {
	shortPattern := strings.Repeat("a", patternLengthThreshold-1)
	shortMatcher, err := Compile(shortPattern)
	require.NoError(t, err)
	require.IsType(t, &stdregexp.Regexp{}, shortMatcher)

	longPattern := strings.Repeat("a", patternLengthThreshold)
	longMatcher, err := Compile(longPattern)
	require.NoError(t, err)
	require.IsType(t, &re2.Regexp{}, longMatcher)
}

func TestCompileReturnsEngineErrors(t *testing.T) {
	shortMatcher, err := Compile("(")
	require.Error(t, err)
	require.Nil(t, shortMatcher)

	longMatcher, err := Compile(strings.Repeat("a", patternLengthThreshold) + "(")
	require.Error(t, err)
	require.Nil(t, longMatcher)
}

func TestMustCompilePanicsOnEngineErrors(t *testing.T) {
	require.Panics(t, func() {
		MustCompile("(")
	})
	require.Panics(t, func() {
		MustCompile(strings.Repeat("a", patternLengthThreshold) + "(")
	})
}
