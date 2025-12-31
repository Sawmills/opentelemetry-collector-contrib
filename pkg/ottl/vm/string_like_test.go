// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package vm

import (
	"regexp"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

func TestStringLikeFromValue_Float(t *testing.T) {
	val := ir.Float64Value(0.1e-7)
	s, ok, err := stringLikeFromValue(val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok true")
	}
	if !regexp.MustCompile("0").MatchString(s) {
		t.Fatalf("expected string to contain 0, got %q", s)
	}
}
