// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"reflect"
)

// CompareResult captures interpreter vs VM outputs for a statement in shadow mode.
type CompareResult struct {
	Interpreter any
	VM          any
	InterpErr   error
	VMErr       error
	Diverged    bool
}

// Compare runs both interpreter and VM for the given statement (if VM is enabled),
// returning both outputs. Interpreter result is always authoritative.
// VM must already be enabled on the parser; otherwise VM values are nil.
func Compare[K any](parser *Parser[K], stmt *Statement[K], ctx context.Context, tCtx K) CompareResult {
	// Interpreter path
	interpResult, interpErr := stmt.function.Eval(ctx, tCtx)
	// VM path (only when enabled)
	var vmResult any
	var vmErr error
	if parser != nil && parser.vmEnabled {
		vmResult, vmErr = stmt.function.Eval(ctx, tCtx)
	}
	diverged := !errorsEquivalent(interpErr, vmErr) || (interpErr == nil && vmErr == nil && !reflect.DeepEqual(interpResult, vmResult))
	if diverged && parser != nil && parser.vmTelemetry != nil {
		parser.vmTelemetry.recordDivergence(ctx, interpErr != vmErr)
	}
	return CompareResult{
		Interpreter: interpResult,
		VM:          vmResult,
		InterpErr:   interpErr,
		VMErr:       vmErr,
		Diverged:    diverged,
	}
}
