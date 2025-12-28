// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"os"
	"strings"
)

const defaultMicroVMStackSize = 32
const vmEnvVar = "OTELCOL_OTTL_VM"

// WithVMEnabled enables the experimental VM execution path for supported comparisons.
func WithVMEnabled[K any]() Option[K] {
	return func(p *Parser[K]) {
		p.vmEnabled = true
		if p.vmProgramCache == nil {
			p.vmProgramCache = map[*comparison]*microProgram[K]{}
		}
		if p.vmBoolProgramCache == nil {
			p.vmBoolProgramCache = map[*booleanExpression]*microProgram[K]{}
		}
	}
}

// WithVMGasLimit sets the per-program gas limit for the VM. A zero value keeps the default.
func WithVMGasLimit[K any](limit uint64) Option[K] {
	return func(p *Parser[K]) {
		if limit == 0 {
			return
		}
		p.vmGasLimit = limit
	}
}

// WithVMEnabledFromEnv enables the VM when OTELCOL_OTTL_VM is set to a truthy value.
func WithVMEnabledFromEnv[K any]() Option[K] {
	return func(p *Parser[K]) {
		if !isVMEnvEnabled() {
			return
		}
		WithVMEnabled[K]()(p)
	}
}

func isVMEnvEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(vmEnvVar))) {
	case "1", "true", "t", "yes", "y":
		return true
	default:
		return false
	}
}
