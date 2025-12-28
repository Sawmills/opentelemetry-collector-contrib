// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"os"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
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

// WithVMAttrGetter sets a VM fast-path attribute getter used by OpLoadAttrFast.
func WithVMAttrGetter[K any](getter vm.AttrGetter[K]) Option[K] {
	return func(p *Parser[K]) {
		p.vmAttrGetter = getter
	}
}

// WithVMAttrSetter sets a VM fast-path attribute setter used by OpSetAttrFast.
func WithVMAttrSetter[K any](setter vm.AttrSetter[K]) Option[K] {
	return func(p *Parser[K]) {
		p.vmAttrSetter = setter
	}
}

// WithVMAttrContextNames restricts OpLoadAttrFast to paths with these contexts.
func WithVMAttrContextNames[K any](contexts []string) Option[K] {
	return func(p *Parser[K]) {
		if len(contexts) == 0 {
			p.vmAttrContextNames = nil
			return
		}
		names := make(map[string]struct{}, len(contexts))
		for _, ctx := range contexts {
			names[ctx] = struct{}{}
		}
		p.vmAttrContextNames = names
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
