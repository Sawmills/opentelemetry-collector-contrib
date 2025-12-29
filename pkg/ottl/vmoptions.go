// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"os"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
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

// WithVMLogRecordGetter sets a direct log record getter for VM direct field opcodes.
func WithVMLogRecordGetter[K any](getter func(K) plog.LogRecord) Option[K] {
	return func(p *Parser[K]) {
		p.vmLogRecordGetter = getter
	}
}

// WithVMSpanGetter sets a direct span getter for VM direct field opcodes.
func WithVMSpanGetter[K any](getter func(K) ptrace.Span) Option[K] {
	return func(p *Parser[K]) {
		p.vmSpanGetter = getter
	}
}

// WithVMMetricGetter sets a direct metric getter for VM direct field opcodes.
func WithVMMetricGetter[K any](getter func(K) pmetric.Metric) Option[K] {
	return func(p *Parser[K]) {
		p.vmMetricGetter = getter
	}
}

// WithVMResourceGetter sets a direct resource getter for VM direct field opcodes.
func WithVMResourceGetter[K any](getter func(K) pcommon.Resource) Option[K] {
	return func(p *Parser[K]) {
		p.vmResourceGetter = getter
	}
}

// WithVMResourceSchemaURLGetter sets a direct resource schema URL getter for VM direct field opcodes.
func WithVMResourceSchemaURLGetter[K any](getter func(K) string) Option[K] {
	return func(p *Parser[K]) {
		p.vmResourceSchemaURLGetter = getter
	}
}

// WithVMResourceSchemaURLSetter sets a direct resource schema URL setter for VM direct field opcodes.
func WithVMResourceSchemaURLSetter[K any](setter func(K, string)) Option[K] {
	return func(p *Parser[K]) {
		p.vmResourceSchemaURLSetter = setter
	}
}

// WithVMScopeGetter sets a direct scope getter for VM direct field opcodes.
func WithVMScopeGetter[K any](getter func(K) pcommon.InstrumentationScope) Option[K] {
	return func(p *Parser[K]) {
		p.vmScopeGetter = getter
	}
}

// WithVMScopeSchemaURLGetter sets a direct scope schema URL getter for VM direct field opcodes.
func WithVMScopeSchemaURLGetter[K any](getter func(K) string) Option[K] {
	return func(p *Parser[K]) {
		p.vmScopeSchemaURLGetter = getter
	}
}

// WithVMScopeSchemaURLSetter sets a direct scope schema URL setter for VM direct field opcodes.
func WithVMScopeSchemaURLSetter[K any](setter func(K, string)) Option[K] {
	return func(p *Parser[K]) {
		p.vmScopeSchemaURLSetter = setter
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
