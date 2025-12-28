// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

func (p *Parser[K]) getOrCompileVMProgram(cmp *comparison) (*microProgram[K], error) {
	p.vmProgramCacheMu.Lock()
	if p.vmProgramCache != nil {
		if program, ok := p.vmProgramCache[cmp]; ok {
			p.vmProgramCacheMu.Unlock()
			return program, nil
		}
	}
	p.vmProgramCacheMu.Unlock()

	program, err := p.compileMicroComparisonVM(cmp)
	if err != nil {
		return nil, err
	}

	p.vmProgramCacheMu.Lock()
	if p.vmProgramCache == nil {
		p.vmProgramCache = map[*comparison]*microProgram[K]{}
	}
	p.vmProgramCache[cmp] = program
	p.vmProgramCacheMu.Unlock()
	return program, nil
}

func (p *Parser[K]) getOrCompileVMBoolProgram(expr *booleanExpression) (*microProgram[K], error) {
	p.vmProgramCacheMu.Lock()
	if p.vmBoolProgramCache != nil {
		if program, ok := p.vmBoolProgramCache[expr]; ok {
			p.vmProgramCacheMu.Unlock()
			return program, nil
		}
	}
	p.vmProgramCacheMu.Unlock()

	program, err := p.compileMicroBoolExpression(expr)
	if err != nil {
		return nil, err
	}

	p.vmProgramCacheMu.Lock()
	if p.vmBoolProgramCache == nil {
		p.vmBoolProgramCache = map[*booleanExpression]*microProgram[K]{}
	}
	p.vmBoolProgramCache[expr] = program
	p.vmProgramCacheMu.Unlock()
	return program, nil
}
