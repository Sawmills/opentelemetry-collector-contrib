// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlprofilesample

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal/ctxprofilecommon"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ir"
)

func newBenchProfileSampleContext() TransformContext {
	sample := pprofile.NewSample()
	profile := pprofile.NewProfile()
	dict := pprofile.NewProfilesDictionary()
	if err := ctxprofilecommon.SetAttributeValueFromVM(dict, sample, "foo", ir.Int64Value(7)); err != nil {
		panic(err)
	}
	return NewTransformContext(
		sample,
		profile,
		dict,
		pcommon.NewInstrumentationScope(),
		pcommon.NewResource(),
		pprofile.NewScopeProfiles(),
		pprofile.NewResourceProfiles(),
	)
}

func newBenchProfileSampleParser(withVM bool) (ottl.Parser[TransformContext], error) {
	options := []ottl.Option[TransformContext]{}
	if withVM {
		options = append(options, ottl.WithVMEnabled[TransformContext]())
	}
	return NewParser(
		map[string]ottl.Factory[TransformContext]{},
		component.TelemetrySettings{Logger: zap.NewNop()},
		options...,
	)
}

func BenchmarkOTTLProfileSampleAttributesEq_Interpreter(b *testing.B) {
	parser, err := newBenchProfileSampleParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchProfileSampleContext()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := cond.Eval(ctx, tCtx)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("expected true")
		}
	}
}

func BenchmarkOTTLProfileSampleAttributesEq_VM(b *testing.B) {
	parser, err := newBenchProfileSampleParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchProfileSampleContext()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, err := cond.Eval(ctx, tCtx)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("expected true")
		}
	}
}
