// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlscope

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func newBenchScopeContext() TransformContext {
	scope := pcommon.NewInstrumentationScope()
	scope.Attributes().PutInt("foo", 7)
	return NewTransformContext(scope, pcommon.NewResource(), pmetric.NewResourceMetrics())
}

func newBenchScopeParser(withVM bool) (ottl.Parser[TransformContext], error) {
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

func BenchmarkOTTLScopeAttributesEq_Interpreter(b *testing.B) {
	parser, err := newBenchScopeParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchScopeContext()
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

func BenchmarkOTTLScopeAttributesEq_VM(b *testing.B) {
	parser, err := newBenchScopeParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchScopeContext()
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
