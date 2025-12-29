// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlresource

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func newBenchResourceContext() TransformContext {
	res := pcommon.NewResource()
	res.Attributes().PutInt("foo", 7)
	return NewTransformContext(res, pmetric.NewResourceMetrics())
}

func newBenchResourceParser(withVM bool) (ottl.Parser[TransformContext], error) {
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

func BenchmarkOTTLResourceAttributesEq_Interpreter(b *testing.B) {
	parser, err := newBenchResourceParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
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

func BenchmarkOTTLResourceAttributesEq_VM(b *testing.B) {
	parser, err := newBenchResourceParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
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

func BenchmarkOTTLResourceDroppedAttributesCountEq_Interpreter(b *testing.B) {
	parser, err := newBenchResourceParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`dropped_attributes_count == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
	tCtx.GetResource().SetDroppedAttributesCount(7)
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

func BenchmarkOTTLResourceDroppedAttributesCountEq_VM(b *testing.B) {
	parser, err := newBenchResourceParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`dropped_attributes_count == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
	tCtx.GetResource().SetDroppedAttributesCount(7)
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

func BenchmarkOTTLResourceSchemaURLEq_Interpreter(b *testing.B) {
	parser, err := newBenchResourceParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`schema_url == "https://example.com/schema"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
	tCtx.GetResourceSchemaURLItem().SetSchemaUrl("https://example.com/schema")
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

func BenchmarkOTTLResourceSchemaURLEq_VM(b *testing.B) {
	parser, err := newBenchResourceParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`schema_url == "https://example.com/schema"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchResourceContext()
	tCtx.GetResourceSchemaURLItem().SetSchemaUrl("https://example.com/schema")
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
