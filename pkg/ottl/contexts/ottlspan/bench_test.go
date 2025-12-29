// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlspan

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func newBenchSpanContext() TransformContext {
	span := ptrace.NewSpan()
	span.Attributes().PutInt("foo", 7)
	return NewTransformContext(
		span,
		pcommon.NewInstrumentationScope(),
		pcommon.NewResource(),
		ptrace.NewScopeSpans(),
		ptrace.NewResourceSpans(),
	)
}

func newBenchSpanParser(withVM bool) (ottl.Parser[TransformContext], error) {
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

func BenchmarkOTTLSpanAttributesEq_Interpreter(b *testing.B) {
	parser, err := newBenchSpanParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
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

func BenchmarkOTTLSpanAttributesEq_VM(b *testing.B) {
	parser, err := newBenchSpanParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`attributes["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
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

func BenchmarkOTTLSpanTraceIDStringEq_Interpreter(b *testing.B) {
	parser, err := newBenchSpanParser(false)
	if err != nil {
		b.Fatal(err)
	}
	traceID := pcommon.TraceID{0x01, 0x02, 0x03, 0x04}
	cond, err := parser.ParseCondition(fmt.Sprintf("trace_id.string == %q", traceutil.TraceIDToHexOrEmptyString(traceID)))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().SetTraceID(traceID)
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

func BenchmarkOTTLSpanTraceIDStringEq_VM(b *testing.B) {
	parser, err := newBenchSpanParser(true)
	if err != nil {
		b.Fatal(err)
	}
	traceID := pcommon.TraceID{0x01, 0x02, 0x03, 0x04}
	cond, err := parser.ParseCondition(fmt.Sprintf("trace_id.string == %q", traceutil.TraceIDToHexOrEmptyString(traceID)))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().SetTraceID(traceID)
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

func BenchmarkOTTLSpanTraceStateKeyEq_Interpreter(b *testing.B) {
	parser, err := newBenchSpanParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`trace_state["rojo"] == "00f067aa0ba902b7"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().TraceState().FromRaw("rojo=00f067aa0ba902b7,congo=t61rcWkgMzE")
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

func BenchmarkOTTLSpanTraceStateKeyEq_VM(b *testing.B) {
	parser, err := newBenchSpanParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`trace_state["rojo"] == "00f067aa0ba902b7"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().TraceState().FromRaw("rojo=00f067aa0ba902b7,congo=t61rcWkgMzE")
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

func BenchmarkOTTLSpanDroppedEventsCountEq_Interpreter(b *testing.B) {
	parser, err := newBenchSpanParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`dropped_events_count == 3`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().SetDroppedEventsCount(3)
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

func BenchmarkOTTLSpanDroppedEventsCountEq_VM(b *testing.B) {
	parser, err := newBenchSpanParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`dropped_events_count == 3`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchSpanContext()
	tCtx.GetSpan().SetDroppedEventsCount(3)
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
