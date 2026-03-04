// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlmetric

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func newBenchMetricContext() TransformContext {
	metric := pmetric.NewMetric()
	metric.Metadata().PutInt("foo", 7)
	return NewTransformContext(
		metric,
		pmetric.NewMetricSlice(),
		pcommon.NewInstrumentationScope(),
		pcommon.NewResource(),
		pmetric.NewScopeMetrics(),
		pmetric.NewResourceMetrics(),
	)
}

func newBenchMetricParser(withVM bool) (ottl.Parser[TransformContext], error) {
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

func BenchmarkOTTLMetricMetadataEq_Interpreter(b *testing.B) {
	parser, err := newBenchMetricParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`metadata["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
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

func BenchmarkOTTLMetricMetadataEq_VM(b *testing.B) {
	parser, err := newBenchMetricParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`metadata["foo"] == 7`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
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

func BenchmarkOTTLMetricDescriptionEq_Interpreter(b *testing.B) {
	parser, err := newBenchMetricParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`description == "desc"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetDescription("desc")
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

func BenchmarkOTTLMetricDescriptionEq_VM(b *testing.B) {
	parser, err := newBenchMetricParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`description == "desc"`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetDescription("desc")
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

func BenchmarkOTTLMetricAggTemporalityEq_Interpreter(b *testing.B) {
	parser, err := newBenchMetricParser(false)
	if err != nil {
		b.Fatal(err)
	}
	agg := int64(pmetric.AggregationTemporalityCumulative)
	cond, err := parser.ParseCondition(fmt.Sprintf("aggregation_temporality == %d", agg))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetEmptySum()
	tCtx.GetMetric().Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
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

func BenchmarkOTTLMetricAggTemporalityEq_VM(b *testing.B) {
	parser, err := newBenchMetricParser(true)
	if err != nil {
		b.Fatal(err)
	}
	agg := int64(pmetric.AggregationTemporalityCumulative)
	cond, err := parser.ParseCondition(fmt.Sprintf("aggregation_temporality == %d", agg))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetEmptySum()
	tCtx.GetMetric().Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
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

func BenchmarkOTTLMetricIsMonotonicEq_Interpreter(b *testing.B) {
	parser, err := newBenchMetricParser(false)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`is_monotonic == true`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetEmptySum()
	tCtx.GetMetric().Sum().SetIsMonotonic(true)
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

func BenchmarkOTTLMetricIsMonotonicEq_VM(b *testing.B) {
	parser, err := newBenchMetricParser(true)
	if err != nil {
		b.Fatal(err)
	}
	cond, err := parser.ParseCondition(`is_monotonic == true`)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	tCtx := newBenchMetricContext()
	tCtx.GetMetric().SetEmptySum()
	tCtx.GetMetric().Sum().SetIsMonotonic(true)
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
