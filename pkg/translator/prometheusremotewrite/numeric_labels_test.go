// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import (
	"math"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestNumericLabelFormats(t *testing.T) {
	for _, mode := range []string{"default", "go", "decimal"} {
		settings := Settings{UseGoFloatFormat: mode == "go", UseDecimalFloatFormat: mode == "decimal", DisableTargetInfo: true, DisableScopeInfo: true}
		metrics := pmetric.NewMetrics()
		ms := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		histogram := ms.AppendEmpty()
		histogram.SetName("latency")
		histogram.SetDescription("Histogram help")
		h := histogram.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		pt := h.DataPoints().AppendEmpty()
		pt.SetTimestamp(pcommon.Timestamp(1e9))
		pt.SetCount(9)
		pt.SetSum(42)
		pt.Attributes().PutStr("source", "app")
		pt.ExplicitBounds().FromRaw([]float64{6.399999999999999e-7, 1e-5, 0.1, 1, 10, 100000, 1e6, 1048576})
		pt.BucketCounts().FromRaw([]uint64{1, 1, 1, 1, 1, 1, 1, 1, 1})
		summary := ms.AppendEmpty()
		summary.SetName("summary")
		sp := summary.SetEmptySummary().DataPoints().AppendEmpty()
		sp.SetTimestamp(pcommon.Timestamp(1e9))
		sp.SetCount(2)
		sp.SetSum(10)
		for _, q := range []float64{1e-5, 0.5, 1} {
			qp := sp.QuantileValues().AppendEmpty()
			qp.SetQuantile(q)
			qp.SetValue(3)
		}
		want := []string{"0.0000006399999999999999", "0.00001", "0.1", "1", "10", "100000", "1000000", "1048576", "+Inf"}
		quantiles := []string{"0.00001", "0.5", "1"}
		switch mode {
		case "go":
			want = []string{"6.399999999999999e-07", "1e-05", "0.1", "1", "10", "100000", "1e+06", "1.048576e+06", "+Inf"}
			quantiles[0] = "1e-05"
		case "decimal":
			want = []string{"0.0000006399999999999999", "0.00001", "0.1", "1.0", "10.0", "100000.0", "1000000.0", "1048576.0", "+Inf"}
			quantiles[2] = "1.0"
		}
		for _, version := range []string{"v1", "v2"} {
			t.Run(version+"/"+mode, func(t *testing.T) {
				buckets, gotQuantiles := map[string]float64{}, []string{}
				check := func(labels []prompb.Label, samples []prompb.Sample) {
					for _, label := range labels {
						if label.Name == "le" {
							buckets[label.Value] = samples[0].Value
						}
						if label.Name == "quantile" {
							gotQuantiles = append(gotQuantiles, label.Value)
						}
					}
				}
				if version == "v1" {
					out, err := FromMetrics(metrics, settings)
					require.NoError(t, err)
					for _, ts := range out {
						check(ts.Labels, ts.Samples)
					}
				} else {
					out, symbols, err := FromMetricsV2(metrics, settings)
					require.NoError(t, err)
					for _, ts := range out {
						labels := make([]prompb.Label, 0, len(ts.LabelsRefs)/2)
						for i := 0; i < len(ts.LabelsRefs); i += 2 {
							labels = append(labels, prompb.Label{Name: symbols.Symbols()[ts.LabelsRefs[i]], Value: symbols.Symbols()[ts.LabelsRefs[i+1]]})
						}
						samples := make([]prompb.Sample, len(ts.Samples))
						for i, s := range ts.Samples {
							samples[i] = prompb.Sample{Value: s.Value, Timestamp: s.Timestamp}
						}
						check(labels, samples)
					}
				}
				require.Len(t, buckets, len(want))
				for i, label := range want {
					require.Equal(t, float64(i+1), buckets[label], label)
				}
				require.ElementsMatch(t, quantiles, gotQuantiles)
			})
		}
	}
}

func TestGoFloatSignedZero(t *testing.T) {
	require.Equal(t, "-0", formatNumericLabel(math.Copysign(0, -1), Settings{}))
	require.Equal(t, "0", formatNumericLabel(math.Copysign(0, -1), Settings{UseGoFloatFormat: true}))
}

func TestDecimalCapturedLabels(t *testing.T) {
	// Needle Wide /metrics samples, September 28, 2026. Finite spellings also
	// match prometheus-client 0.24.1 with dtoa 1.0.11 for these specific bounds.
	for _, want := range []string{
		"0.000001", "0.00001", "0.0001", "0.001", "0.005", "0.01", "0.025",
		"0.05", "0.1", "0.25", "0.5", "1.0", "2.0", "2.5", "5.0",
		"10.0", "15.0", "20.0", "30.0", "60.0", "+Inf",
	} {
		value, err := strconv.ParseFloat(want, 64)
		require.NoError(t, err)
		require.Equal(t, want, formatNumericLabel(value, Settings{UseDecimalFloatFormat: true}))
	}
}

func TestDecimalFloatEdges(t *testing.T) {
	for _, tt := range []struct {
		value float64
		want  string
	}{
		{0, "0.0"},
		{math.Copysign(0, -1), "-0.0"},
		{-1, "-1.0"},
		{-1.5, "-1.5"},
		{math.Inf(1), "+Inf"},
		{math.Inf(-1), "-Inf"},
		{math.NaN(), "NaN"},
		// Fixed-point mode is not an emulator for dtoa: these valid outputs
		// intentionally differ from dtoa 1.0.11's formatting.
		{1e-7, "0.0000001"},
		{1e21, "1000000000000000000000.0"},
		{0.30000000000000004, "0.30000000000000004"},
	} {
		require.Equal(t, tt.want, formatNumericLabel(tt.value, Settings{UseDecimalFloatFormat: true}))
	}
}
