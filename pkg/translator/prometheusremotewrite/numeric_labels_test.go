// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestGoNumericLabels(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		settings := Settings{UseGoFloatFormat: enabled, DisableTargetInfo: true, DisableScopeInfo: true}
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
		if enabled {
			want = []string{"6.399999999999999e-07", "1e-05", "0.1", "1", "10", "100000", "1e+06", "1.048576e+06", "+Inf"}
			quantiles[0] = "1e-05"
		}
		for _, version := range []string{"v1", "v2"} {
			t.Run(version+map[bool]string{false: "/default", true: "/go"}[enabled], func(t *testing.T) {
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
	require.Equal(t, "-0", formatNumericLabel(math.Copysign(0, -1), false))
	require.Equal(t, "0", formatNumericLabel(math.Copysign(0, -1), true))
}
