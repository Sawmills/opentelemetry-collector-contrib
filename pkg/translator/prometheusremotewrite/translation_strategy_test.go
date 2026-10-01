// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import (
	"testing"

	"github.com/prometheus/otlptranslator"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestNoTranslationPreservesScrapedIdentifiers(t *testing.T) {
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("rpc.server.duration_milliseconds")
	m.SetUnit("ms")
	h := m.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := h.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(2_000_000_000))
	dp.SetStartTimestamp(pcommon.Timestamp(1_000_000_000))
	dp.SetCount(3)
	dp.SetSum(50)
	dp.ExplicitBounds().FromRaw([]float64{25})
	dp.BucketCounts().FromRaw([]uint64{1, 2})
	dp.Attributes().PutStr("rpc.method", "CreateInsight")
	dp.Attributes().PutStr("rpc_method", "separate_source_label")

	settings := Settings{
		TranslationStrategy:       otlptranslator.NoTranslation,
		AddMetricSuffixes:         true,
		DisableTargetInfo:         true,
		DisableScopeInfo:          true,
		UseOpenMetricsFloatFormat: true,
	}
	checkLabels := func(t *testing.T, labels map[string]string, names map[string]bool) {
		require.Equal(t, "CreateInsight", labels["rpc.method"])
		require.Equal(t, "separate_source_label", labels["rpc_method"])
		names[labels["__name__"]] = true
	}
	wantNames := map[string]bool{
		"rpc.server.duration_milliseconds_bucket": true,
		"rpc.server.duration_milliseconds_count":  true,
		"rpc.server.duration_milliseconds_sum":    true,
	}
	t.Run("v1", func(t *testing.T) {
		series, err := FromMetrics(metrics, settings)
		require.NoError(t, err)
		require.Len(t, series, 4)
		names := make(map[string]bool)
		for _, ts := range series {
			labels := make(map[string]string)
			for _, label := range ts.Labels {
				labels[label.Name] = label.Value
			}
			checkLabels(t, labels, names)
		}
		require.Equal(t, wantNames, names)
	})
	t.Run("v2", func(t *testing.T) {
		series, symbols, err := FromMetricsV2(metrics, settings)
		require.NoError(t, err)
		require.Len(t, series, 4)
		names := make(map[string]bool)
		for _, ts := range series {
			labels := make(map[string]string)
			for i := 0; i < len(ts.LabelsRefs); i += 2 {
				labels[symbols.Symbols()[ts.LabelsRefs[i]]] = symbols.Symbols()[ts.LabelsRefs[i+1]]
			}
			checkLabels(t, labels, names)
		}
		require.Equal(t, wantNames, names)
	})
}

func TestNoTranslationV1TargetInfoResourceLabels(t *testing.T) {
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "rpc-service")
	resource.Resource().Attributes().PutStr("service.instance.id", "fixture")
	resource.Resource().Attributes().PutStr("server.address", "dotted")
	resource.Resource().Attributes().PutStr("server_address", "underscore")
	m := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(2e9)
	dp.SetIntValue(3)
	for _, strategy := range []otlptranslator.TranslationStrategyOption{otlptranslator.NoTranslation, ""} {
		t.Run(string(strategy), func(t *testing.T) {
			series, err := FromMetrics(metrics, Settings{TranslationStrategy: strategy, DisableScopeInfo: true})
			require.NoError(t, err)
			var labels map[string]string
			for _, ts := range series {
				candidate := make(map[string]string)
				for _, label := range ts.Labels {
					candidate[label.Name] = label.Value
				}
				if candidate["__name__"] == "target_info" {
					labels = candidate
					break
				}
			}
			require.NotNil(t, labels)
			if strategy == otlptranslator.NoTranslation {
				require.Equal(t, "dotted", labels["server.address"])
				require.Equal(t, "underscore", labels["server_address"])
			} else {
				require.NotContains(t, labels, "server.address")
				require.Contains(t, labels["server_address"], "dotted")
				require.Contains(t, labels["server_address"], "underscore")
			}
		})
	}
}

func TestNoTranslationUnderscoreLabels(t *testing.T) {
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "rpc-service")
	resource.Resource().Attributes().PutStr("service.instance.id", "fixture")
	resource.Resource().Attributes().PutStr("_", "resource")
	m := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(2e9)
	dp.SetIntValue(3)
	dp.Attributes().PutStr("_", "sample")
	settings := Settings{TranslationStrategy: otlptranslator.NoTranslation, DisableScopeInfo: true}
	t.Run("v1", func(t *testing.T) {
		series, err := FromMetrics(metrics, settings)
		require.NoError(t, err)
		require.Len(t, series, 2)
		observed := make(map[string]string)
		for _, ts := range series {
			labels := make(map[string]string)
			for _, label := range ts.Labels {
				labels[label.Name] = label.Value
			}
			observed[labels["__name__"]] = labels["_"]
		}
		require.Equal(t, map[string]string{"requests": "sample", "target_info": "resource"}, observed)
	})
	t.Run("v2", func(t *testing.T) {
		series, symbols, err := FromMetricsV2(metrics, settings)
		require.NoError(t, err)
		require.Len(t, series, 2)
		observed := make(map[string]string)
		for _, ts := range series {
			labels := make(map[string]string)
			for i := 0; i < len(ts.LabelsRefs); i += 2 {
				labels[symbols.Symbols()[ts.LabelsRefs[i]]] = symbols.Symbols()[ts.LabelsRefs[i+1]]
			}
			observed[labels["__name__"]] = labels["_"]
		}
		require.Equal(t, map[string]string{"requests": "sample", "target_info": "resource"}, observed)
	})
}

func TestRejectUnsafeUTF8SuffixStrategy(t *testing.T) {
	metrics := pmetric.NewMetrics()
	scope := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for _, name := range []string{"subtotal", "su"} {
		m := scope.Metrics().AppendEmpty()
		m.SetName(name)
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(2e9)
		dp.SetIntValue(3)
	}
	settings := Settings{TranslationStrategy: otlptranslator.NoUTF8EscapingWithSuffixes, DisableTargetInfo: true, DisableScopeInfo: true}
	v1, err := FromMetrics(metrics, settings)
	require.ErrorContains(t, err, "unsupported translation_strategy")
	require.Empty(t, v1)
	v2, _, err := FromMetricsV2(metrics, settings)
	require.ErrorContains(t, err, "unsupported translation_strategy")
	require.Empty(t, v2)
	rows, err := OtelMetricsToMetadataWithSettings(metrics, settings)
	require.ErrorContains(t, err, "unsupported translation_strategy")
	require.Empty(t, rows)
}
