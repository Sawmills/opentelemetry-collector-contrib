// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import (
	"testing"

	"github.com/prometheus/otlptranslator"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	prometheus "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
)

func TestNoTranslationPreservesDeclaredFamily(t *testing.T) {
	for _, tt := range []struct {
		name      string
		sample    string
		original  string
		family    string
		strategy  otlptranslator.TranslationStrategyOption
		namespace string
		want      string
	}{
		{name: "OpenMetrics counter", sample: "requests_total", family: "requests", strategy: otlptranslator.NoTranslation, want: "requests"},
		{name: "Prometheus counter", sample: "requests_total", family: "requests_total", strategy: otlptranslator.NoTranslation, want: "requests_total"},
		{name: "UTF-8 family", sample: "événements_total", family: "événements", strategy: otlptranslator.NoTranslation, want: "événements"},
		{name: "namespace", sample: "requests_total", family: "requests", strategy: otlptranslator.NoTranslation, namespace: "platform", want: "platform_requests"},
		{name: "suffix-adding rename", sample: "requests_total", original: "requests", family: "requests", strategy: otlptranslator.NoTranslation, want: "requests_total"},
		{name: "renamed sample", sample: "renamed_total", family: "requests", strategy: otlptranslator.NoTranslation, want: "renamed_total"},
		{name: "legacy translation", sample: "requests_total", family: "requests", want: "requests_total"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			md := pmetric.NewMetrics()
			metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName(tt.sample)
			metric.SetDescription("Requests help.")
			metric.Metadata().PutStr(prometheus.MetricMetadataFamilyKey, tt.family)
			original := tt.original
			if original == "" {
				original = tt.sample
			}
			metric.Metadata().PutStr(prometheus.MetricMetadataSourceNameKey, original)
			sum := metric.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			sum.SetIsMonotonic(true)
			dp := sum.DataPoints().AppendEmpty()
			dp.SetDoubleValue(7)
			settings := Settings{TranslationStrategy: tt.strategy, Namespace: tt.namespace, DisableTargetInfo: true, DisableScopeInfo: true}
			rows, err := OtelMetricsToMetadataWithSettings(md, settings)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, tt.want, rows[0].MetricFamilyName)
			require.Equal(t, "Requests help.", rows[0].Help)
			series, err := FromMetrics(md, settings)
			require.NoError(t, err)
			require.Len(t, series, 1)
			for _, ts := range series {
				for _, label := range ts.Labels {
					if label.Name == "__name__" {
						wantSample := tt.sample
						if tt.namespace != "" {
							wantSample = tt.namespace + "_" + wantSample
						}
						require.Equal(t, wantSample, label.Value, "family metadata must not rename samples")
					}
				}
			}
		})
	}
}

func TestDeclaredFamilyMetadataRemoteWriteVersionBoundary(t *testing.T) {
	md := pmetric.NewMetrics()
	metric := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("requests_total")
	metric.SetDescription("Requests help.")
	metric.Metadata().PutStr(prometheus.MetricMetadataFamilyKey, "requests")
	metric.Metadata().PutStr(prometheus.MetricMetadataSourceNameKey, "requests_total")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	sum.DataPoints().AppendEmpty().SetDoubleValue(7)
	settings := Settings{TranslationStrategy: otlptranslator.NoTranslation, DisableTargetInfo: true, DisableScopeInfo: true}

	v1Metadata, err := OtelMetricsToMetadataWithSettings(md, settings)
	require.NoError(t, err)
	require.Len(t, v1Metadata, 1)
	require.Equal(t, "requests", v1Metadata[0].MetricFamilyName, "v1 supports separate declared family metadata")
	v1Series, err := FromMetrics(md, settings)
	require.NoError(t, err)
	require.Len(t, v1Series, 1)
	for _, series := range v1Series {
		require.Len(t, series.Labels, 1)
		require.Equal(t, "__name__", series.Labels[0].Name)
		require.Equal(t, "requests_total", series.Labels[0].Value)
		require.Equal(t, float64(7), series.Samples[0].Value)
	}

	v2Series, table, err := FromMetricsV2(md, settings)
	require.NoError(t, err)
	require.Len(t, v2Series, 1)
	for _, series := range v2Series {
		symbols := table.Symbols()
		require.Len(t, series.LabelsRefs, 2, "family provenance must not become a new label")
		require.Equal(t, "__name__", symbols[series.LabelsRefs[0]])
		require.Equal(t, "requests_total", symbols[series.LabelsRefs[1]], "v2 metadata remains attached to the original sample identity")
		require.Equal(t, writev2.Metadata_METRIC_TYPE_COUNTER, series.Metadata.Type)
		require.Equal(t, "Requests help.", symbols[series.Metadata.HelpRef])
		require.Equal(t, float64(7), series.Samples[0].Value)
		require.NotContains(t, symbols, "requests", "v2 has no distinct declared family-name field")
	}
}
