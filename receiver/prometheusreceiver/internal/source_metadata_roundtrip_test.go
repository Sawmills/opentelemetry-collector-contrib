// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	prw "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
)

func TestSourceMetadataRoundTrip(t *testing.T) {
	tests := []struct {
		name         string
		metricName   string
		store        scrape.MetricMetadataStore
		wantMetadata bool
		wantType     prompb.MetricMetadata_MetricType
		wantHelp     string
		wantUnit     string
	}{
		{name: "no declaration", metricName: "go_memstats_heap_idle_bytes", store: emptyMetadataStore{}},
		{name: "explicit unknown with empty help", metricName: "unknown_metric", store: newFakeMetadataStore(map[string]scrape.MetricMetadata{
			"unknown_metric": {MetricFamily: "unknown_metric", Type: model.MetricTypeUnknown},
		}), wantMetadata: true, wantType: prompb.MetricMetadata_UNKNOWN},
		{name: "gauge with empty help", metricName: "gauge_metric", store: newFakeMetadataStore(map[string]scrape.MetricMetadata{
			"gauge_metric": {MetricFamily: "gauge_metric", Type: model.MetricTypeGauge},
		}), wantMetadata: true, wantType: prompb.MetricMetadata_GAUGE},
		{name: "declared gauge", metricName: "declared_seconds", store: newFakeMetadataStore(map[string]scrape.MetricMetadata{
			"declared_seconds": {MetricFamily: "declared_seconds", Type: model.MetricTypeGauge, Help: "Declared help", Unit: "seconds"},
		}), wantMetadata: true, wantType: prompb.MetricMetadata_GAUGE, wantHelp: "Declared help", wantUnit: "seconds"},
		{name: "help only", metricName: "help_only", store: newFakeMetadataStore(map[string]scrape.MetricMetadata{
			"help_only": {MetricFamily: "help_only", Type: model.MetricTypeUnknown, Help: "Untyped help"},
		}), wantMetadata: true, wantType: prompb.MetricMetadata_UNKNOWN, wantHelp: "Untyped help"},
		{name: "normalized counter metadata", metricName: "requests_total", store: newFakeMetadataStore(map[string]scrape.MetricMetadata{
			"requests": {MetricFamily: "requests", Type: model.MetricTypeCounter, Help: "Requests help"},
		}), wantMetadata: true, wantType: prompb.MetricMetadata_COUNTER, wantHelp: "Requests help"},
		{name: "internal scrape metric", metricName: "up", store: emptyMetadataStore{}, wantMetadata: true, wantType: prompb.MetricMetadata_GAUGE, wantHelp: "The scraping was successful"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mf := newMetricFamily(tt.metricName, tt.store, zap.NewNop(), false, false)
			require.NoError(t, mf.addSeries(1, tt.metricName, labels.FromStrings("instance", "source:9090"), 1000, 42))
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("service.instance.id", "source:9090")
			metrics := rm.ScopeMetrics().AppendEmpty().Metrics()
			mf.appendMetric(metrics, false)
			require.Equal(t, 1, metrics.Len())
			ts, err := prw.FromMetrics(md, prw.Settings{DisableTargetInfo: true, DisableScopeInfo: true})
			require.NoError(t, err)
			require.Len(t, ts, 1, "metadata omission must retain the sample")
			for _, series := range ts {
				require.Len(t, series.Samples, 1)
				require.Equal(t, float64(42), series.Samples[0].Value)
				require.Equal(t, int64(1000), series.Samples[0].Timestamp)
				require.Equal(t, []prompb.Label{{Name: "__name__", Value: tt.metricName}, {Name: "instance", Value: "source:9090"}}, series.Labels)
			}
			rows, err := prw.OtelMetricsToMetadata(md, false, "")
			require.NoError(t, err)
			if tt.wantMetadata {
				require.Len(t, rows, 1)
				require.Equal(t, tt.metricName, rows[0].MetricFamilyName)
				require.Equal(t, tt.wantType, rows[0].Type)
				require.Equal(t, tt.wantHelp, rows[0].Help)
				require.Equal(t, tt.wantUnit, rows[0].Unit)
			} else {
				require.Empty(t, rows, "a sample without a declaration must not overwrite source metadata")
			}
		})
	}
}

func TestNativeHistogramMetadataWithoutDeclaration(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(map[bool]string{false: "exponential", true: "custom buckets"}[custom], func(t *testing.T) {
			const name = "native_histogram"
			mf := newMetricFamily(name, emptyMetadataStore{}, zap.NewNop(), true, custom)
			ls := labels.FromStrings("instance", "source:9090")
			h := &histogram.Histogram{Schema: 0, Count: 2, Sum: 3, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{2}}
			if custom {
				h.Schema = -53
				h.CustomValues = []float64{2}
				require.NoError(t, mf.addNHCBSeries(1, name, ls, 1000, h, nil))
			} else {
				require.NoError(t, mf.addExponentialHistogramSeries(1, name, ls, 1000, h, nil))
			}
			md := pmetric.NewMetrics()
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("service.instance.id", "source:9090")
			mf.appendMetric(rm.ScopeMetrics().AppendEmpty().Metrics(), false)
			rows, err := prw.OtelMetricsToMetadata(md, false, "")
			require.NoError(t, err)
			require.Equal(t, []*prompb.MetricMetadata{{Type: prompb.MetricMetadata_HISTOGRAM, MetricFamilyName: name}}, rows)
			ts, err := prw.FromMetrics(md, prw.Settings{DisableTargetInfo: true, DisableScopeInfo: true})
			require.NoError(t, err)
			require.NotEmpty(t, ts)
			for _, series := range ts {
				found := false
				for _, label := range series.Labels {
					if label.Name == "instance" {
						require.Equal(t, "source:9090", label.Value)
						found = true
					}
					require.NotContains(t, label.Name, "metadata")
				}
				require.True(t, found)
				require.True(t, len(series.Samples) > 0 || len(series.Histograms) > 0)
			}
		})
	}
}
