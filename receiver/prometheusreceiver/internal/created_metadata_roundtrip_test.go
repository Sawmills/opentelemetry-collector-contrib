// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"math"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	prw "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
)

func TestCreatedSampleRemoteWriteRoundTrip(t *testing.T) {
	for _, sample := range []float64{0, 1700000000.1234567, math.Float64frombits(value.StaleNaN)} {
		t.Run(model.SampleValue(sample).String(), func(t *testing.T) {
			mf := newMetricFamily("requests_total", newFakeMetadataStore(map[string]scrape.MetricMetadata{
				"requests": {MetricFamily: "requests", Type: model.MetricTypeCounter, Help: "Requests help."},
			}), zap.NewNop(), false, false)
			mf.preserveCreatedMetrics = true
			require.NoError(t, mf.addSeries(1, "requests_total", labels.FromStrings("__name__", "requests_total", "owner", "sdk"), 1000, 7))
			require.NoError(t, mf.addSeries(1, "requests_created", labels.FromStrings("__name__", "requests_created", "owner", "sdk"), 1000, sample))
			// A later intrinsic start timestamp must not round the observed value.
			mf.addCreationTimestamp(1, labels.FromStrings("__name__", "requests_total", "owner", "sdk"), 1000, 1700000000123)
			md := pmetric.NewMetrics()
			metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			parentAppended := mf.appendMetric(metrics, false)
			mf.appendCreatedMetric(metrics, parentAppended)
			require.Equal(t, 2, metrics.Len())
			settings := prw.Settings{TranslationStrategy: otlptranslator.NoTranslation, DisableTargetInfo: true, DisableScopeInfo: true}
			metadata, err := prw.OtelMetricsToMetadataWithSettings(md, settings)
			require.NoError(t, err)
			require.Len(t, metadata, 1, "do not invent a declaration for the created sample")
			require.Equal(t, "requests", metadata[0].MetricFamilyName)
			require.Equal(t, "Requests help.", metadata[0].Help)
			series, err := prw.FromMetrics(md, settings)
			require.NoError(t, err)
			require.Len(t, series, 2)
			seen := map[string]bool{}
			for _, ts := range series {
				name := ""
				for _, label := range ts.Labels {
					require.Contains(t, []string{"__name__", "owner"}, label.Name, "internal provenance must not become a label")
					if label.Name == "__name__" {
						name = label.Value
					}
				}
				require.False(t, seen[name], "duplicate source identity")
				seen[name] = true
				require.Len(t, ts.Samples, 1)
				require.Equal(t, int64(1000), ts.Samples[0].Timestamp)
				if name == "requests_created" {
					if value.IsStaleNaN(sample) {
						require.True(t, value.IsStaleNaN(ts.Samples[0].Value))
					} else {
						require.Equal(t, sample, ts.Samples[0].Value)
					}
				} else {
					require.Equal(t, "requests_total", name)
					require.Equal(t, float64(7), ts.Samples[0].Value)
				}
			}
			require.Equal(t, map[string]bool{"requests_total": true, "requests_created": true}, seen)
		})
	}
}

func TestCreatedSampleDoesNotInventIntrinsicTimestampSeries(t *testing.T) {
	mf := newMetricFamily("requests_total", newFakeMetadataStore(map[string]scrape.MetricMetadata{
		"requests": {MetricFamily: "requests", Type: model.MetricTypeCounter},
	}), zap.NewNop(), false, false)
	mf.preserveCreatedMetrics = true
	ls := labels.FromStrings("__name__", "requests_total")
	require.NoError(t, mf.addSeries(1, "requests_total", ls, 1000, 7))
	mf.addCreationTimestamp(1, ls, 1000, 500)
	metrics := pmetric.NewMetricSlice()
	mf.appendCreatedMetric(metrics, false)
	require.Zero(t, metrics.Len(), "a protobuf/start timestamp is not an exposed _created sample")
}

func TestCreatedSampleStalenessHasIndependentTimestamp(t *testing.T) {
	mf := newMetricFamily("requests_total", newFakeMetadataStore(map[string]scrape.MetricMetadata{
		"requests": {MetricFamily: "requests", Type: model.MetricTypeCounter},
	}), zap.NewNop(), false, false)
	mf.preserveCreatedMetrics = true
	require.NoError(t, mf.addSeries(1, "requests_total", labels.FromStrings("__name__", "requests_total"), 1000, 8))
	require.NoError(t, mf.addSeries(1, "requests_created", labels.FromStrings("__name__", "requests_created"), 2000, math.Float64frombits(value.StaleNaN)))
	metrics := pmetric.NewMetricSlice()
	parentAppended := mf.appendMetric(metrics, false)
	mf.appendCreatedMetric(metrics, parentAppended)
	require.Equal(t, 2, metrics.Len())
	point := metrics.At(1).Gauge().DataPoints().At(0)
	require.True(t, point.Flags().NoRecordedValue())
	require.Equal(t, timestampFromMs(2000), point.Timestamp())
	require.Equal(t, timestampFromMs(1000), metrics.At(0).Sum().DataPoints().At(0).Timestamp())
}

func TestOrphanCreatedSampleRetainsParentMetadata(t *testing.T) {
	for _, typ := range []model.MetricType{model.MetricTypeCounter, model.MetricTypeHistogram, model.MetricTypeSummary} {
		t.Run(string(typ), func(t *testing.T) {
			mf := newMetricFamily("orphan_created", newFakeMetadataStore(map[string]scrape.MetricMetadata{
				"orphan": {MetricFamily: "orphan", Type: typ, Help: "Parent help.", Unit: "seconds"},
			}), zap.NewNop(), false, false)
			mf.preserveCreatedMetrics = true
			require.NoError(t, mf.addSeries(1, "orphan_created", labels.FromStrings("__name__", "orphan_created"), 1000, 1700000000.125))
			md := pmetric.NewMetrics()
			metrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
			parentAppended := mf.appendMetric(metrics, false)
			mf.appendCreatedMetric(metrics, parentAppended)
			require.Equal(t, 1, metrics.Len(), "do not invent parent samples")
			settings := prw.Settings{TranslationStrategy: otlptranslator.NoTranslation, DisableTargetInfo: true, DisableScopeInfo: true}
			rows, err := prw.OtelMetricsToMetadataWithSettings(md, settings)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "orphan", rows[0].MetricFamilyName)
			require.Equal(t, strings.ToUpper(string(typ)), rows[0].Type.String())
			require.Equal(t, "Parent help.", rows[0].Help)
			require.Equal(t, "seconds", rows[0].Unit)
			series, err := prw.FromMetrics(md, settings)
			require.NoError(t, err)
			require.Len(t, series, 1)
			for _, ts := range series {
				require.Len(t, ts.Samples, 1)
				require.Equal(t, 1700000000.125, ts.Samples[0].Value)
				for _, label := range ts.Labels {
					require.Equal(t, "__name__", label.Name)
					require.Equal(t, "orphan_created", label.Value)
				}
			}
		})
	}
}

func TestCreatedSampleBeforeParentHasIndependentTimestamp(t *testing.T) {
	mf := newMetricFamily("requests_total", newFakeMetadataStore(map[string]scrape.MetricMetadata{
		"requests": {MetricFamily: "requests", Type: model.MetricTypeCounter},
	}), zap.NewNop(), false, false)
	mf.preserveCreatedMetrics = true
	require.NoError(t, mf.addSeries(1, "requests_created", labels.FromStrings("__name__", "requests_created"), 2000, 500))
	require.NoError(t, mf.addSeries(1, "requests_total", labels.FromStrings("__name__", "requests_total"), 1000, 8))
	metrics := pmetric.NewMetricSlice()
	parentAppended := mf.appendMetric(metrics, false)
	mf.appendCreatedMetric(metrics, parentAppended)
	require.Equal(t, 2, metrics.Len())
	require.Equal(t, timestampFromMs(2000), metrics.At(1).Gauge().DataPoints().At(0).Timestamp())
	require.Equal(t, timestampFromMs(1000), metrics.At(0).Sum().DataPoints().At(0).Timestamp())
}
