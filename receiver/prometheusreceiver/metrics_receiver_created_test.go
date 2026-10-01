// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusreceiver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	prw "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
)

func TestPreserveCreatedMetrics(t *testing.T) {
	const body = `# HELP requests Requests help.
# TYPE requests counter
requests_total{owner="sdk"} 7
requests_created{owner="sdk"} 1700000000.1234567
# HELP latency Latency help.
# TYPE latency histogram
latency_bucket{owner="sdk",le="1"} 1
latency_bucket{owner="sdk",le="+Inf"} 1
latency_count{owner="sdk"} 1
latency_sum{owner="sdk"} 0.3
latency_created{owner="sdk"} 1700000000.1234567
# HELP payload Payload help.
# TYPE payload summary
payload{owner="sdk",quantile="0.5"} 2
payload_count{owner="sdk"} 1
payload_sum{owner="sdk"} 2
payload_created{owner="sdk"} 0
# HELP no_creation A counter without creation time.
# TYPE no_creation counter
no_creation_total{owner="sdk"} 1
# TYPE orphan counter
orphan_created{owner="sdk"} 1700000000.1234567
# TYPE declared_created gauge
declared_created{owner="sdk"} 42
# EOF
`
	for _, preserve := range []bool{false, true} {
		t.Run(fmt.Sprintf("preserve=%t", preserve), func(t *testing.T) {
			target := &testData{name: "created-source", pages: []mockPrometheusResponse{{code: 200, data: body, useOpenMetrics: true}}, validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
				verifyNumValidScrapeResults(t, td, results)
				metrics := make(map[string]pmetric.Metric)
				for _, metric := range getMetrics(results[0]) {
					require.NotContains(t, metrics, metric.Name(), "duplicate metric family")
					metrics[metric.Name()] = metric
				}
				require.Contains(t, metrics, "requests_total")
				assert.Equal(t, float64(7), metrics["requests_total"].Sum().DataPoints().At(0).DoubleValue())
				assert.NotContains(t, metrics, "no_creation_created", "do not invent a sample from missing creation time")
				assert.NotContains(t, metrics, "orphan_total", "retain existing orphan counter handling")
				require.Contains(t, metrics, "declared_created")
				assert.Equal(t, float64(42), metrics["declared_created"].Gauge().DataPoints().At(0).DoubleValue())
				for name, expected := range map[string]float64{"requests_created": 1700000000.1234567, "latency_created": 1700000000.1234567, "payload_created": 0, "orphan_created": 1700000000.1234567} {
					if !preserve {
						assert.NotContains(t, metrics, name)
						continue
					}
					require.Contains(t, metrics, name)
					metric := metrics[name]
					require.Equal(t, pmetric.MetricTypeGauge, metric.Type())
					points := metric.Gauge().DataPoints()
					require.Equal(t, 1, points.Len())
					assert.Equal(t, expected, points.At(0).DoubleValue(), "retain source seconds without millisecond rounding")
					assert.Equal(t, map[string]any{"owner": "sdk"}, points.At(0).Attributes().AsRaw(), "bucket and quantile labels must not leak")
					assert.NotZero(t, points.At(0).Timestamp())
					present, ok := metric.Metadata().Get(prometheus.MetricMetadataPresentKey)
					require.True(t, ok)
					assert.False(t, present.Bool(), "creation samples have no independent source metadata declaration")
					assert.Empty(t, metric.Description())
					assert.Empty(t, metric.Unit())
				}
			}}
			testComponent(t, []*testData{target}, func(cfg *Config) { cfg.PreserveCreatedMetrics = preserve })
		})
	}
}

func TestPreserveCreatedMetricsDoesNotSuppressDeclaredGauge(t *testing.T) {
	const body = `# TYPE requests_total counter
requests_total 7
# HELP requests_created Explicit source gauge.
# TYPE requests_created gauge
requests_created 1700000000.125
`
	for _, preserve := range []bool{false, true} {
		t.Run(fmt.Sprintf("preserve=%t", preserve), func(t *testing.T) {
			target := &testData{name: "text-created-source", pages: []mockPrometheusResponse{{code: 200, data: body}}, validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
				verifyNumValidScrapeResults(t, td, results)
				for _, metric := range getMetrics(results[0]) {
					if !strings.HasSuffix(metric.Name(), "_created") {
						continue
					}
					require.Equal(t, "requests_created", metric.Name())
					assert.Equal(t, "Explicit source gauge.", metric.Description())
					assert.Equal(t, float64(1700000000.125), metric.Gauge().DataPoints().At(0).DoubleValue())
					present, ok := metric.Metadata().Get(prometheus.MetricMetadataPresentKey)
					assert.False(t, ok && !present.Bool(), "retain explicitly declared gauge metadata")
					return
				}
				t.Fatal("declared creation-time gauge missing")
			}}
			testComponent(t, []*testData{target}, func(cfg *Config) { cfg.PreserveCreatedMetrics = preserve })
		})
	}
}

func TestPreserveCreatedMetricsRejectsZeroIngestionGate(t *testing.T) {
	const gate = "receiver.prometheusreceiver.EnableCreatedTimestampZeroIngestion"
	old := metadata.ReceiverPrometheusreceiverEnableCreatedTimestampZeroIngestionFeatureGate.IsEnabled()
	require.NoError(t, featuregate.GlobalRegistry().Set(gate, true))
	t.Cleanup(func() { require.NoError(t, featuregate.GlobalRegistry().Set(gate, old)) })
	cfg := createDefaultConfig().(*Config)
	cfg.PreserveCreatedMetrics = true
	_, err := newPrometheusReceiver(receivertest.NewNopSettings(metadata.Type), cfg, new(consumertest.MetricsSink))
	require.ErrorContains(t, err, "preserve_created_metrics is incompatible with")
}

func TestPreserveCreatedMetricsStalenessAcrossScrapes(t *testing.T) {
	target := &testData{name: "created-staleness", pages: []mockPrometheusResponse{
		{code: 200, useOpenMetrics: true, data: "# TYPE requests counter\nrequests_total 7 1.0\nrequests_created 0.5 1.0\n# EOF\n"},
		{code: 200, useOpenMetrics: true, data: "# TYPE requests counter\nrequests_total 8 1.0\n# EOF\n"},
	}, validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
		verifyNumValidScrapeResults(t, td, results)
		for i, rm := range results {
			found := false
			for _, metric := range getMetrics(rm) {
				if metric.Name() != "requests_created" {
					continue
				}
				found = true
				require.Equal(t, 1, metric.Gauge().DataPoints().Len())
				point := metric.Gauge().DataPoints().At(0)
				if i == 0 {
					require.False(t, point.Flags().NoRecordedValue())
					require.Equal(t, float64(0.5), point.DoubleValue())
				} else {
					require.True(t, point.Flags().NoRecordedValue(), "removed created series needs its own stale marker")
					require.Greater(t, point.Timestamp(), timestampFromFloat64(1))
				}
			}
			require.True(t, found, "scrape %d is missing its creation-time sample", i)
		}
	}}
	testComponent(t, []*testData{target}, func(cfg *Config) {
		cfg.PreserveCreatedMetrics = true
		cfg.PrometheusConfig.ScrapeConfigs[0].TrackTimestampsStaleness = true
	})
}

func TestCreatedMetricsReceiverNameRelabelDefaults(t *testing.T) {
	target := &testData{name: "source-name-relabel", pages: []mockPrometheusResponse{{code: 200, data: "# TYPE requests counter\nrequests 7\n"}}, validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
		verifyNumValidScrapeResults(t, td, results)
		md := pmetric.NewMetrics()
		results[0].CopyTo(md.ResourceMetrics().AppendEmpty())
		rows, err := prw.OtelMetricsToMetadataWithSettings(md, prw.Settings{TranslationStrategy: otlptranslator.NoTranslation})
		require.NoError(t, err)
		found := false
		for _, row := range rows {
			require.NotEqual(t, "requests", row.MetricFamilyName, "a renamed text counter must not retain its old declaration name")
			if row.MetricFamilyName == "requests_total" {
				found = true
			}
		}
		require.True(t, found)
	}}
	testComponent(t, []*testData{target}, func(cfg *Config) {
		cfg.PreserveCreatedMetrics = false
		rule := relabel.DefaultRelabelConfig
		rule.SourceLabels = model.LabelNames{"__name__"}
		rule.Regex = relabel.MustNewRegexp("requests")
		rule.TargetLabel = "__name__"
		rule.Replacement = "requests_total"
		cfg.PrometheusConfig.ScrapeConfigs[0].MetricRelabelConfigs = []*relabel.Config{&rule}
	})
}

func TestPreserveCreatedMetricsRejectsReceiverNameRelabel(t *testing.T) {
	target := &testData{name: "rejected-name-relabel", pages: []mockPrometheusResponse{{code: 200, data: "# TYPE requests counter\nrequests 7\n"}}}
	server, promCfg, err := setupMockPrometheus(target)
	require.NoError(t, err)
	defer server.Close()
	rule := relabel.DefaultRelabelConfig
	rule.SourceLabels = model.LabelNames{"__name__"}
	rule.Regex = relabel.MustNewRegexp("requests")
	rule.TargetLabel = "__name__"
	rule.Replacement = "requests_total"
	promCfg.ScrapeConfigs[0].MetricRelabelConfigs = []*relabel.Config{&rule}
	cfg := &Config{PrometheusConfig: promCfg, PreserveCreatedMetrics: true}
	_, err = newPrometheusReceiver(receivertest.NewNopSettings(metadata.Type), cfg, new(consumertest.MetricsSink))
	require.ErrorContains(t, err, "requires name-preserving metric_relabel_configs")
}

func TestPreserveCreatedMetricsWithNHCBDisabled(t *testing.T) {
	const body = "# TYPE latency histogram\nlatency_bucket{le=\"+Inf\"} 1\nlatency_count 1\nlatency_sum 1\nlatency_created 0.5\n# TYPE requests counter\nrequests_total 7\nrequests_created 0.5\n# EOF\n"
	target := &testData{name: "nhcb-created", pages: []mockPrometheusResponse{{code: 200, data: body, useOpenMetrics: true}}, validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
		verifyNumValidScrapeResults(t, td, results)
		names := map[string]bool{}
		for _, metric := range getMetrics(results[0]) {
			names[metric.Name()] = true
		}
		require.True(t, names["latency_created"], "conversion must not silently drop observed creation samples")
		require.True(t, names["requests_created"], "conversion must not skip later families' creation samples")
	}}
	testComponent(t, []*testData{target}, func(cfg *Config) {
		cfg.PreserveCreatedMetrics = true
		enabled := false
		cfg.PrometheusConfig.ScrapeConfigs[0].ConvertClassicHistogramsToNHCB = &enabled
	})
}

func TestPreserveCreatedMetricsRejectsExternalName(t *testing.T) {
	server, promCfg, err := setupMockPrometheus(&testData{name: "external-name-invalid", pages: []mockPrometheusResponse{{code: 200, data: "# TYPE requests counter\nrequests 7\n"}}})
	require.NoError(t, err)
	defer server.Close()
	promCfg.GlobalConfig.ExternalLabels = labels.FromStrings("__name__", "requests_total")
	_, err = newPrometheusReceiver(receivertest.NewNopSettings(metadata.Type), &Config{PrometheusConfig: promCfg, PreserveCreatedMetrics: true}, new(consumertest.MetricsSink))
	require.ErrorContains(t, err, "external_labels must not contain __name__")
}

func TestPreserveCreatedMetricsRejectsNHCB(t *testing.T) {
	server, promCfg, err := setupMockPrometheus(&testData{name: "nhcb-invalid", pages: []mockPrometheusResponse{{code: 200, data: "# TYPE latency histogram\nlatency_count 1\n"}}})
	require.NoError(t, err)
	defer server.Close()
	enabled := true
	promCfg.ScrapeConfigs[0].ConvertClassicHistogramsToNHCB = &enabled
	_, err = newPrometheusReceiver(receivertest.NewNopSettings(metadata.Type), &Config{PrometheusConfig: promCfg, PreserveCreatedMetrics: true}, new(consumertest.MetricsSink))
	require.ErrorContains(t, err, "incompatible with convert_classic_histograms_to_nhcb")
}

func TestPreserveCreatedMetricsDisablesLiveTargetConversion(t *testing.T) {
	const body = "# TYPE latency histogram\nlatency_bucket{le=\"+Inf\"} 1\nlatency_count 1\nlatency_sum 1\nlatency_created 0.5\n# EOF\n"
	for _, tc := range []struct {
		name     string
		body     string
		code     int
		drop     bool
		discover bool
		preserve bool
	}{
		{name: "source", body: body, code: 200, preserve: true},
		{name: "discovery", body: body, code: 200, preserve: true, discover: true},
		{name: "empty", body: "# EOF\n", code: 200, preserve: true},
		{name: "dropped", body: body, code: 200, drop: true, preserve: true},
		{name: "failed", body: body, code: 500, preserve: true},
		{name: "default", body: body, code: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages := make([]mockPrometheusResponse, 10)
			for i := range pages {
				pages[i] = mockPrometheusResponse{code: tc.code, data: tc.body, useOpenMetrics: true}
			}
			server, promCfg, err := setupMockPrometheus(&testData{name: "target-conversion", pages: pages})
			require.NoError(t, err)
			defer server.Close()
			rule := relabel.DefaultRelabelConfig
			rule.TargetLabel = "__convert_classic_histograms_to_nhcb__"
			rule.Replacement = "true"
			promCfg.ScrapeConfigs[0].RelabelConfigs = []*relabel.Config{&rule}
			if tc.discover {
				promCfg.ScrapeConfigs[0].ServiceDiscoveryConfigs[0].(discovery.StaticConfig)[0].Labels = model.LabelSet{"__convert_classic_histograms_to_nhcb__": "true"}
				promCfg.ScrapeConfigs[0].RelabelConfigs = nil
			}
			if tc.drop {
				drop := relabel.DefaultRelabelConfig
				drop.Action = relabel.Drop
				promCfg.ScrapeConfigs[0].MetricRelabelConfigs = []*relabel.Config{&drop}
			}
			r, sink := newTestReceiver(t, &Config{PrometheusConfig: promCfg, PreserveCreatedMetrics: tc.preserve, skipOffsetting: true})
			wantUp := float64(1)
			if tc.code != 200 {
				wantUp = 0
			}
			require.Eventually(t, func() bool {
				for _, batch := range sink.AllMetrics() {
					for i := 0; i < batch.ResourceMetrics().Len(); i++ {
						for _, metric := range getMetrics(batch.ResourceMetrics().At(i)) {
							if metric.Name() == "up" && metric.Gauge().DataPoints().At(0).DoubleValue() == wantUp {
								return true
							}
						}
					}
				}
				return false
			}, 3*time.Second, 10*time.Millisecond, "retain generated scrape-health reports")
			targets := flattenTargets(r.scrapeManager.TargetsAll())
			require.Len(t, targets, 1)
			if tc.code == 200 {
				require.NoError(t, targets[0].LastError())
			} else {
				require.Error(t, targets[0].LastError())
			}
			wantConversion := "true"
			if tc.preserve {
				wantConversion = "false"
			}
			require.Equal(t, wantConversion, targets[0].GetValue("__convert_classic_histograms_to_nhcb__"))
			if tc.discover {
				require.Empty(t, promCfg.ScrapeConfigs[0].RelabelConfigs, "leave the supplied configuration unchanged")
			} else {
				require.Len(t, promCfg.ScrapeConfigs[0].RelabelConfigs, 1, "leave the supplied configuration unchanged")
				require.Equal(t, "true", promCfg.ScrapeConfigs[0].RelabelConfigs[0].Replacement)
			}
			for _, batch := range sink.AllMetrics() {
				for i := 0; i < batch.ResourceMetrics().Len(); i++ {
					metrics := map[string]pmetric.Metric{}
					for _, metric := range getMetrics(batch.ResourceMetrics().At(i)) {
						metrics[metric.Name()] = metric
					}
					require.Contains(t, metrics, "scrape_duration_seconds")
					require.Contains(t, metrics, "scrape_samples_scraped")
					if tc.name == "source" || tc.name == "discovery" {
						require.Contains(t, metrics, "latency_created")
						assert.Equal(t, float64(0.5), metrics["latency_created"].Gauge().DataPoints().At(0).DoubleValue())
						require.Contains(t, metrics, "latency")
						assert.Equal(t, pmetric.MetricTypeHistogram, metrics["latency"].Type())
					} else {
						require.NotContains(t, metrics, "latency_created")
					}
				}
			}
		})
	}
}
