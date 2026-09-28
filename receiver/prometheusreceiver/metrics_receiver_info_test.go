// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusreceiver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
)

func TestPreserveInfoMetrics(t *testing.T) {
	const body = `# HELP target_info Source target metadata.
# TYPE target_info gauge
target_info{source_owner="sdk"} 1
# HELP otel_scope_info Source scope metadata.
# TYPE otel_scope_info gauge
otel_scope_info{library="client",sawmills_preserved_scope_name="sdk/client"} 1
# HELP ordinary_metric An ordinary metric.
# TYPE ordinary_metric gauge
ordinary_metric 7
`
	for _, preserve := range []bool{false, true} {
		for _, format := range []struct {
			name        string
			openMetrics bool
			infoType    bool
		}{{"prometheus", false, false}, {"openmetrics-gauge", true, false}, {"openmetrics-info", true, true}} {
			t.Run(fmt.Sprintf("preserve=%t/%s", preserve, format.name), func(t *testing.T) {
				page := body
				if format.infoType {
					for _, name := range []string{"target", "otel_scope"} {
						page = strings.ReplaceAll(page, "# HELP "+name+"_info ", "# HELP "+name+" ")
						page = strings.ReplaceAll(page, "# TYPE "+name+"_info gauge", "# TYPE "+name+" info")
					}
				}
				if format.openMetrics {
					page += "# EOF\n"
				}
				target := &testData{
					name:  "info-source",
					pages: []mockPrometheusResponse{{code: 200, data: page, useOpenMetrics: format.openMetrics}},
					validateFunc: func(t *testing.T, td *testData, results []pmetric.ResourceMetrics) {
						verifyNumValidScrapeResults(t, td, results)
						metrics := make(map[string]pmetric.Metric)
						for _, metric := range getMetrics(results[0]) {
							metrics[metric.Name()] = metric
						}
						require.Contains(t, metrics, "ordinary_metric")
						assert.Equal(t, float64(7), metrics["ordinary_metric"].Gauge().DataPoints().At(0).DoubleValue())
						owner, enriched := results[0].Resource().Attributes().Get("source_owner")
						if !preserve {
							assert.NotContains(t, metrics, "target_info")
							assert.NotContains(t, metrics, "otel_scope_info")
							require.True(t, enriched)
							assert.Equal(t, "sdk", owner.Str())
							return
						}
						assert.False(t, enriched, "source info must remain a metric rather than enrich other metrics")
						attributes := make(map[string]map[string]any)
						for name, help := range map[string]string{"target_info": "Source target metadata.", "otel_scope_info": "Source scope metadata."} {
							require.Contains(t, metrics, name)
							metric := metrics[name]
							assert.Equal(t, help, metric.Description())
							var points pmetric.NumberDataPointSlice
							expectedKind := "gauge"
							if format.infoType {
								require.Equal(t, pmetric.MetricTypeSum, metric.Type())
								assert.False(t, metric.Sum().IsMonotonic())
								points = metric.Sum().DataPoints()
								expectedKind = "info"
							} else {
								require.Equal(t, pmetric.MetricTypeGauge, metric.Type())
								points = metric.Gauge().DataPoints()
							}
							kind, ok := metric.Metadata().Get(prometheus.MetricMetadataTypeKey)
							require.True(t, ok)
							assert.Equal(t, expectedKind, kind.Str())
							require.Equal(t, 1, points.Len())
							assert.Equal(t, float64(1), points.At(0).DoubleValue())
							attributes[name] = points.At(0).Attributes().AsRaw()
						}
						assert.Equal(t, map[string]any{"source_owner": "sdk"}, attributes["target_info"])
						assert.Equal(t, map[string]any{"library": "client", "sawmills_preserved_scope_name": "sdk/client"}, attributes["otel_scope_info"])
					},
				}
				testComponent(t, []*testData{target}, func(cfg *Config) { cfg.PreserveInfoMetrics = preserve })
			})
		}
	}
}
