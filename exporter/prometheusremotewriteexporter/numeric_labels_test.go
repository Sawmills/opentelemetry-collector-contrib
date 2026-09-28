// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewriteexporter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter/internal/metadata"
)

func TestGoFloatFormatRemoteWrite(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "go"}[enabled], func(t *testing.T) {
			var requests [][]byte
			var requestMu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				requestMu.Lock()
				requests = append(requests, body)
				requestMu.Unlock()
			}))
			defer server.Close()
			cfg := createDefaultConfig().(*Config)
			require.NoError(t, confmap.NewFromStringMap(map[string]any{
				"endpoint": server.URL, "use_go_float_format": enabled, "send_metadata": true,
				"add_metric_suffixes": false, "target_info": map[string]any{"enabled": false},
			}).Unmarshal(cfg))
			require.NoError(t, cfg.Validate())
			exp, err := newPRWExporter(cfg, exportertest.NewNopSettings(metadata.Type))
			require.NoError(t, err)
			require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
			t.Cleanup(func() { require.NoError(t, exp.Shutdown(t.Context())) })
			metrics := pmetric.NewMetrics()
			m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("latency")
			m.SetDescription("Histogram help")
			h := m.SetEmptyHistogram()
			h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			p := h.DataPoints().AppendEmpty()
			p.SetTimestamp(1e9)
			p.SetCount(3)
			p.SetSum(5)
			p.Attributes().PutStr("source", "app")
			p.ExplicitBounds().FromRaw([]float64{1e6})
			p.BucketCounts().FromRaw([]uint64{2, 1})
			require.NoError(t, exp.PushMetrics(t.Context(), metrics))
			requestMu.Lock()
			received := append([][]byte(nil), requests...)
			requestMu.Unlock()
			require.NotEmpty(t, received)
			var request prompb.WriteRequest
			for _, body := range received {
				decoded, err := snappy.Decode(nil, body)
				require.NoError(t, err)
				var batch prompb.WriteRequest
				require.NoError(t, batch.Unmarshal(decoded))
				request.Timeseries = append(request.Timeseries, batch.Timeseries...)
				request.Metadata = append(request.Metadata, batch.Metadata...)
			}
			require.NotEmpty(t, request.Metadata)
			for _, m := range request.Metadata {
				require.Equal(t, prompb.MetricMetadata_HISTOGRAM, m.Type)
				require.Equal(t, "Histogram help", m.Help)
			}
			bounds := map[string]float64{}
			for _, series := range request.Timeseries {
				require.Contains(t, series.Labels, prompb.Label{Name: "source", Value: "app"})
				for _, label := range series.Labels {
					if label.Name == "le" {
						bounds[label.Value] = series.Samples[0].Value
					}
				}
			}
			want := "1000000"
			if enabled {
				want = "1e+06"
			}
			require.Equal(t, map[string]float64{want: 2, "+Inf": 3}, bounds)
		})
	}
}
