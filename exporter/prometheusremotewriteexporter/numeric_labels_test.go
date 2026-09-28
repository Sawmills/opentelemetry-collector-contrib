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

func TestNumericLabelFormatRemoteWrite(t *testing.T) {
	for _, mode := range []string{"default", "go", "decimal", "openmetrics"} {
		t.Run(mode, func(t *testing.T) {
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
			config := map[string]any{
				"endpoint": server.URL, "use_go_float_format": mode == "go", "send_metadata": true,
				"add_metric_suffixes": false, "target_info": map[string]any{"enabled": false},
			}
			if mode == "openmetrics" {
				config["use_openmetrics_float_format"] = true
			}
			if mode == "decimal" {
				config["use_decimal_float_format"] = true
			}
			require.NoError(t, confmap.NewFromStringMap(config).Unmarshal(cfg))
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
			p.SetCount(4)
			p.SetSum(5)
			p.Attributes().PutStr("source", "app")
			p.ExplicitBounds().FromRaw([]float64{1, 1e6})
			p.BucketCounts().FromRaw([]uint64{1, 2, 1})
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
			want, one := "1000000", "1"
			switch mode {
			case "go":
				want = "1e+06"
			case "decimal":
				want, one = "1000000.0", "1.0"
			case "openmetrics":
				want, one = "1e+06", "1.0"
			}
			require.Equal(t, map[string]float64{one: 1, want: 3, "+Inf": 4}, bounds)
		})
	}
}

func TestNumericLabelFormatsMutuallyExclusive(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.False(t, cfg.UseGoFloatFormat)
	require.False(t, cfg.UseDecimalFloatFormat)
	require.False(t, cfg.UseOpenMetricsFloatFormat)
	cfg.UseGoFloatFormat = true
	cfg.UseDecimalFloatFormat = true
	require.EqualError(t, cfg.Validate(), "use_go_float_format and use_decimal_float_format are mutually exclusive")
}

func TestOpenMetricsFormatMutuallyExclusive(t *testing.T) {
	for _, pair := range []struct{ goFormat, decimalFormat bool }{{true, false}, {false, true}, {true, true}} {
		cfg := createDefaultConfig().(*Config)
		cfg.UseOpenMetricsFloatFormat = true
		cfg.UseGoFloatFormat, cfg.UseDecimalFloatFormat = pair.goFormat, pair.decimalFormat
		require.EqualError(t, cfg.Validate(), "use_openmetrics_float_format is mutually exclusive with use_go_float_format and use_decimal_float_format")
	}
}
