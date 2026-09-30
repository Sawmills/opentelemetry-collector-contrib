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
	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
)

func TestTranslationStrategyValidation(t *testing.T) {
	t.Cleanup(testutil.SetFeatureGateForTest(t, enableSendingRW2FeatureGate, true))
	for _, tc := range []struct {
		strategy otlptranslator.TranslationStrategyOption
		protocol remoteapi.WriteMessageType
		wantErr  string
	}{
		{"", remoteapi.WriteV1MessageType, ""},
		{otlptranslator.UnderscoreEscapingWithSuffixes, remoteapi.WriteV1MessageType, ""},
		{otlptranslator.UnderscoreEscapingWithoutSuffixes, remoteapi.WriteV1MessageType, ""},
		{otlptranslator.NoTranslation, remoteapi.WriteV1MessageType, ""},
		{otlptranslator.NoUTF8EscapingWithSuffixes, remoteapi.WriteV1MessageType, ""},
		{otlptranslator.NoTranslation, remoteapi.WriteV2MessageType, ""},
		{otlptranslator.NoUTF8EscapingWithSuffixes, remoteapi.WriteV2MessageType, ""},
		{"misspelled", remoteapi.WriteV2MessageType, "invalid translation_strategy"},
	} {
		t.Run(string(tc.strategy)+string(tc.protocol), func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.ClientConfig.Endpoint = "http://localhost:9090/api/v1/write"
			cfg.TranslationStrategy = tc.strategy
			cfg.RemoteWriteProtoMsg = tc.protocol
			if tc.wantErr == "" {
				require.NoError(t, cfg.Validate())
			} else {
				require.ErrorContains(t, cfg.Validate(), tc.wantErr)
			}
		})
	}
}

func TestNoTranslationRemoteWriteV2(t *testing.T) {
	t.Cleanup(testutil.SetFeatureGateForTest(t, enableSendingRW2FeatureGate, true))
	var received []byte
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append([]byte(nil), body...)
		mu.Unlock()
		w.Header().Set("X-Prometheus-Remote-Write-Samples-Written", "1")
		w.Header().Set("X-Prometheus-Remote-Write-Histograms-Written", "0")
		w.Header().Set("X-Prometheus-Remote-Write-Exemplars-Written", "0")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"endpoint":             server.URL,
		"translation_strategy": "NoTranslation",
		"protobuf_message":     "io.prometheus.write.v2.Request",
		"add_metric_suffixes":  true,
		"target_info":          map[string]any{"enabled": false},
	}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	exp, err := newPRWExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(t.Context())) })
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("rpc.server.requests")
	m.SetDescription("Scraped request count")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(2e9)
	dp.SetStartTimestamp(1e9)
	dp.SetIntValue(3)
	dp.Attributes().PutStr("rpc.method", "CreateInsight")
	dp.Attributes().PutStr("rpc_method", "separate_source_label")
	require.NoError(t, exp.PushMetrics(t.Context(), metrics))
	mu.Lock()
	body := append([]byte(nil), received...)
	mu.Unlock()
	require.NotEmpty(t, body)
	decoded, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	var request writev2.Request
	require.NoError(t, request.Unmarshal(decoded))
	require.Len(t, request.Timeseries, 1)
	ts := request.Timeseries[0]
	labels := make(map[string]string)
	for i := 0; i < len(ts.LabelsRefs); i += 2 {
		labels[request.Symbols[ts.LabelsRefs[i]]] = request.Symbols[ts.LabelsRefs[i+1]]
	}
	require.Equal(t, "rpc.server.requests", labels["__name__"])
	require.Equal(t, "CreateInsight", labels["rpc.method"])
	require.Equal(t, "separate_source_label", labels["rpc_method"])
	require.Equal(t, writev2.Metadata_METRIC_TYPE_COUNTER, ts.Metadata.Type)
	require.Equal(t, "Scraped request count", request.Symbols[ts.Metadata.HelpRef])
}

func TestNoTranslationRemoteWriteV1(t *testing.T) {
	var requests []prompb.WriteRequest
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var request prompb.WriteRequest
		if err := request.Unmarshal(decoded); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"endpoint":             server.URL,
		"translation_strategy": "NoTranslation",
		"add_metric_suffixes":  true,
		"send_metadata":        true,
		"external_labels":      map[string]any{"source.name": "source", "source_name": "separate"},
		"target_info":          map[string]any{"enabled": false},
	}).Unmarshal(cfg))
	require.NoError(t, cfg.Validate())
	exp, err := newPRWExporter(cfg, exportertest.NewNopSettings(metadata.Type))
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(t.Context())) })
	metrics := pmetric.NewMetrics()
	m := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("rpc.server.requests")
	m.SetDescription("Scraped request count")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(2e9)
	dp.SetStartTimestamp(1e9)
	dp.SetIntValue(3)
	dp.Attributes().PutStr("rpc.method", "CreateInsight")
	dp.Attributes().PutStr("rpc_method", "separate_source_label")
	require.NoError(t, exp.PushMetrics(t.Context(), metrics))
	mu.Lock()
	received := append([]prompb.WriteRequest(nil), requests...)
	mu.Unlock()
	var series []prompb.TimeSeries
	var metadataRows []prompb.MetricMetadata
	for _, request := range received {
		series = append(series, request.Timeseries...)
		metadataRows = append(metadataRows, request.Metadata...)
	}
	require.Len(t, series, 1)
	labels := make(map[string]string)
	for _, label := range series[0].Labels {
		labels[label.Name] = label.Value
	}
	require.Equal(t, "rpc.server.requests", labels["__name__"])
	require.Equal(t, "CreateInsight", labels["rpc.method"])
	require.Equal(t, "separate_source_label", labels["rpc_method"])
	require.Equal(t, "source", labels["source.name"])
	require.Equal(t, "separate", labels["source_name"])
	require.Len(t, metadataRows, 1)
	require.Equal(t, "rpc.server.requests", metadataRows[0].MetricFamilyName)
	require.Equal(t, prompb.MetricMetadata_COUNTER, metadataRows[0].Type)
	require.Equal(t, "Scraped request count", metadataRows[0].Help)
}
