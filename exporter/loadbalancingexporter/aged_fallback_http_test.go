// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func TestCentralQueueAgedFallbackHTTPPartialResponseRetainsOwnership(t *testing.T) {
	for _, encoding := range []string{"proto", "json"} {
		t.Run(encoding, func(t *testing.T) {
			var partial atomic.Bool
			partial.Store(true)
			var mu sync.Mutex
			var accepted []int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body io.Reader = r.Body
				if r.Header.Get("Content-Encoding") == "gzip" {
					reader, err := gzip.NewReader(r.Body)
					if err != nil {
						http.Error(w, "invalid compressed fixture request", http.StatusBadRequest)
						return
					}
					defer reader.Close()
					body = reader
				}
				data, err := io.ReadAll(body)
				request := plogotlp.NewExportRequest()
				if err == nil {
					if encoding == "json" {
						err = request.UnmarshalJSON(data)
					} else {
						err = request.UnmarshalProto(data)
					}
				}
				if err != nil {
					http.Error(w, "invalid fixture request", http.StatusBadRequest)
					return
				}
				response := plogotlp.NewExportResponse()
				if partial.Load() {
					response.PartialSuccess().SetRejectedLogRecords(1)
					response.PartialSuccess().SetErrorMessage("synthetic refusal")
				} else {
					mu.Lock()
					for _, resource := range request.Logs().ResourceLogs().All() {
						for _, scope := range resource.ScopeLogs().All() {
							for _, record := range scope.LogRecords().All() {
								id, _ := record.Attributes().Get("test.id")
								accepted = append(accepted, id.Int())
							}
						}
					}
					mu.Unlock()
				}
				if encoding == "json" {
					w.Header().Set("Content-Type", "application/json")
					data, err = response.MarshalJSON()
				} else {
					w.Header().Set("Content-Type", "application/x-protobuf")
					data, err = response.MarshalProto()
				}
				if err != nil {
					http.Error(w, "invalid fixture response", http.StatusInternalServerError)
					return
				}
				_, _ = w.Write(data)
			}))
			t.Cleanup(server.Close)
			factory := otlphttpexporter.NewFactory()
			store := newAgedFallbackStorage(t, t.TempDir())
			exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, &agedFallbackWorker{}), server.URL, 30*time.Millisecond, true,
				agedFallbackSettings{storage: store, factory: factory, queue: map[string]any{
					"fallback_exporter": factory.Type().String() + "/customer",
					"fallback_config":   map[string]any{"endpoint": server.URL, "encoding": encoding, "timeout": "100ms"},
				}})
			require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(881, 882)))
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_failed") >= 2 }, 3*time.Second, time.Millisecond)
			require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
			require.Zero(t, agedFallbackRecords(t, reader, "max_retry_age"))
			partial.Store(false)
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "max_retry_age") == 2 }, 3*time.Second, time.Millisecond)
			require.NoError(t, exp.Shutdown(t.Context()))
			require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
			mu.Lock()
			defer mu.Unlock()
			require.ElementsMatch(t, []int64{881, 882}, accepted)
		})
	}
}

func TestCentralQueueAgedFallbackHTTPUncertainAcknowledgementRetainsOwnership(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		contentType string
		body        string
		truncated   bool
	}{
		{name: "unavailable", status: http.StatusServiceUnavailable},
		{name: "empty JSON", status: http.StatusOK, contentType: "application/json"},
		{name: "empty unknown format", status: http.StatusOK, contentType: "application/octet-stream"},
		{name: "empty missing content type", status: http.StatusOK},
		{name: "trailing JSON data", status: http.StatusOK, contentType: "application/json", body: "{}garbage"},
		{name: "multiple JSON values", status: http.StatusOK, contentType: "application/json", body: "{}{}"},
		{name: "null JSON", status: http.StatusOK, contentType: "application/json", body: "null"},
		{name: "invalid protobuf", status: http.StatusOK, contentType: "application/x-protobuf", body: "\xff"},
		{name: "unknown format", status: http.StatusOK, contentType: "application/octet-stream", body: "unrecognized acknowledgement"},
		{name: "oversized response", status: http.StatusOK, contentType: "application/json", body: strings.Repeat("x", 64*1024+1)},
		{name: "truncated response", status: http.StatusOK, contentType: "application/json", body: "{}", truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var invalid atomic.Bool
			invalid.Store(true)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if invalid.Load() {
					w.Header().Set("Content-Type", tc.contentType)
					if tc.truncated {
						w.Header().Set("Content-Length", "10")
					}
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			factory := otlphttpexporter.NewFactory()
			store := newAgedFallbackStorage(t, t.TempDir())
			exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, &agedFallbackWorker{}), server.URL, 30*time.Millisecond, true,
				agedFallbackSettings{storage: store, factory: factory, queue: map[string]any{
					"fallback_exporter": factory.Type().String() + "/customer",
					"fallback_config":   map[string]any{"endpoint": server.URL, "timeout": "100ms"},
				}})
			require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(891, 892)))
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_failed") >= 2 }, 3*time.Second, time.Millisecond)
			require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
			require.Zero(t, agedFallbackRecords(t, reader, "max_retry_age"))
			invalid.Store(false)
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "max_retry_age") == 2 }, 3*time.Second, time.Millisecond)
			require.NoError(t, exp.Shutdown(t.Context()))
			require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
		})
	}
}
