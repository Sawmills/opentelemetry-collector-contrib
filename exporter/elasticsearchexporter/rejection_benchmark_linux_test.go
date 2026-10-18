// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadatatest"
)

// BenchmarkRejectionDiagnosticsAccepted measures the public ConsumeLogs path,
// including accepted-item encoding, delivery and existing telemetry. The same
// source runs on the baseline and candidate. CPU time excludes compilation.
func BenchmarkRejectionDiagnosticsAccepted(b *testing.B) {
	benchmarkRejectionDiagnosticsAccepted(b, false)
}

func BenchmarkRejectionDiagnosticsAcceptedEnriched(b *testing.B) {
	benchmarkRejectionDiagnosticsAccepted(b, true)
}

func benchmarkRejectionDiagnosticsAccepted(b *testing.B, enriched bool) {
	const batchSize = 1000
	response := `{"errors":false,"items":[` + strings.Repeat(`{"create":{"status":201}},`, batchSize-1) + `{"create":{"status":201}}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/_bulk" {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, response)
			return
		}
		_, _ = io.WriteString(w, `{"version":{"number":"8.17.0"}}`)
	}))
	defer server.Close()
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	config.Endpoints = []string{server.URL}
	config.LogsIndex = "logs-benchmark"
	config.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	config.Retry.Enabled = false
	telemetry := componenttest.NewTelemetry()
	exp, err := factory.CreateLogs(b.Context(), metadatatest.NewSettings(telemetry), config)
	require.NoError(b, err)
	require.NoError(b, exp.Start(b.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(b, exp.Shutdown(context.WithoutCancel(b.Context()))) }()

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "synthetic-benchmark")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("benchmark")
	if enriched {
		// Represent Kubernetes-enriched records without using customer data.
		for i := range 48 {
			rl.Resource().Attributes().PutStr(fmt.Sprintf("synthetic.resource.%02d", i), "synthetic-resource-value")
		}
		for i := range 8 {
			sl.Scope().Attributes().PutStr(fmt.Sprintf("synthetic.scope.%02d", i), "synthetic-scope-value")
		}
	}
	for range batchSize {
		record := sl.LogRecords().AppendEmpty()
		record.Body().SetStr(strings.Repeat("synthetic accepted ", 32))
		record.Attributes().PutDouble("duration", 0.125)
		record.Attributes().PutStr("level", "info")
		record.Attributes().PutEmptyMap("error").PutStr("code", "none")
		record.Attributes().PutInt("sequence", 42)
		record.Attributes().PutBool("enabled", true)
		if enriched {
			for i := range 16 {
				record.Attributes().PutStr(fmt.Sprintf("synthetic.log.%02d", i), "synthetic-log-value")
			}
		}
	}
	logs.MarkReadOnly()
	ctx := client.NewContext(b.Context(), client.Info{Metadata: client.NewMetadata(map[string][]string{"X-Elastic-Mapping-Mode": {"ecs"}})})
	require.NoError(b, exp.ConsumeLogs(ctx, logs))
	var before, after syscall.Rusage
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &before))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := exp.ConsumeLogs(ctx, logs); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &after))
	cpu := after.Utime.Nano() + after.Stime.Nano() - before.Utime.Nano() - before.Stime.Nano()
	b.ReportMetric(float64(cpu)/float64(b.N*batchSize), "cpu-ns/item")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/item")
}
