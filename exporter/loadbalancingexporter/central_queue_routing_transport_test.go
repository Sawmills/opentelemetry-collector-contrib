// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
)

type queueRoutingSlowWorker struct {
	plogotlp.UnimplementedGRPCServer
	started     chan struct{}
	release     <-chan struct{}
	once        sync.Once
	destination durableTestDestination
}

func (w *queueRoutingSlowWorker) Export(ctx context.Context, request plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.release:
		return w.destination.Export(ctx, request)
	case <-ctx.Done():
		return plogotlp.NewExportResponse(), ctx.Err()
	}
}

func newQueueRoutingTransportExporter(t *testing.T, endpoints []string) exporter.Logs {
	t.Helper()
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":    map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}, "timeout": "5s"}},
		"resolver":    map[string]any{"static": map[string]any{"hostnames": endpoints}},
		"log_routing": map[string]any{"ignore_trace_id": true, "record_striping_enabled": true},
		"endpoint_health": map[string]any{
			"enabled": true, "reroute_on_failure": true, "min_eligible_backends": 2,
			"max_quarantined_percent": 100,
		},
		"central_queue": map[string]any{
			"enabled": true, "max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1024,
			"max_inflight_uncompressed_bytes": 1 << 20, "target_compressed_bytes": 1,
			"max_batch_delay": "1ms", "num_consumers": 2, "shutdown_timeout": "100ms",
		},
	}).Unmarshal(cfg))
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	settings := exportertest.NewNopSettings(factory.Type())
	settings.Logger = zaptest.NewLogger(t)
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { _ = exp.Shutdown(context.Background()) })
	return exp
}

func TestLogsCentralQueueRealOTLPSpillsPastSlowWorker(t *testing.T) {
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	slow := &queueRoutingSlowWorker{started: make(chan struct{}), release: release}
	fast := &durableTestDestination{}
	exp := newQueueRoutingTransportExporter(t, []string{
		startAgedFallbackServer(t, slow), startAgedFallbackServer(t, fast),
	})
	t.Cleanup(finish)
	ids := make([]int64, 24)
	for i := range ids {
		ids[i] = int64(i)
	}
	input := connectionRecoveryInput(ids...)
	for _, record := range input.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().All() {
		record.Body().SetStr(strings.Repeat("x", 700))
	}
	require.NoError(t, exp.ConsumeLogs(t.Context(), input))
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("real OTLP slow worker did not receive its window")
	}
	require.Eventually(t, func() bool {
		fast.mu.Lock()
		defer fast.mu.Unlock()
		return len(fast.ids) > len(ids)/2
	}, time.Second, time.Millisecond, "idle worker must drain beyond its home lane while the other worker is blocked")
	finish()
	require.Eventually(t, func() bool {
		fast.mu.Lock()
		defer fast.mu.Unlock()
		slow.destination.mu.Lock()
		defer slow.destination.mu.Unlock()
		return len(fast.ids)+len(slow.destination.ids) == len(ids)
	}, 3*time.Second, time.Millisecond)
	fast.mu.Lock()
	slow.destination.mu.Lock()
	received := append(append([]int64(nil), fast.ids...), slow.destination.ids...)
	slow.destination.mu.Unlock()
	fast.mu.Unlock()
	require.ElementsMatch(t, ids, received)
}

func TestLogsCentralQueueRealOTLPReroutesUnderFailOpen(t *testing.T) {
	failed := &durableTestDestination{}
	failed.blocked.Store(true)
	healthy := &durableTestDestination{}
	exp := newQueueRoutingTransportExporter(t, []string{
		startAgedFallbackServer(t, failed), startAgedFallbackServer(t, healthy),
	})
	ids := []int64{101, 102, 103, 104, 105, 106}
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(ids...)))
	require.Eventually(t, func() bool {
		healthy.mu.Lock()
		defer healthy.mu.Unlock()
		return len(healthy.ids) == len(ids)
	}, 2*time.Second, time.Millisecond, "real OTLP retries must escape the unreachable worker under fail-open")
	require.GreaterOrEqual(t, failed.attempts.Load(), int64(3))
	healthy.mu.Lock()
	defer healthy.mu.Unlock()
	require.ElementsMatch(t, ids, healthy.ids)
}
