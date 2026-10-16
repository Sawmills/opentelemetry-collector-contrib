// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
)

type durableMetricDestination struct {
	pmetricotlp.UnimplementedGRPCServer
	blocked  atomic.Bool
	partial  atomic.Int64
	attempts atomic.Int64
	mu       sync.Mutex
	ids      []int64
}

func (d *durableMetricDestination) Export(_ context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	d.attempts.Add(1)
	if d.blocked.Load() {
		return pmetricotlp.NewExportResponse(), status.Error(codes.Unavailable, "backend unavailable")
	}
	if rejected := d.partial.Load(); rejected > 0 {
		response := pmetricotlp.NewExportResponse()
		response.PartialSuccess().SetRejectedDataPoints(rejected)
		response.PartialSuccess().SetErrorMessage("test terminal metric rejection")
		return response, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, rm := range req.Metrics().ResourceMetrics().All() {
		for _, sm := range rm.ScopeMetrics().All() {
			for _, m := range sm.Metrics().All() {
				for _, point := range m.Gauge().DataPoints().All() {
					id, _ := point.Attributes().Get("test.id")
					d.ids = append(d.ids, id.Int())
				}
			}
		}
	}
	return pmetricotlp.NewExportResponse(), nil
}

func TestDurableCentralQueueRecoversAcceptedMetricsAfterProcessKill(t *testing.T) {
	runDurableMetricRecovery(t, "")
}

func TestDurableCentralQueueMetricsSurviveSecondKillDuringReplay(t *testing.T) {
	runDurableMetricRecovery(t, "replay-crash")
}

func TestDurableCentralQueueDoesNotReplayAcknowledgedMetrics(t *testing.T) {
	runDurableMetricRecovery(t, "acknowledged")
}

func TestDurableCentralQueueRetainsMetricsAtShutdownDeadline(t *testing.T) {
	runDurableMetricRecovery(t, "graceful")
}

func TestDurableCentralQueueMetricsReplaySkipsEmptyResources(t *testing.T) {
	runDurableMetricRecovery(t, "empty-resources")
}

func runDurableMetricRecovery(t *testing.T, scenario string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableMetricDestination{}
	destination.blocked.Store(true)
	server := grpc.NewServer()
	pmetricotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	child := func(mode string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDurableCentralQueueMetricsProcess$", "-test.timeout=15s") // #nosec G204 -- Runs only this test binary with fixed arguments.
		cmd.Env = append(os.Environ(), "SAW13194_METRIC_STORE="+dir, "SAW13194_METRIC_MODE="+mode, "SAW13194_METRIC_ENDPOINT="+listener.Addr().String())
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd
	}
	seedMode := "seed"
	switch scenario {
	case "graceful":
		seedMode = "seed-graceful"
	case "empty-resources":
		seedMode = "seed-empty-resources"
	}
	seed := child(seedMode)
	require.NoError(t, seed.Start())
	t.Cleanup(func() {
		if seed.ProcessState == nil {
			_ = seed.Process.Kill()
			_ = seed.Wait()
		}
	})
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "accepted"))
		return err == nil && string(b) == "8"
	}, 5*time.Second, 10*time.Millisecond)
	if scenario == "graceful" {
		require.NoError(t, seed.Wait())
	} else {
		require.NoError(t, seed.Process.Kill())
		require.Error(t, seed.Wait())
	}
	if scenario == "replay-crash" {
		err := child("replay-crash").Run()
		var exited *exec.ExitError
		require.ErrorAs(t, err, &exited)
		require.Equal(t, 42, exited.ExitCode())
	}
	destination.blocked.Store(false)
	recoverMode := "recover"
	if scenario == "acknowledged" {
		recoverMode = "recover-drain"
	}
	recovered := child(recoverMode)
	require.NoError(t, recovered.Start())
	t.Cleanup(func() { _ = recovered.Process.Kill(); _ = recovered.Wait() })
	var ids []int64
	require.Eventually(t, func() bool {
		destination.mu.Lock()
		defer destination.mu.Unlock()
		ids = append([]int64(nil), destination.ids...)
		return len(ids) >= 8
	}, 3*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids)
	if scenario == "acknowledged" {
		require.NoError(t, recovered.Wait())
		require.NoError(t, child("recover-drain").Run())
		destination.mu.Lock()
		ids = append([]int64(nil), destination.ids...)
		destination.mu.Unlock()
		require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids, "committed metric acknowledgement must survive reopening")
	}
}

func TestDurableCentralQueueMetricsProcess(t *testing.T) {
	dir := os.Getenv("SAW13194_METRIC_STORE")
	if dir == "" {
		t.Skip("subprocess only")
	}
	storageFactory := filestorage.NewFactory()
	storageConfig := storageFactory.CreateDefaultConfig().(*filestorage.Config)
	storageConfig.Directory = dir
	storageConfig.FSync = true
	storageConfig.Recreate = false
	ext, err := storageFactory.Create(t.Context(), extensiontest.NewNopSettings(storageFactory.Type()), storageConfig)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{os.Getenv("SAW13194_METRIC_ENDPOINT")}}},
		"central_queue": map[string]any{"enabled": true, "storage": "file_storage/queue", "shutdown_timeout": "20ms", "max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 512, "max_batch_delay": "10ms", "num_consumers": 2, "lane_count": 1},
	}).Unmarshal(cfg))
	if os.Getenv("SAW13194_METRIC_MODE") == "recover-drain" {
		cfg.(*loadbalancingexporter.Config).CentralQueue.ShutdownTimeout = 3 * time.Second
	}
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
	exp, err := factory.CreateMetrics(t.Context(), settings, cfg)
	require.NoError(t, err)
	var storageExtension component.Component = ext
	if os.Getenv("SAW13194_METRIC_MODE") == "replay-crash" {
		storageExtension = replayCrashStorage{ext.(storage.Extension)}
	}
	require.NoError(t, exp.Start(t.Context(), durableTestHost{extensions: map[component.ID]component.Component{component.MustNewIDWithName("file_storage", "queue"): storageExtension}}))
	if os.Getenv("SAW13194_METRIC_MODE") == "recover-drain" {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		require.NoError(t, exp.Shutdown(ctx))
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	if os.Getenv("SAW13194_METRIC_MODE") == "seed" || os.Getenv("SAW13194_METRIC_MODE") == "seed-graceful" || os.Getenv("SAW13194_METRIC_MODE") == "seed-empty-resources" {
		metrics := pmetric.NewMetrics()
		resource := metrics.ResourceMetrics().AppendEmpty()
		resource.Resource().Attributes().PutStr("service.name", "durable-test")
		metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		metric.SetName("durable.test")
		points := metric.SetEmptyGauge().DataPoints()
		for i := range 8 {
			point := points.AppendEmpty()
			point.Attributes().PutInt("test.id", int64(i))
			point.SetIntValue(int64(i))
		}
		if os.Getenv("SAW13194_METRIC_MODE") == "seed-empty-resources" {
			empty := pmetric.NewMetrics()
			empty.ResourceMetrics().AppendEmpty().Resource().Attributes().PutStr("service.name", "empty-only")
			require.NoError(t, exp.ConsumeMetrics(t.Context(), empty))
			resource := metrics.ResourceMetrics().AppendEmpty()
			resource.Resource().Attributes().PutStr("service.name", "empty-resource")
			resource = metrics.ResourceMetrics().AppendEmpty()
			resource.Resource().Attributes().PutStr("service.name", "empty-gauge")
			metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			metric.SetName("empty.gauge")
			metric.SetEmptyGauge()
		}
		require.NoError(t, exp.ConsumeMetrics(t.Context(), metrics))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "accepted"), []byte("8"), 0o600))
		if os.Getenv("SAW13194_METRIC_MODE") == "seed-graceful" {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			require.ErrorIs(t, exp.Shutdown(ctx), context.DeadlineExceeded)
			require.NoError(t, ext.Shutdown(t.Context()))
			return
		}
	}
	select {}
}

func durableMetricInput(ids ...int64) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	resource := metrics.ResourceMetrics().AppendEmpty()
	resource.Resource().Attributes().PutStr("service.name", "durable-test")
	metric := resource.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("durable.test")
	points := metric.SetEmptyGauge().DataPoints()
	for _, id := range ids {
		point := points.AppendEmpty()
		point.Attributes().PutInt("test.id", id)
		point.SetIntValue(id)
	}
	return metrics
}

func TestDurableCentralQueueRecordsMetricPartialSuccessAsTerminalRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableMetricDestination{}
	destination.partial.Store(2)
	server := grpc.NewServer()
	pmetricotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	exp, store, reader := newDurableTestMetricsExporter(t, dir, listener.Addr().String())
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		_ = store.Shutdown(context.WithoutCancel(t.Context()))
	})
	require.NoError(t, exp.ConsumeMetrics(t.Context(), durableMetricInput(0, 1)))
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items") == 2
	}, time.Second, 10*time.Millisecond)
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	require.NoError(t, exp.Shutdown(t.Context()))
	require.NoError(t, store.Shutdown(t.Context()))
	attempts := destination.attempts.Load()
	destination.partial.Store(0)
	reopened, reopenedStore, reopenedReader := newDurableTestMetricsExporter(t, dir, listener.Addr().String())
	require.Equal(t, int64(2), durableMetricValue(t, reopenedReader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items"))
	require.NoError(t, reopened.Shutdown(t.Context()))
	require.NoError(t, reopenedStore.Shutdown(t.Context()))
	require.Equal(t, attempts, destination.attempts.Load(), "OTLP partial metric success must not be retried")
}

func TestDurableCentralQueueSeparatesSignalStoresWithSameExporterIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	logsDestination := &durableTestDestination{}
	metricsDestination := &durableMetricDestination{}
	logsDestination.blocked.Store(true)
	metricsDestination.blocked.Store(true)
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, logsDestination)
	pmetricotlp.RegisterGRPCServer(server, metricsDestination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	logs, logsStore, _ := newDurableTestExporter(t, dir, listener.Addr().String())
	metrics, metricsStore, _ := newDurableTestMetricsExporter(t, dir, listener.Addr().String())
	t.Cleanup(func() {
		_ = logs.Shutdown(context.WithoutCancel(t.Context()))
		_ = metrics.Shutdown(context.WithoutCancel(t.Context()))
		_ = logsStore.Shutdown(context.WithoutCancel(t.Context()))
		_ = metricsStore.Shutdown(context.WithoutCancel(t.Context()))
	})
	input := plog.NewLogs()
	record := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Attributes().PutInt("test.id", 101)
	record.Body().SetStr("independent durable log")
	require.NoError(t, logs.ConsumeLogs(t.Context(), input))
	require.NoError(t, metrics.ConsumeMetrics(t.Context(), durableMetricInput(202)))
	require.ErrorIs(t, logs.Shutdown(t.Context()), context.DeadlineExceeded)
	require.ErrorIs(t, metrics.Shutdown(t.Context()), context.DeadlineExceeded)
	require.NoError(t, logsStore.Shutdown(t.Context()))
	require.NoError(t, metricsStore.Shutdown(t.Context()))
	logsDestination.blocked.Store(false)
	metricsDestination.blocked.Store(false)
	recoveredLogs, logsStore, logsReader := newDurableTestExporter(t, dir, listener.Addr().String())
	recoveredMetrics, metricsStore, metricsReader := newDurableTestMetricsExporter(t, dir, listener.Addr().String())
	require.Eventually(t, func() bool {
		return durableMetricValue(t, logsReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0 && durableMetricValue(t, metricsReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, recoveredLogs.Shutdown(t.Context()))
	require.NoError(t, recoveredMetrics.Shutdown(t.Context()))
	require.NoError(t, logsStore.Shutdown(t.Context()))
	require.NoError(t, metricsStore.Shutdown(t.Context()))
	logsDestination.mu.Lock()
	logIDs := append([]int64(nil), logsDestination.ids...)
	logsDestination.mu.Unlock()
	metricsDestination.mu.Lock()
	metricIDs := append([]int64(nil), metricsDestination.ids...)
	metricsDestination.mu.Unlock()
	require.ElementsMatch(t, []int64{101}, logIDs)
	require.ElementsMatch(t, []int64{202}, metricIDs)
}
