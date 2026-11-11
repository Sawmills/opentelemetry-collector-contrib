// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"maps"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
)

type agedFallbackHost struct {
	component.Host
	target component.Component
}

type agedFallbackNonLogs struct{}

func (agedFallbackNonLogs) Start(context.Context, component.Host) error { return nil }
func (agedFallbackNonLogs) Shutdown(context.Context) error              { return nil }

func (h agedFallbackHost) GetExporters() map[pipeline.Signal]map[component.ID]component.Component {
	return map[pipeline.Signal]map[component.ID]component.Component{
		pipeline.SignalLogs: {component.MustNewIDWithName("otlp", "customer"): h.target},
	}
}

type agedFallbackWorker struct {
	plogotlp.UnimplementedGRPCServer
	attempts atomic.Int64
}

func (w *agedFallbackWorker) Export(context.Context, plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	w.attempts.Add(1)
	return plogotlp.NewExportResponse(), status.Error(codes.DeadlineExceeded, "stuck worker")
}

func startAgedFallbackServer(t *testing.T, server plogotlp.GRPCServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	plogotlp.RegisterGRPCServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return listener.Addr().String()
}

type agedFallbackSettings struct {
	storage component.Component
	queue   map[string]any
	logger  *zap.Logger
}

func newAgedFallbackExporter(t *testing.T, worker, destination string, age time.Duration, configured bool, options ...agedFallbackSettings) (exporter.Logs, *sdkmetric.ManualReader) {
	t.Helper()
	fallbackFactory := otlpexporter.NewFactory()
	fallbackConfig := fallbackFactory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"endpoint": destination, "tls": map[string]any{"insecure": true},
		"sending_queue":    map[string]any{"enabled": false},
		"retry_on_failure": map[string]any{"enabled": false}, "timeout": "100ms",
	}).Unmarshal(fallbackConfig))
	fallback, err := fallbackFactory.CreateLogs(t.Context(), exportertest.NewNopSettings(fallbackFactory.Type()), fallbackConfig)
	require.NoError(t, err)
	require.NoError(t, fallback.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, fallback.Shutdown(context.WithoutCancel(t.Context()))) })

	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	queue := map[string]any{
		"enabled": true, "max_retry_age": age.String(), "shutdown_timeout": "50ms",
		"max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1 << 20,
		"max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 1,
		"max_batch_delay": "1ms", "num_consumers": 1, "lane_count": 1,
	}
	if configured {
		queue["fallback_exporter"] = "otlp/customer"
	}
	host := agedFallbackHost{Host: componenttest.NewNopHost(), target: fallback}
	if len(options) > 0 {
		maps.Copy(queue, options[0].queue)
		if options[0].storage != nil {
			queue["storage"] = "file_storage/queue"
			host.Host = durableTestHost{extensions: map[component.ID]component.Component{
				component.MustNewIDWithName("file_storage", "queue"): options[0].storage,
			}}
		}
	}
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}, "timeout": "100ms"}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{worker}}},
		"central_queue": queue,
	}).Unmarshal(cfg))
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "aged-fallback")
	settings.Logger = zaptest.NewLogger(t)
	if len(options) > 0 && options[0].logger != nil {
		settings.Logger = options[0].logger
	}
	settings.MeterProvider = provider
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), host))
	t.Cleanup(func() { _ = exp.Shutdown(context.WithoutCancel(t.Context())) })
	return exp, reader
}

func TestCentralQueueAgedFallbackRefusalsRetainLogs(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "permanent refusal"}[permanent], func(t *testing.T) {
			worker := &agedFallbackWorker{}
			destination := &durableTestDestination{}
			if permanent {
				destination.permanent.Store(true)
			} else {
				destination.blocked.Store(true)
			}
			exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), 30*time.Millisecond, true)
			require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(811, 812)))
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_failed") >= 2 }, 3*time.Second, 5*time.Millisecond)
			require.Zero(t, agedFallbackRecords(t, reader, "max_retry_age"))
			destination.blocked.Store(false)
			destination.permanent.Store(false)
			require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "max_retry_age") == 2 }, 3*time.Second, 5*time.Millisecond)
			require.NoError(t, exp.Shutdown(t.Context()))
			destination.mu.Lock()
			defer destination.mu.Unlock()
			require.ElementsMatch(t, []int64{811, 812}, destination.ids)
			require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_shutdown_dropped_items"))
		})
	}
}

func TestCentralQueueAgedFallbackOmissionKeepsWorkerDelivery(t *testing.T) {
	worker := &durableTestDestination{}
	worker.blocked.Store(true)
	destination := &durableTestDestination{}
	exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), 30*time.Millisecond, false)
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(821)))
	require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_not_configured") > 0 }, 3*time.Second, 5*time.Millisecond)
	require.Zero(t, destination.attempts.Load())
	worker.blocked.Store(false)
	require.Eventually(t, func() bool {
		worker.mu.Lock()
		defer worker.mu.Unlock()
		return len(worker.ids) == 1
	}, 3*time.Second, 5*time.Millisecond)
	require.NoError(t, exp.Shutdown(t.Context()))
	require.Zero(t, agedFallbackRecords(t, reader, "max_retry_age"))
}

func TestCentralQueueAgedFallbackHealthyPath(t *testing.T) {
	worker := &durableTestDestination{}
	destination := &durableTestDestination{}
	exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), time.Minute, true)
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(831, 832)))
	require.Eventually(t, func() bool {
		worker.mu.Lock()
		defer worker.mu.Unlock()
		return len(worker.ids) == 2
	}, time.Second, time.Millisecond)
	require.NoError(t, exp.Shutdown(t.Context()))
	require.Zero(t, destination.attempts.Load())
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_fallback_records"))
}

func newAgedFallbackStorage(t *testing.T, dir string) component.Component {
	t.Helper()
	factory := filestorage.NewFactory()
	cfg := factory.CreateDefaultConfig().(*filestorage.Config)
	cfg.Directory = dir
	cfg.FSync = true
	store, err := factory.Create(t.Context(), extensiontest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, store.Shutdown(context.WithoutCancel(t.Context()))) })
	return store
}

func TestCentralQueueAgedFallbackDurableReplay(t *testing.T) {
	worker := &agedFallbackWorker{}
	destination := &durableTestDestination{}
	destination.blocked.Store(true)
	workerAddress := startAgedFallbackServer(t, worker)
	destinationAddress := startAgedFallbackServer(t, destination)
	dir := t.TempDir()
	store := newAgedFallbackStorage(t, dir)
	exp, reader := newAgedFallbackExporter(t, workerAddress, destinationAddress, 30*time.Millisecond, true, agedFallbackSettings{storage: store})
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(841, 842)))
	require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_failed") >= 2 }, 3*time.Second, time.Millisecond)
	require.Error(t, exp.Shutdown(t.Context()), "failed fallback must leave durable ownership")
	require.NoError(t, store.Shutdown(t.Context()))
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	destination.blocked.Store(false)
	workerAttempts := worker.attempts.Load()
	store2 := newAgedFallbackStorage(t, dir)
	reopened, reopenedReader := newAgedFallbackExporter(t, workerAddress, destinationAddress, 30*time.Millisecond, true, agedFallbackSettings{storage: store2})

	require.Eventually(t, func() bool { return agedFallbackRecords(t, reopenedReader, "max_retry_age") == 2 }, 3*time.Second, time.Millisecond)
	require.NoError(t, reopened.Shutdown(t.Context()))
	require.NoError(t, store2.Shutdown(t.Context()))
	require.Equal(t, workerAttempts, worker.attempts.Load(), "replay must preserve age and bypass the worker")
	require.Zero(t, durableMetricValue(t, reopenedReader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	store3 := newAgedFallbackStorage(t, dir)
	final, finalReader := newAgedFallbackExporter(t, workerAddress, destinationAddress, 30*time.Millisecond, true, agedFallbackSettings{storage: store3})
	require.NoError(t, final.Shutdown(t.Context()))
	require.Zero(t, durableMetricValue(t, finalReader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.ElementsMatch(t, []int64{841, 842}, destination.ids)
}

func TestCentralQueueAgedFallbackDivertsStuckLogsOnce(t *testing.T) {
	worker := &agedFallbackWorker{}
	destination := &durableTestDestination{}
	core, entries := observer.New(zap.InfoLevel)
	exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), 50*time.Millisecond, true, agedFallbackSettings{logger: zap.New(core)})
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(801, 802)))
	require.Eventually(t, func() bool {
		destination.mu.Lock()
		defer destination.mu.Unlock()
		return len(destination.ids) == 2
	}, 3*time.Second, 5*time.Millisecond, "accepted old records must reach fallback")
	require.Positive(t, worker.attempts.Load(), "the fixture must exercise the stuck worker")
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_fallback_records") == 2
	}, time.Second, time.Millisecond)
	require.NoError(t, exp.Shutdown(t.Context()))
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.ElementsMatch(t, []int64{801, 802}, destination.ids)
	require.Equal(t, int(destination.attempts.Load()), entries.FilterMessage("central queue fallback batch").Len())
}

func TestCentralQueueAgedFallbackMixedWindow(t *testing.T) {
	worker := &durableTestDestination{}
	destination := &durableTestDestination{}
	exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), 200*time.Millisecond, true,
		agedFallbackSettings{queue: map[string]any{"target_compressed_bytes": 512 << 10, "max_batch_delay": "300ms"}})
	input := func(id int64) plogotlp.ExportRequest {
		logs := connectionRecoveryInput(id)
		logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetTraceID(pcommon.TraceID{1})
		return plogotlp.NewExportRequestFromLogs(logs)
	}
	require.NoError(t, exp.ConsumeLogs(t.Context(), input(851).Logs()))
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, exp.ConsumeLogs(t.Context(), input(852).Logs()))
	require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "max_retry_age") == 1 }, 2*time.Second, time.Millisecond)
	require.NoError(t, exp.Shutdown(t.Context()))
	worker.mu.Lock()
	require.Equal(t, []int64{852}, worker.ids, "fresh records in the same window must use the worker")
	worker.mu.Unlock()
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.Equal(t, []int64{851}, destination.ids)
}

func TestCentralQueueAgedFallbackCommitFailurePauses(t *testing.T) {
	worker := &agedFallbackWorker{}
	destination := &durableTestDestination{}
	store := newAgedFallbackStorage(t, t.TempDir())
	failingStore := ackFailureStorage{Extension: store.(storage.Extension), marker: filepath.Join(t.TempDir(), "failed")}
	exp, reader := newAgedFallbackExporter(t, startAgedFallbackServer(t, worker), startAgedFallbackServer(t, destination), 30*time.Millisecond, true, agedFallbackSettings{storage: failingStore})
	require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(861, 862)))
	require.Eventually(t, func() bool { return agedFallbackRecords(t, reader, "fallback_commit_failed") == 2 }, 3*time.Second, time.Millisecond)
	require.Zero(t, agedFallbackRecords(t, reader, "max_retry_age"))
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	attempts := destination.attempts.Load()
	require.Never(t, func() bool { return destination.attempts.Load() > attempts }, 150*time.Millisecond, 5*time.Millisecond)
	require.Error(t, exp.Shutdown(t.Context()))
}

func TestCentralQueueAgedFallbackConfiguration(t *testing.T) {
	factory := loadbalancingexporter.NewFactory()
	t.Run("default age", func(t *testing.T) {
		cfg := factory.CreateDefaultConfig().(*loadbalancingexporter.Config)
		require.Equal(t, 15*time.Minute, cfg.CentralQueue.MaxRetryAge)
	})
	for _, tc := range []struct {
		name    string
		age     time.Duration
		enabled bool
		target  string
	}{
		{name: "negative age", age: -time.Second, enabled: true},
		{name: "fallback without queue", target: "otlp/customer"},
		{name: "fallback loop", enabled: true, target: "loadbalancing/other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := factory.CreateDefaultConfig().(*loadbalancingexporter.Config)
			cfg.CentralQueue.Enabled = tc.enabled
			cfg.CentralQueue.MaxCompressedBytes = 1 << 20
			cfg.CentralQueue.MaxRetryAge = tc.age
			if tc.target != "" {
				var id component.ID
				require.NoError(t, id.UnmarshalText([]byte(tc.target)))
				cfg.CentralQueue.FallbackExporter = &id
			}
			require.Error(t, cfg.CentralQueue.Validate())
		})
	}
}

func TestCentralQueueAgedFallbackRejectsUnavailableTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		host component.Host
	}{
		{name: "host without exporter capability", host: componenttest.NewNopHost()},
		{name: "missing target", host: agedFallbackHost{Host: componenttest.NewNopHost()}},
		{name: "wrong signal", host: agedFallbackHost{Host: componenttest.NewNopHost(), target: agedFallbackNonLogs{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := loadbalancingexporter.NewFactory()
			cfg := factory.CreateDefaultConfig()
			require.NoError(t, confmap.NewFromStringMap(map[string]any{
				"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
				"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{"localhost:4317"}}},
				"central_queue": map[string]any{"enabled": true, "max_compressed_bytes": 1 << 20, "fallback_exporter": "otlp/customer"},
			}).Unmarshal(cfg))
			exp, err := factory.CreateLogs(t.Context(), exportertest.NewNopSettings(factory.Type()), cfg)
			require.NoError(t, err)
			require.ErrorContains(t, exp.Start(t.Context(), tc.host), "fallback_exporter")
			require.NoError(t, exp.Shutdown(t.Context()))
		})
	}
}

func TestCentralQueueAgedFallbackRejectsOtherSignals(t *testing.T) {
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{"localhost:4317"}}},
		"central_queue": map[string]any{"enabled": true, "max_compressed_bytes": 1 << 20, "fallback_exporter": "otlp/customer"},
	}).Unmarshal(cfg))
	settings := exportertest.NewNopSettings(factory.Type())
	_, err := factory.CreateMetrics(t.Context(), settings, cfg)
	require.ErrorContains(t, err, "fallback_exporter is only supported for logs")
	_, err = factory.CreateTraces(t.Context(), settings, cfg)
	require.ErrorContains(t, err, "fallback_exporter is only supported for logs")
}

func agedFallbackRecords(t *testing.T, reader *sdkmetric.ManualReader, reason string) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var records int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "otelcol_loadbalancer_central_queue_fallback_records" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				value, _ := point.Attributes.Value(attribute.Key("reason"))
				if value.AsString() == reason {
					records += point.Value
				}
			}
		}
	}
	return records
}
