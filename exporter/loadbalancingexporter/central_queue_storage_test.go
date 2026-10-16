// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"errors"
	"maps"
	mathrand "math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configmiddleware"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
)

func TestDurableCentralQueueRejectsMissingStorageAtStartup(t *testing.T) {
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"protocol": map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver": map[string]any{"static": map[string]any{"hostnames": []string{"localhost:4317"}}},
		"central_queue": map[string]any{
			"enabled": true, "storage": "file_storage/queue", "max_compressed_bytes": 1 << 20,
			"max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20,
			"target_compressed_bytes": 512, "num_consumers": 1, "lane_count": 1,
		},
	})
	require.NoError(t, conf.Unmarshal(cfg))
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		_ = exp.Shutdown(ctx)
	})
	require.ErrorContains(t, exp.Start(t.Context(), componenttest.NewNopHost()), "file_storage/queue")
	require.Error(t, exp.ConsumeLogs(t.Context(), plog.NewLogs()), "missing storage must never fall back to memory")
}

type durableTestHost struct {
	extensions map[component.ID]component.Component
}

func (h durableTestHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

type durableStatusHost struct {
	component.Host
	events chan *componentstatus.Event
}

func (h durableStatusHost) Report(event *componentstatus.Event) { h.events <- event }

type durableTestDestination struct {
	seen map[int64]struct{}
	plogotlp.UnimplementedGRPCServer
	permanent atomic.Bool
	partial   atomic.Int64
	blocked   atomic.Bool
	attempts  atomic.Int64
	mu        sync.Mutex
	ids       []int64
}

func (d *durableTestDestination) Export(_ context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	d.attempts.Add(1)
	var ids []int64
	for _, rl := range req.Logs().ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, record := range sl.LogRecords().All() {
				id, _ := record.Attributes().Get("test.id")
				ids = append(ids, id.Int())
			}
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = make(map[int64]struct{})
	}
	for _, id := range ids {
		d.seen[id] = struct{}{}
	}
	if d.blocked.Load() {
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "backend unavailable")
	}
	if d.permanent.Load() {
		return plogotlp.NewExportResponse(), status.Error(codes.InvalidArgument, "test permanent rejection")
	}
	if rejected := d.partial.Load(); rejected > 0 {
		response := plogotlp.NewExportResponse()
		response.PartialSuccess().SetRejectedLogRecords(rejected)
		response.PartialSuccess().SetErrorMessage("test terminal rejection")
		return response, nil
	}
	d.ids = append(d.ids, ids...)
	return plogotlp.NewExportResponse(), nil
}

func TestDurableCentralQueueRejectsReducedCapacityBeforeReadingPayloads(t *testing.T) {
	runDurableRecovery(t, false, "reduced-capacity")
}

func TestDurableCentralQueueRecoversAcceptedLogsAfterProcessKill(t *testing.T) {
	runDurableRecovery(t, false, "")
}

func TestDurableCentralQueueSurvivesSecondKillDuringReplay(t *testing.T) {
	runDurableRecovery(t, false, "replay-crash")
}

func TestDurableCentralQueueRetainsBacklogAtShutdownDeadline(t *testing.T) {
	runDurableRecovery(t, true, "")
}

func TestDurableCentralQueueRejectsMissingManifestWithoutResettingStore(t *testing.T) {
	runDurableRecovery(t, false, "missing-manifest")
}

func TestDurableCentralQueueDoesNotReplayAcknowledgedLogs(t *testing.T) {
	runDurableRecovery(t, false, "acknowledged")
}

func TestDurableCentralQueueRetainsFailedAcknowledgement(t *testing.T) {
	runDurableRecovery(t, false, "ack-failure")
}

func TestDurableCentralQueueRejectsConcurrentWriter(t *testing.T) {
	runDurableRecovery(t, false, "lock")
}

func TestDurableCentralQueueRejectsCorruptManifest(t *testing.T) {
	runDurableRecovery(t, false, "corrupt-manifest")
}

func runDurableRecovery(t *testing.T, graceful bool, recoveryFault string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	destination.blocked.Store(true)
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	child := func(mode string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDurableCentralQueueProcess$", "-test.timeout=15s") // #nosec G204 -- Runs only this test binary with fixed arguments.
		cmd.Env = append(os.Environ(), "SAW13194_LB_STORE="+dir, "SAW13194_LB_MODE="+mode, "SAW13194_LB_ENDPOINT="+listener.Addr().String())
		return cmd
	}
	mode := "seed"
	if graceful {
		mode = "seed-graceful"
	}
	seed := child(mode)
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
	require.Eventually(t, func() bool { return destination.attempts.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	if recoveryFault == "lock" {
		output, err := child("lock-contender").CombinedOutput()
		require.NoError(t, err, string(output))
	}
	if graceful {
		require.NoError(t, seed.Wait())
	} else {
		require.NoError(t, seed.Process.Kill())
		require.Error(t, seed.Wait())
	}
	if recoveryFault != "" && recoveryFault != "acknowledged" && recoveryFault != "ack-failure" && recoveryFault != "lock" {
		output, err := child(recoveryFault).CombinedOutput()
		if recoveryFault == "replay-crash" {
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, string(output))
			require.Equal(t, 42, exited.ExitCode(), string(output))
		} else {
			require.NoError(t, err, string(output))
		}
	}
	destination.blocked.Store(false)
	if recoveryFault == "ack-failure" {
		failedAck := child("ack-failure")
		require.NoError(t, failedAck.Start())
		t.Cleanup(func() {
			if failedAck.ProcessState == nil {
				_ = failedAck.Process.Kill()
				_ = failedAck.Wait()
			}
		})
		require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(dir, "ack-failed")); return err == nil }, 3*time.Second, 10*time.Millisecond)
		attemptsAfterFailure := destination.attempts.Load()
		require.Never(t, func() bool { return destination.attempts.Load() > attemptsAfterFailure }, time.Second, 10*time.Millisecond, "an uncertain local commit must stop delivery until reopen")
		require.NoError(t, failedAck.Process.Kill())
		require.Error(t, failedAck.Wait())
		destination.mu.Lock()
		before := len(destination.ids)
		destination.ids = nil
		destination.mu.Unlock()
		require.GreaterOrEqual(t, before, 8)
		t.Logf("downstream accepted %d records before failed local acknowledgement; replay duplicates are allowed", before)
	}
	recoverMode := "recover"
	if recoveryFault == "acknowledged" {
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
	}, 3*time.Second, 10*time.Millisecond, "all eight accepted IDs must return after reopening the same ordinal store")
	require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids)
	if recoveryFault == "acknowledged" {
		require.NoError(t, recovered.Wait())
		output, err := child("recover-drain").CombinedOutput()
		require.NoError(t, err, string(output))
		destination.mu.Lock()
		ids = append([]int64(nil), destination.ids...)
		destination.mu.Unlock()
		require.ElementsMatch(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids, "a completed acknowledgement must not replay on another restart")
	}
}

func TestDurableCentralQueueProcess(t *testing.T) {
	dir := os.Getenv("SAW13194_LB_STORE")
	if dir == "" {
		t.Skip("subprocess only")
	}
	endpoint := os.Getenv("SAW13194_LB_ENDPOINT")
	storageFactory := filestorage.NewFactory()
	storageConfig := storageFactory.CreateDefaultConfig().(*filestorage.Config)
	storageConfig.Directory = dir
	storageConfig.FSync = true
	storageConfig.Recreate = false
	if os.Getenv("SAW13194_LB_MODE") == "lock-contender" {
		storageConfig.Timeout = 100 * time.Millisecond
	}
	ext, err := storageFactory.Create(t.Context(), extensiontest.NewNopSettings(storageFactory.Type()), storageConfig)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	conf := confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{endpoint}}},
		"central_queue": map[string]any{"enabled": true, "storage": "file_storage/queue", "max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 512, "max_batch_delay": "10ms", "num_consumers": 2, "lane_count": 1},
	})
	require.NoError(t, conf.Unmarshal(cfg))
	if os.Getenv("SAW13194_LB_MODE") == "reduced-capacity" {
		cfg.(*loadbalancingexporter.Config).CentralQueue.MaxCompressedBytes = 1
	}
	if os.Getenv("SAW13194_LB_MODE") == "reduced-uncompressed" {
		cfg.(*loadbalancingexporter.Config).CentralQueue.MaxUncompressedBatchBytes = 1
	}
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	var storageExtension component.Component = ext
	if os.Getenv("SAW13194_LB_MODE") == "replay-crash" || os.Getenv("SAW13194_LB_MODE") == "reduced-capacity" {
		storageExtension = replayCrashStorage{ext.(storage.Extension)}
	}
	if os.Getenv("SAW13194_LB_MODE") == "missing-manifest" || os.Getenv("SAW13194_LB_MODE") == "corrupt-manifest" {
		storageExtension = missingManifestStorage{Extension: ext.(storage.Extension), corrupt: os.Getenv("SAW13194_LB_MODE") == "corrupt-manifest"}
	}
	if os.Getenv("SAW13194_LB_MODE") == "ack-failure" {
		storageExtension = ackFailureStorage{Extension: ext.(storage.Extension), marker: filepath.Join(dir, "ack-failed")}
	}
	if os.Getenv("SAW13194_LB_MODE") == "corrupt-codec" || os.Getenv("SAW13194_LB_MODE") == "reduced-uncompressed" {
		storageExtension = corruptCodecStorage{Extension: ext.(storage.Extension)}
	}
	var host component.Host = durableTestHost{map[component.ID]component.Component{component.MustNewIDWithName("file_storage", "queue"): storageExtension}}
	if os.Getenv("SAW13194_LB_MODE") == "corrupt-manifest" {
		// The real Collector cannot consume asynchronous fatal events until
		// startup returns. A startup error must return without reporting one.
		host = durableStatusHost{Host: host, events: make(chan *componentstatus.Event)}
	}
	startErr := exp.Start(t.Context(), host)
	if os.Getenv("SAW13194_LB_MODE") == "corrupt-codec" {
		require.ErrorContains(t, startErr, "durable central queue payload")
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	if os.Getenv("SAW13194_LB_MODE") == "reduced-capacity" || os.Getenv("SAW13194_LB_MODE") == "reduced-uncompressed" {
		require.ErrorContains(t, startErr, "capacity")
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	if os.Getenv("SAW13194_LB_MODE") == "missing-manifest" || os.Getenv("SAW13194_LB_MODE") == "corrupt-manifest" {
		require.ErrorContains(t, startErr, "manifest")
		require.Error(t, exp.ConsumeLogs(t.Context(), plog.NewLogs()))
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	if os.Getenv("SAW13194_LB_MODE") == "lock-contender" {
		require.Error(t, startErr)
		require.Error(t, exp.ConsumeLogs(t.Context(), plog.NewLogs()))
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	require.NoError(t, startErr)
	if os.Getenv("SAW13194_LB_MODE") == "recover-drain" {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		require.NoError(t, exp.Shutdown(ctx))
		require.NoError(t, ext.Shutdown(t.Context()))
		return
	}
	if strings.HasPrefix(os.Getenv("SAW13194_LB_MODE"), "seed") {
		logs := plog.NewLogs()
		records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		for i := range 8 {
			record := records.AppendEmpty()
			record.Attributes().PutInt("test.id", int64(i))
			record.Body().SetStr("accepted durable record")
		}
		require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "accepted"), []byte("8"), 0o600))
		if os.Getenv("SAW13194_LB_MODE") == "seed-graceful" {
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, exp.Shutdown(ctx), context.DeadlineExceeded)
			require.NoError(t, ext.Shutdown(t.Context()))
			return
		}
	}
	select {}
}

type replayCrashStorage struct{ storage.Extension }

func (e replayCrashStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	client, err := e.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	return replayCrashClient{client}, nil
}

type replayCrashClient struct{ storage.Client }

func (c replayCrashClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	if err := c.Client.Batch(ctx, ops...); err != nil {
		return err
	}
	onlyReads := len(ops) > 0
	hasData := false
	for _, op := range ops {
		onlyReads = onlyReads && op.Type == storage.Get
		hasData = hasData || len(op.Value) > 0
	}
	if onlyReads && hasData {
		os.Exit(42)
	}
	return nil
}

func TestDurableCentralQueueStorageRequiresCentralQueueEnabled(t *testing.T) {
	cfg := loadbalancingexporter.NewFactory().CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{"central_queue": map[string]any{"storage": "file_storage/queue"}}).Unmarshal(cfg))
	require.ErrorContains(t, cfg.(*loadbalancingexporter.Config).Validate(), "requires central_queue.enabled")
}

type failingWriteStorage struct {
	storage.Extension
	armed    *atomic.Bool
	attempts *atomic.Int64
}

func (e failingWriteStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	client, err := e.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	return failingWriteClient{Client: client, armed: e.armed, attempts: e.attempts}, nil
}

type failingWriteClient struct {
	storage.Client
	armed    *atomic.Bool
	attempts *atomic.Int64
}

func (c failingWriteClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	if c.armed.Load() {
		for _, op := range ops {
			if op.Type != storage.Get {
				c.attempts.Add(1)
				return errors.New("injected storage write failure")
			}
		}
	}
	return c.Client.Batch(ctx, ops...)
}

func TestDurableCentralQueueRejectsAndCountsFailedStorageAdmission(t *testing.T) {
	storageFactory := filestorage.NewFactory()
	storageConfig := storageFactory.CreateDefaultConfig().(*filestorage.Config)
	storageConfig.Directory = t.TempDir()
	storageConfig.FSync = true
	storageConfig.Recreate = false
	ext, err := storageFactory.Create(t.Context(), extensiontest.NewNopSettings(storageFactory.Type()), storageConfig)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{"127.0.0.1:1"}}},
		"central_queue": map[string]any{"enabled": true, "storage": "file_storage/queue", "max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 512, "max_batch_delay": "10ms", "num_consumers": 1, "lane_count": 1},
	}).Unmarshal(cfg))
	armed, attempts := &atomic.Bool{}, &atomic.Int64{}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
	settings.MeterProvider = provider
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	events := make(chan *componentstatus.Event, 10)
	host := durableStatusHost{Host: durableTestHost{map[component.ID]component.Component{component.MustNewIDWithName("file_storage", "queue"): failingWriteStorage{Extension: ext.(storage.Extension), armed: armed, attempts: attempts}}}, events: events}
	require.NoError(t, exp.Start(t.Context(), host))
	t.Cleanup(func() {
		require.ErrorContains(t, exp.Shutdown(context.WithoutCancel(t.Context())), "injected storage write failure")
		require.NoError(t, ext.Shutdown(context.WithoutCancel(t.Context())))
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	armed.Store(true)
	input := plog.NewLogs()
	input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("must not be acknowledged without storage")
	require.ErrorContains(t, exp.ConsumeLogs(t.Context(), input), "injected storage write failure")
	require.Error(t, exp.ConsumeLogs(t.Context(), input), "an uncertain write must block subsequent mutations until reopen")
	require.Equal(t, int64(1), attempts.Load())
	require.Eventually(t, func() bool { return len(events) == 1 }, time.Second, 10*time.Millisecond, "a paused owner must report permanent component failure")
	event := <-events
	require.Equal(t, componentstatus.StatusPermanentError, event.Status())
	require.ErrorContains(t, event.Err(), "injected storage write failure")
	require.Empty(t, events, "cached refusal must not report another permanent status")
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var failures int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "otelcol_loadbalancer_central_queue_storage_failures" {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				for _, point := range sum.DataPoints {
					operation, _ := point.Attributes.Value(attribute.Key("operation"))
					reason, _ := point.Attributes.Value(attribute.Key("reason"))
					require.Equal(t, "admit", operation.AsString())
					require.Equal(t, "write", reason.AsString())
					failures += point.Value
				}
			}
		}
	}
	require.Equal(t, int64(1), failures, "one failed write must emit one storage failure")
}

type missingManifestStorage struct {
	storage.Extension
	corrupt bool
}

func (e missingManifestStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	client, err := e.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	return &missingManifestClient{Client: client, corrupt: e.corrupt}, nil
}

type missingManifestClient struct {
	storage.Client
	read, corrupt bool
}

func (c *missingManifestClient) Get(ctx context.Context, key string) ([]byte, error) {
	if !c.read {
		c.read = true
		if c.corrupt {
			return []byte("invalid JSON"), nil
		}
		return nil, nil
	}
	return c.Client.Get(ctx, key)
}

type ackFailureStorage struct {
	storage.Extension
	marker string
}

func (e ackFailureStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	client, err := e.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	return ackFailureClient{Client: client, marker: e.marker}, nil
}

type ackFailureClient struct {
	storage.Client
	marker string
}

func (c ackFailureClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	for _, op := range ops {
		if op.Type == storage.Delete {
			if err := os.WriteFile(c.marker, []byte("failed"), 0o600); err != nil {
				return err
			}
			return errors.New("injected acknowledgement write failure")
		}
	}
	return c.Client.Batch(ctx, ops...)
}

// Retained work remains visible after the scheduler stops and must not be counted as loss.
func TestDurableCentralQueueReportsRetainedBacklogAtDeadline(t *testing.T) {
	exp, ext, reader := newDurableTestExporter(t, t.TempDir(), "127.0.0.1:1")
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		require.NoError(t, ext.Shutdown(context.WithoutCancel(t.Context())))
	})
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	records.AppendEmpty().Body().SetStr("retained one")
	records.AppendEmpty().Body().SetStr("retained two")
	require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	require.Positive(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_bytes"))
	require.Positive(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_oldest_enqueued_time"))
	require.ErrorIs(t, exp.Shutdown(t.Context()), context.DeadlineExceeded)
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_deferred_items"))
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_shutdown_dropped_items"))
}

func durableMetricValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var value int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch points := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range points.DataPoints {
					value += p.Value
				}
			case metricdata.Sum[int64]:
				for _, p := range points.DataPoints {
					value += p.Value
				}
			default:
				t.Fatalf("unexpected metric data for %s: %T", name, m.Data)
			}
		}
	}
	return value
}

func newDurableTestExporter(t *testing.T, dir, endpoint string, shutdownTimeout ...time.Duration) (exporter.Logs, component.Component, *sdkmetric.ManualReader) {
	exp, store, reader := newDurableSignalTestExporter(t, dir, []string{endpoint}, "logs", shutdownTimeout...)
	return exp.(exporter.Logs), store, reader
}

func newDurableTestMetricsExporter(t *testing.T, dir, endpoint string) (exporter.Metrics, component.Component, *sdkmetric.ManualReader) {
	exp, store, reader := newDurableSignalTestExporter(t, dir, []string{endpoint}, "metrics")
	return exp.(exporter.Metrics), store, reader
}

func newDurableSignalTestExporter(t *testing.T, dir string, endpoints []string, signal string, shutdownTimeout ...time.Duration) (component.Component, component.Component, *sdkmetric.ManualReader) {
	return newDurableSignalTestExporterWithMiddlewares(t, dir, endpoints, signal, nil, shutdownTimeout...)
}

func newDurableSignalTestExporterWithMiddlewares(t *testing.T, dir string, endpoints []string, signal string, middlewares map[component.ID]component.Component, shutdownTimeout ...time.Duration) (component.Component, component.Component, *sdkmetric.ManualReader) {
	timeout := 20 * time.Millisecond
	if len(shutdownTimeout) > 0 {
		timeout = shutdownTimeout[0]
	}
	t.Helper()

	storageFactory := filestorage.NewFactory()
	storageConfig := storageFactory.CreateDefaultConfig().(*filestorage.Config)
	storageConfig.Directory = dir
	storageConfig.FSync = true
	ext, err := storageFactory.Create(t.Context(), extensiontest.NewNopSettings(storageFactory.Type()), storageConfig)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	lanes, consumers := 1, 1
	if len(endpoints) > 1 {
		lanes, consumers = 16, 2
	}
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": endpoints}},
		"central_queue": map[string]any{"enabled": true, "storage": "file_storage/queue", "shutdown_timeout": timeout.String(), "max_compressed_bytes": 1 << 20, "max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 512, "max_batch_delay": "10ms", "num_consumers": consumers, "lane_count": lanes},
	}).Unmarshal(cfg))
	for id := range middlewares {
		cfg.(*loadbalancingexporter.Config).Protocol.OTLP.ClientConfig.Middlewares = append(cfg.(*loadbalancingexporter.Config).Protocol.OTLP.ClientConfig.Middlewares, configmiddleware.Config{ID: id})
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
	settings.MeterProvider = provider
	var exp component.Component
	switch signal {
	case "logs":
		exp, err = factory.CreateLogs(t.Context(), settings, cfg)
	case "metrics":
		exp, err = factory.CreateMetrics(t.Context(), settings, cfg)
	default:
		t.Fatalf("unsupported test signal %q", signal)
	}
	require.NoError(t, err)
	extensions := map[component.ID]component.Component{component.MustNewIDWithName("file_storage", "queue"): ext}
	maps.Copy(extensions, middlewares)
	require.NoError(t, exp.Start(t.Context(), durableTestHost{extensions}))
	return exp, ext, reader
}

func TestDurableCentralQueueRecordsPartialSuccessAsTerminalRejection(t *testing.T) {
	testDurableTerminalRejection(t, false)
}

func TestDurableCentralQueueRecordsPermanentErrorAsTerminalRejection(t *testing.T) {
	testDurableTerminalRejection(t, true)
}

func testDurableTerminalRejection(t *testing.T, permanent bool) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	destination.partial.Store(2)
	destination.permanent.Store(permanent)
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	exp, ext, reader := newDurableTestExporter(t, dir, listener.Addr().String())
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		_ = ext.Shutdown(context.WithoutCancel(t.Context()))
	})
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	records.AppendEmpty().Body().SetStr("terminally rejected one")
	records.AppendEmpty().Body().SetStr("terminally rejected two")
	require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items") == 2
	}, time.Second, 10*time.Millisecond)
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	require.NoError(t, exp.Shutdown(t.Context()))
	require.NoError(t, ext.Shutdown(t.Context()))
	attempts := destination.attempts.Load()
	destination.partial.Store(0)
	destination.permanent.Store(false)
	reopened, store, reopenedReader := newDurableTestExporter(t, dir, listener.Addr().String())
	require.Equal(t, int64(2), durableMetricValue(t, reopenedReader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items"), "terminal disposition must survive reopening")
	require.NoError(t, reopened.Shutdown(t.Context()))
	require.NoError(t, store.Shutdown(t.Context()))
	require.Equal(t, attempts, destination.attempts.Load(), "OTLP partial success must not be retried")
}

func TestDurableCentralQueueAtomicallyReservesConcurrentRequestCapacity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	destination.blocked.Store(true)
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	exp, ext, reader := newDurableTestExporter(t, t.TempDir(), listener.Addr().String())
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		require.NoError(t, ext.Shutdown(context.WithoutCancel(t.Context())))
	})
	body := make([]byte, 600<<10)
	_, err = mathrand.NewChaCha8([32]byte{1}).Read(body)
	require.NoError(t, err)
	request := func(ids ...int64) plog.Logs {
		logs := plog.NewLogs()
		for _, id := range ids {
			record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.Attributes().PutInt("test.id", id)
			record.Body().SetEmptyBytes().FromRaw(body)
		}
		return logs
	}
	// Each payload fits, but the complete request exceeds the 1 MiB capacity.
	require.Error(t, exp.ConsumeLogs(t.Context(), request(10, 11)))
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"), "a refused multi-payload request must leave no accepted subset")
	type result struct {
		id  int64
		err error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, id := range []int64{20, 21} {
		go func() { <-start; results <- result{id, exp.ConsumeLogs(t.Context(), request(id))} }()
	}
	close(start)
	var accepted []int64
	var refused int
	for range 2 {
		r := <-results
		if r.err == nil {
			accepted = append(accepted, r.id)
		} else {
			refused++
		}
	}
	require.Len(t, accepted, 1, "pending and in-flight durable bytes must both reserve capacity")
	require.Equal(t, 1, refused)
	require.Equal(t, int64(1), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	destination.blocked.Store(false)
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, 3*time.Second, 10*time.Millisecond)
	destination.mu.Lock()
	ids := append([]int64(nil), destination.ids...)
	destination.mu.Unlock()
	require.ElementsMatch(t, accepted, ids, "only the atomically accepted request may reach the destination")
}
