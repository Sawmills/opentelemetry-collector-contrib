// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestInMemoryCompressionBoundsHeapAndPreservesDelivery(t *testing.T) {
	if runMemoryTestInSubprocess(t) {
		return
	}
	previous := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(previous)
	raw := measureInMemoryQueueCompression(t, false)
	compressed := measureInMemoryQueueCompression(t, true)
	const allowance = int64(16 << 20)
	t.Logf("queued post-GC heap: compression off=%d on=%d allowance=%d; admitted=%d/%d rejected=%d/%d; after drain=%d/%d",
		raw.retained, compressed.retained, allowance, len(raw.admitted), len(compressed.admitted),
		raw.rejected, compressed.rejected, raw.afterDrain, compressed.afterDrain)
	require.Equal(t, raw.admitted, compressed.admitted, "compression must preserve admitted and rejected identities")
	require.Equal(t, raw.rejected, compressed.rejected)
	require.Greater(t, raw.retained, allowance, "the raw control must expose retained decoded queue inventory")
	require.LessOrEqual(t, compressed.retained, allowance, "compress_in_memory must release decoded queued requests")
}

type queueCompressionMeasurement struct {
	admitted   map[int64]int
	rejected   int
	retained   int64
	afterDrain int64
}

type queueCompressionPeer struct {
	plogotlp.UnimplementedGRPCServer
	hold     atomic.Bool
	attempts atomic.Int64
	canceled atomic.Int64
	release  chan struct{}
	mu       sync.Mutex
	received map[int64]int
}

func (b *queueCompressionPeer) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	b.attempts.Add(1)
	// Normal RPC completion cancels its context after the handler returns.
	// Count only cancellation observed while this handler is still active,
	// including calls that start after recovery has cleared hold.
	defer func() {
		if ctx.Err() != nil {
			b.canceled.Add(1)
		}
	}()
	if b.hold.Load() {
		select {
		case <-b.release:
		case <-ctx.Done():
			return plogotlp.NewExportResponse(), ctx.Err()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, resource := range req.Logs().ResourceLogs().All() {
		for _, scope := range resource.ScopeLogs().All() {
			for _, record := range scope.LogRecords().All() {
				id, _ := record.Attributes().Get("id")
				b.received[id.Int()]++
			}
		}
	}
	return plogotlp.NewExportResponse(), nil
}

func queueCompressionLogs(id int64) plog.Logs {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr(strings.Repeat("x", 16368) + fmt.Sprintf("%016d", id))
	record.Attributes().PutInt("id", id)
	logs.MarkReadOnly()
	return logs
}

func queueCompressionHeap() int64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}

func queueCompressionSize(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &metrics))
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if strings.HasSuffix(metric.Name, "exporter_queue_size") {
				if gauge, ok := metric.Data.(metricdata.Gauge[int64]); ok && len(gauge.DataPoints) == 1 {
					return gauge.DataPoints[0].Value
				}
			}
		}
	}
	t.Fatal("queue size metric coverage is missing or ambiguous")
	return 0
}

func measureInMemoryQueueCompression(t *testing.T, compress bool) queueCompressionMeasurement {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 70*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := &queueCompressionPeer{release: make(chan struct{}), received: make(map[int64]int)}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, peer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { require.NoError(t, provider.Shutdown(context.Background())) }()
	settings := exportertest.NewNopSettings(metadata.Type)
	settings.MeterProvider = provider
	var errorLogs atomic.Int64
	// Count errors without retaining their fields or affecting the heap sample
	// with an in-memory log buffer. Admission rejections are included; the
	// recovery snapshot separates them from subsequent send/decode errors.
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(io.Discard), zap.ErrorLevel)
	settings.Logger = zap.New(core, zap.Hooks(func(zapcore.Entry) error {
		errorLogs.Add(1)
		return nil
	}))
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Resolver = ResolverSettings{Static: configoptional.Some(StaticResolver{Hostnames: []string{listener.Addr().String()}})}
	cfg.Protocol.OTLP.ClientConfig.TLS.Insecure = true
	cfg.Protocol.OTLP.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.Protocol.OTLP.RetryConfig.Enabled = false
	// Keep the deliberately blocked send alive through intake and the heap
	// snapshot, including slower race/ARM runs. The scenario context still
	// bounds the test; transport deadlines are not the behavior under test.
	cfg.TimeoutSettings.Timeout = 70 * time.Second
	cfg.Protocol.OTLP.TimeoutConfig.Timeout = 70 * time.Second
	cfg.CentralQueue.Enabled = false
	queueConfig := exporterhelper.NewDefaultQueueConfig()
	queueConfig.NumConsumers = 1
	queueConfig.QueueSize = 4100
	cfg.QueueSettings.QueueConfig = configoptional.Some(queueConfig)
	// Only this flag differs. The codec remains configured in both cells.
	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	cfg.QueueSettings.CompressInMemory = compress
	require.NoError(t, cfg.Validate())
	exp, err := factory.CreateLogs(ctx, settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	var release sync.Once
	recoverBackend := func() { release.Do(func() { peer.hold.Store(false); close(peer.release) }) }
	defer func() {
		recoverBackend()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		require.NoError(t, exp.Shutdown(shutdown))
	}()
	require.NoError(t, exp.ConsumeLogs(ctx, queueCompressionLogs(-1)))
	require.Eventually(t, func() bool {
		peer.mu.Lock()
		defer peer.mu.Unlock()
		return peer.received[-1] == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return queueCompressionSize(t, reader) == 0 }, 5*time.Second, 10*time.Millisecond)
	peer.mu.Lock()
	peer.received = make(map[int64]int)
	peer.mu.Unlock()
	peer.attempts.Store(0)
	baseline := queueCompressionHeap()
	setupElapsed := time.Since(started)
	peer.hold.Store(true)
	result := queueCompressionMeasurement{admitted: make(map[int64]int)}
	completed := 0
	admissionStarted := time.Now()
	for id := int64(0); id < 4608; id++ {
		err := exp.ConsumeLogs(ctx, queueCompressionLogs(id))
		completed++
		if err == nil {
			result.admitted[id] = 1
		} else {
			result.rejected++
			require.False(t, consumererror.IsPermanent(err), "capacity pressure must remain retryable")
		}
	}
	admissionElapsed := time.Since(admissionStarted)
	require.Equal(t, 4608, completed, "no caller may remain held at the memory snapshot")
	require.Equal(t, 4100, len(result.admitted))
	require.Equal(t, 508, result.rejected)
	require.Eventually(t, func() bool { return peer.attempts.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(4100), queueCompressionSize(t, reader))
	snapshotStarted := time.Now()
	result.retained = queueCompressionHeap() - baseline
	snapshotElapsed := time.Since(snapshotStarted)
	require.Zero(t, peer.canceled.Load(), "fixture transport expired while backend was intentionally held")
	beforeCanceled, beforeErrors := peer.canceled.Load(), errorLogs.Load()
	t.Logf("compression=%t setup_ms=%d admission_ms=%d snapshot_ms=%d retained=%d admitted=%d rejected=%d handler_canceled_before=%d error_logs_before=%d",
		compress, setupElapsed.Milliseconds(), admissionElapsed.Milliseconds(), snapshotElapsed.Milliseconds(),
		result.retained, len(result.admitted), result.rejected, beforeCanceled, beforeErrors)
	drainStarted := time.Now()
	// At most one sample per five seconds, plus the initial and final state.
	// Columns: elapsed ms, received identities, queued units, backend attempts,
	// canceled active handlers, error log events (not unique lost records).
	history := make([][6]int64, 0, 8)
	observe := func() [6]int64 {
		peer.mu.Lock()
		received := len(peer.received)
		peer.mu.Unlock()
		return [6]int64{time.Since(drainStarted).Milliseconds(), int64(received), queueCompressionSize(t, reader),
			peer.attempts.Load(), peer.canceled.Load(), errorLogs.Load()}
	}
	history = append(history, observe())
	recoverBackend()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		current := observe()
		if current[0]-history[len(history)-1][0] >= 5000 || current[1] == int64(len(result.admitted)) {
			history = append(history, current)
		}
		assert.Equal(collect, int64(len(result.admitted)), current[1],
			"compression=%t current(ms,received,queued,attempts,handler_canceled,error_logs)=%v history=%v handler_canceled_before=%d error_logs_before=%d",
			compress, current, history, beforeCanceled, beforeErrors)
	}, 30*time.Second, 10*time.Millisecond)
	t.Logf("compression=%t drain_ms=%d history(ms,received,queued,attempts,handler_canceled,error_logs)=%v", compress, time.Since(drainStarted).Milliseconds(), history)
	peer.mu.Lock()
	received := make(map[int64]int, len(peer.received))
	for id, count := range peer.received {
		received[id] = count
	}
	peer.mu.Unlock()
	require.Equal(t, result.admitted, received, "every admitted identity must drain exactly once, without rejected identities")
	require.Eventually(t, func() bool { return queueCompressionSize(t, reader) == 0 }, 5*time.Second, 10*time.Millisecond)
	result.afterDrain = queueCompressionHeap() - baseline
	return result
}
