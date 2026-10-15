// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type shutdownWarningWriter func([]byte) (int, error)

func (w shutdownWarningWriter) Write(p []byte) (int, error) { return w(p) }

func TestLogsShutdownCancelsDeliveryBeforeWarningOutput(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	warningStarted := make(chan struct{})
	releaseWarning := make(chan struct{})
	var warningOnce sync.Once
	release := sync.OnceFunc(func() { close(releaseWarning) })
	defer release()
	writer := shutdownWarningWriter(func(p []byte) (int, error) {
		warningOnce.Do(func() { close(warningStarted) })
		<-releaseWarning
		return len(p), nil
	})
	ts.Logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(writer), zap.WarnLevel))
	cfg := twoEndpointLogConfig()
	cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1024, MaxInflightUncompressedBytes: 1024, TargetCompressedBytes: 1, NumConsumers: 1, LaneCount: 1}
	entered := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, _ plog.Logs) error {
			entered <- struct{}{}
			<-ctx.Done()
			cancelled <- struct{}{}
			return ctx.Err()
		}), nil
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), compressibleLogs(20, 64)))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow backend was not reached")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown(ctx) }()
	select {
	case <-warningStarted:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not warn about pending records")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Error("blocked warning output delayed backend cancellation past the drain deadline")
	}
	release()
	require.ErrorIs(t, <-shutdown, context.DeadlineExceeded)
}

func TestLogsShutdownDrainsAcceptedCentralQueueRecords(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "retry"}[retry], func(t *testing.T) {
			ts, tb := getTelemetryAssets(t)
			cfg := twoEndpointLogConfig()
			cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1 << 20, MaxBatchDelay: time.Hour, NumConsumers: 2, LaneCount: 2}
			var delivered atomic.Int64
			var idsMu sync.Mutex
			var ids []int64
			var attempts atomic.Int64
			p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
				return newMockLogsExporter(func(ctx context.Context, logs plog.Logs) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if attempts.Add(1) == 1 && retry {
						return errors.New("transient backend failure")
					}
					delivered.Add(int64(logs.LogRecordCount()))
					idsMu.Lock()
					for _, rl := range logs.ResourceLogs().All() {
						for _, sl := range rl.ScopeLogs().All() {
							for _, lr := range sl.LogRecords().All() {
								id, _ := lr.Attributes().Get("test.id")
								ids = append(ids, id.Int())
							}
						}
					}
					idsMu.Unlock()
					return nil
				}), nil
			})
			require.NotNil(t, p.centralQueue)
			p.centralQueue.settings.maxBatchDelay = time.Hour
			p.centralQueue.settings.targetCompressedBytes = 1 << 20
			require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
			input := compressibleLogs(20, 32)
			var expected []int64
			for i, lr := range input.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().All() {
				lr.Attributes().PutInt("test.id", int64(i))
				expected = append(expected, int64(i))
			}
			require.NoError(t, p.ConsumeLogs(t.Context(), input))
			require.Positive(t, p.centralQueue.len())
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			require.NoError(t, p.Shutdown(ctx))
			require.Equal(t, int64(20), delivered.Load(), "every accepted record must finish before shutdown returns")
			require.ElementsMatch(t, expected, ids)
			require.Zero(t, p.centralQueue.compressedBytes())
			require.ErrorIs(t, p.ConsumeLogs(t.Context(), simpleLogs()), errExporterIsStopping)
		})
	}
}

func TestLogsShutdownHonorsDeadlineWithUnavailableBackend(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1 << 20, MaxBatchDelay: time.Hour, NumConsumers: 2, LaneCount: 2}
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, _ plog.Logs) error { <-ctx.Done(); return ctx.Err() }), nil
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), compressibleLogs(20, 32)))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Shutdown(ctx), context.DeadlineExceeded)
	cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
	defer cancelCleanup()
	require.NoError(t, waitForInflight(cleanup, &p.centralWG))
}

func TestLogsShutdownCountsEveryUnsentRecordAtDeadline(t *testing.T) {
	ts, tb, telemetry := getTelemetryAssetsWithReader(t)
	core, warnings := observer.New(zap.WarnLevel)
	ts.Logger = zap.New(core)
	cfg := twoEndpointLogConfig()
	cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1024, MaxInflightUncompressedBytes: 1024, TargetCompressedBytes: 1, NumConsumers: 1, LaneCount: 1}
	entered := make(chan struct{}, 1)
	exited := make(chan struct{}, 1)
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, _ plog.Logs) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			exited <- struct{}{}
			return ctx.Err()
		}), nil
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), compressibleLogs(20, 64)))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow backend was not reached")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Shutdown(ctx), context.DeadlineExceeded)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel the slow backend")
	}
	// Includes both pending payloads and the cancelled in-flight payload.
	require.Eventually(t, func() bool {
		m, err := telemetry.GetMetric("otelcol_loadbalancer_central_queue_shutdown_dropped_items")
		if err != nil {
			return false
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		return ok && len(sum.DataPoints) == 1 && sum.DataPoints[0].Value == 20
	}, time.Second, time.Millisecond, "accepted records discarded at shutdown must be counted")
	requireCentralQueueIntSum(t, telemetry, "otelcol_loadbalancer_central_queue_shutdown_dropped_items", "{items}", attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", "forced")), 20)
	require.Eventually(t, func() bool {
		var warned int64
		for _, entry := range warnings.FilterMessage("central queue records discarded at shutdown deadline").All() {
			warned += entry.ContextMap()["dropped_items"].(int64)
		}
		return warned == 20
	}, time.Second, time.Millisecond, "every discarded record must also be warned")
	for _, entry := range warnings.FilterMessage("central queue records discarded at shutdown deadline").All() {
		require.Equal(t, "forced", entry.ContextMap()["reason"])
	}
}

func TestLogsShutdownDrainsBeyondThirtySeconds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ts, tb := getTelemetryAssets(t)
		cfg := twoEndpointLogConfig()
		cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1 << 20, MaxBatchDelay: time.Hour, NumConsumers: 1, LaneCount: 1}
		var delivered atomic.Int64
		p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
			return newMockLogsExporter(func(ctx context.Context, logs plog.Logs) error {
				select {
				case <-time.After(31 * time.Second):
					delivered.Add(int64(logs.LogRecordCount()))
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}), nil
		})
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeLogs(t.Context(), compressibleLogs(20, 32)))
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx), "drain must use the longer bounded shutdown budget")
		require.Equal(t, int64(20), delivered.Load())
	})
}

func TestMetricsShutdownDrainsAcceptedDatapoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ts, tb := getTelemetryAssets(t)
		cfg := serviceBasedRoutingConfig()
		cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1 << 20, MaxBatchDelay: time.Hour, NumConsumers: 1, LaneCount: 1}
		var delivered atomic.Int64
		p, _ := newTestMetricsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
			return newMockMetricsExporter(func(ctx context.Context, md pmetric.Metrics) error {
				select {
				case <-time.After(time.Second):
					delivered.Add(int64(md.DataPointCount()))
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}), nil
		})
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeMetrics(t.Context(), metricsWithServiceDataPoints("same", "same", "same")))
		require.NoError(t, p.Shutdown(t.Context()))
		require.Equal(t, int64(3), delivered.Load(), "accepted datapoints must drain before shutdown")
	})
}

func TestMetricsShutdownCountsUnsentDatapointsAtConfiguredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ts, tb, telemetry := getTelemetryAssetsWithReader(t)
		cfg := serviceBasedRoutingConfig()
		cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1 << 20, MaxBatchDelay: time.Hour, ShutdownTimeout: 50 * time.Millisecond, NumConsumers: 1, LaneCount: 1}
		p, _ := newTestMetricsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
			return newMockMetricsExporter(func(ctx context.Context, _ pmetric.Metrics) error {
				<-ctx.Done()
				return ctx.Err()
			}), nil
		})
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeMetrics(t.Context(), metricsWithServiceDataPoints("same", "same", "same")))
		require.ErrorIs(t, p.Shutdown(t.Context()), context.DeadlineExceeded)
		requireCentralQueueIntSum(t, telemetry, "otelcol_loadbalancer_central_queue_shutdown_dropped_items", "{items}", attribute.NewSet(attribute.String("signal", "metrics"), attribute.String("reason", "forced")), 3)
		// Repeated shutdown cannot count the same ownership twice.
		require.NoError(t, p.Shutdown(t.Context()))
		requireCentralQueueIntSum(t, telemetry, "otelcol_loadbalancer_central_queue_shutdown_dropped_items", "{items}", attribute.NewSet(attribute.String("signal", "metrics"), attribute.String("reason", "forced")), 3)
	})
}

func TestLogsShutdownWaitsForInflightDeliveryAndRejectsNewIntake(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.CentralQueue = CentralQueueConfig{Enabled: true, PayloadCompression: QueuePayloadCompressionZstd, MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1, NumConsumers: 2, LaneCount: 2}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var records atomic.Int64
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, ld plog.Logs) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
				records.Add(int64(ld.LogRecordCount()))
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), nil
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), simpleLogs()))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	shutdown := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	go func() { shutdown <- p.Shutdown(ctx) }()
	require.Eventually(t, func() bool {
		p.centralQueue.mu.Lock()
		defer p.centralQueue.mu.Unlock()
		return p.centralQueue.draining
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, p.ConsumeLogs(t.Context(), simpleLogs()), errExporterIsStopping)
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown returned before delivery completed: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-shutdown)
	require.Equal(t, int64(simpleLogs().LogRecordCount()), records.Load())
}

// Complete delivery when drain starts waiting, before it observes cancellation.
type drainCompletionContext struct {
	context.Context
	complete func()
}

func (c drainCompletionContext) Done() <-chan struct{} {
	c.complete()
	return c.Context.Done()
}

func TestCentralQueueDrainRechecksCompletionAtDeadline(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{})
	q.currentInflightBytes = 1
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	complete := sync.OnceFunc(func() {
		q.mu.Lock()
		q.currentInflightBytes = 0
		q.mu.Unlock()
		cancel()
	})
	require.NoError(t, q.drain(drainCompletionContext{Context: ctx, complete: complete}))
}
