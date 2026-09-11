// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
)

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
