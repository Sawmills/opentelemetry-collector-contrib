// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLogsCentralQueueBackloggedLaneUsesIdleWorker(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.LogRouting.IgnoreTraceID = true
	cfg.LogRouting.RecordStripingEnabled = true
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1,
		NumConsumers: 2, LaneCount: 1,
	}
	var slowEndpoint atomic.Pointer[string]
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	delivered := make(chan int, 1)
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, logs plog.Logs) error {
			slowEndpoint.CompareAndSwap(nil, &endpoint)
			if endpoint != *slowEndpoint.Load() {
				delivered <- logs.LogRecordCount()
				return nil
			}
			entered <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), nil
	})
	p.randomTraceID = func() pcommon.TraceID { return pcommon.TraceID{1} }
	t.Cleanup(func() {
		finish()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), simpleLogs()))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first window did not reach its home worker")
	}
	input := simpleLogs()
	expected := input.LogRecordCount()
	require.NoError(t, p.ConsumeLogs(t.Context(), input))
	select {
	case count := <-delivered:
		require.Equal(t, expected, count)
	case <-time.After(time.Second):
		t.Fatal("backlogged lane waited on a slow worker while another worker was idle")
	}
}

func TestLogsCentralQueueSpillSkipsRecentlyFailedWorker(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.Resolver.Static = configoptional.Some(StaticResolver{Hostnames: []string{
		"endpoint-1:4317", "endpoint-2:4317", "endpoint-3:4317", "endpoint-4:4317",
	}})
	cfg.LogRouting.IgnoreTraceID = true
	cfg.LogRouting.RecordStripingEnabled = true
	cfg.EndpointHealth = EndpointHealthConfig{
		Enabled: true, RerouteOnFailure: true, MaxRerouteAttempts: 1,
		MinEligibleBackends: 4, MaxQuarantinedPercent: 100, QuarantineDuration: time.Minute,
	}
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1, NumConsumers: 4,
	}
	entered, delivered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	var enteredOnce, deliveredOnce sync.Once
	var freshFailures atomic.Int64
	p, lb := newTestLogsExporter(t, ts, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, logs plog.Logs) error {
			id, _ := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("test.id")
			if endpoint == "endpoint-1:4317" {
				if id.Int() == 3 {
					freshFailures.Add(1)
				}
				return status.Error(codes.Unavailable, "fast failure")
			}
			if endpoint == "endpoint-2:4317" && id.Int() == 2 {
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if id.Int() == 3 {
				deliveredOnce.Do(func() { close(delivered) })
			}
			return nil
		}), nil
	})
	t.Cleanup(func() {
		finish()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	consumeCentralQueueTestLogAtLane(t, p, 1, 0, 4)
	require.Eventually(t, lb.endpointHealth.failOpen, time.Second, time.Millisecond)
	consumeCentralQueueTestLogAtLane(t, p, 2, 1, 4)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow healthy worker did not receive its home window")
	}
	consumeCentralQueueTestLogAtLane(t, p, 3, 1, 4)
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh backlog did not reach an idle healthy worker")
	}
	require.Zero(t, freshFailures.Load(), "recently failed workers must not attract fresh spill traffic")
}

func TestLogsCentralQueueFailedSpillDoesNotBlockOtherLanes(t *testing.T) {
	settings, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	hosts := make([]string, 11)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("endpoint-%02d:4317", i+1)
	}
	cfg.Resolver.Static = configoptional.Some(StaticResolver{Hostnames: hosts})
	cfg.LogRouting.IgnoreTraceID = true
	cfg.LogRouting.RecordStripingEnabled = true
	cfg.EndpointHealth = EndpointHealthConfig{
		Enabled: true, RerouteOnFailure: true, MaxRerouteAttempts: 1,
		MinEligibleBackends: 1, MaxQuarantinedPercent: 100, QuarantineDuration: time.Minute,
	}
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1, NumConsumers: 2,
	}
	homeEntered, failedSpill, delivered, release := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	var enteredOnce, failedOnce, deliveredOnce sync.Once
	exp, lb := newTestLogsExporter(t, settings, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, logs plog.Logs) error {
			id, _ := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("test.id")
			if endpoint == "endpoint-01:4317" {
				enteredOnce.Do(func() { close(homeEntered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if endpoint == "endpoint-02:4317" {
				failedOnce.Do(func() { close(failedSpill) })
				return status.Error(codes.Unavailable, "spill failed")
			}
			if id.Int() == 3 {
				deliveredOnce.Do(func() { close(delivered) })
			}
			return nil
		}), nil
	})
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		finish()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, exp.Shutdown(ctx))
	})
	consumeCentralQueueTestLogAtLane(t, exp, 1, 0, 11)
	select {
	case <-homeEntered:
	case <-time.After(time.Second):
		t.Fatal("slow home did not start")
	}
	consumeCentralQueueTestLogAtLane(t, exp, 2, 0, 11)
	select {
	case <-failedSpill:
	case <-time.After(time.Second):
		t.Fatal("spill failure was not observed")
	}
	require.Eventually(t, func() bool {
		lb.updateLock.RLock()
		defer lb.updateLock.RUnlock()
		return len(lb.ring.endpoints) == 10
	}, time.Second, time.Millisecond)
	consumeCentralQueueTestLogAtLane(t, exp, 3, 1, 10)
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("a failed spill tied up a consumer waiting for the busy home")
	}
}

func consumeCentralQueueTestLogAtLane(t *testing.T, exp *logExporterImp, id int64, lane uint32, lanes int) {
	t.Helper()
	for seed := 1; seed < 256; seed++ {
		key := pcommon.TraceID{byte(seed)}
		if centralQueueLaneIndex(signalKindLogs, key[:], lanes) != lane {
			continue
		}
		exp.randomTraceID = func() pcommon.TraceID { return key }
		logs := plog.NewLogs()
		logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes().PutInt("test.id", id)
		require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
		return
	}
	t.Fatal("no synthetic key for the selected test lane")
}
