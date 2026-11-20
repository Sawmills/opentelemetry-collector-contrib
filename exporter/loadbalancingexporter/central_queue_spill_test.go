// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
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
	consume := func(id int64, lane uint32) {
		for seed := 1; seed < 256; seed++ {
			key := pcommon.TraceID{byte(seed)}
			if centralQueueLaneIndex(signalKindLogs, key[:], 4) != lane {
				continue
			}
			p.randomTraceID = func() pcommon.TraceID { return key }
			logs := plog.NewLogs()
			logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes().PutInt("test.id", id)
			require.NoError(t, p.ConsumeLogs(t.Context(), logs))
			return
		}
		t.Fatal("no synthetic key for the selected test lane")
	}
	consume(1, 0)
	require.Eventually(t, lb.endpointHealth.failOpen, time.Second, time.Millisecond)
	consume(2, 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("slow healthy worker did not receive its home window")
	}
	consume(3, 1)
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh backlog did not reach an idle healthy worker")
	}
	require.Zero(t, freshFailures.Load(), "recently failed workers must not attract fresh spill traffic")
}
