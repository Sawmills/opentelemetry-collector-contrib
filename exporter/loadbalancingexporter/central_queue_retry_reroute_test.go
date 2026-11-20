// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLogsCentralQueueRetryEscapesFailedHomeUnderFailOpen(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.LogRouting.IgnoreTraceID = true
	cfg.LogRouting.RecordStripingEnabled = true
	cfg.EndpointHealth = EndpointHealthConfig{
		Enabled: true, RerouteOnFailure: true, MaxRerouteAttempts: 1,
		MinEligibleBackends: 2, MaxQuarantinedPercent: 100,
		QuarantineDuration: time.Minute,
	}
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1,
		NumConsumers: 2, LaneCount: 1,
	}
	var homeAttempts atomic.Int64
	var homeEndpoint atomic.Pointer[string]
	delivered := make(chan int, 1)
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
		return newMockLogsExporter(func(_ context.Context, logs plog.Logs) error {
			homeEndpoint.CompareAndSwap(nil, &endpoint)
			if endpoint == *homeEndpoint.Load() {
				homeAttempts.Add(1)
				return status.Error(codes.Unavailable, "home endpoint is unreachable")
			}
			delivered <- logs.LogRecordCount()
			return nil
		}), nil
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		require.NoError(t, p.Shutdown(ctx))
	})
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	input := simpleLogs()
	expected := input.LogRecordCount()
	require.NoError(t, p.ConsumeLogs(t.Context(), input))

	select {
	case count := <-delivered:
		require.Equal(t, expected, count, "rerouting must retain all accepted logs")
		require.EqualValues(t, 3, homeAttempts.Load(), "home retries must have a fixed bound before rerouting")
	case <-time.After(2 * time.Second):
		t.Fatal("accepted logs stayed pinned to the failed home endpoint under fail-open")
	}
}

func TestLogsCentralQueueFailOpenHonorsRerouteCap(t *testing.T) {
	for _, limit := range []int{0, 1} {
		name := "disabled"
		if limit == 1 {
			name = "one_alternate"
		}
		t.Run(name, func(t *testing.T) {
			settings, tb := getTelemetryAssets(t)
			cfg := twoEndpointLogConfig()
			cfg.Resolver.Static = configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1:4317", "endpoint-2:4317", "endpoint-3:4317"}})
			cfg.EndpointHealth = EndpointHealthConfig{
				Enabled: true, RerouteOnFailure: true, MaxRerouteAttempts: limit,
				MinEligibleBackends: 3, MaxQuarantinedPercent: 100, QuarantineDuration: time.Minute,
			}
			cfg.CentralQueue = CentralQueueConfig{
				Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
				MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
				MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1, NumConsumers: 3,
			}
			attempts := make(chan string, 32)
			exp, _ := newTestLogsExporter(t, settings, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
				return newMockLogsExporter(func(context.Context, plog.Logs) error {
					select {
					case attempts <- endpoint:
					default:
					}
					return status.Error(codes.Unavailable, "backend unavailable")
				}), nil
			})
			require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				require.Error(t, exp.Shutdown(ctx), "undeliverable accepted work must report its forced shutdown")
			})
			require.NoError(t, exp.ConsumeLogs(t.Context(), simpleLogs()))
			seen := make(map[string]bool)
			deadline := time.After(12 * time.Second)
			for i := 0; i < 4+limit*3; i++ {
				select {
				case endpoint := <-attempts:
					seen[endpoint] = true
					require.LessOrEqual(t, len(seen), limit+1, "fail-open queue retries must preserve the configured destination cap")
				case <-deadline:
					t.Fatal("did not observe retries beyond the configured reroute cap")
				}
			}
		})
	}
}

func TestLogsCentralQueueRetrySkipsKnownFailedAlternateWithinCap(t *testing.T) {
	settings, tb := getTelemetryAssets(t)
	cfg := twoEndpointLogConfig()
	cfg.Resolver.Static = configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1:4317", "endpoint-2:4317", "endpoint-3:4317"}})
	cfg.LogRouting.IgnoreTraceID = true
	cfg.LogRouting.RecordStripingEnabled = true
	cfg.EndpointHealth = EndpointHealthConfig{
		Enabled: true, RerouteOnFailure: true, MaxRerouteAttempts: 2,
		MinEligibleBackends: 3, MaxQuarantinedPercent: 100, QuarantineDuration: time.Minute,
	}
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1, NumConsumers: 3,
	}
	failedAlternate := make(chan struct{}, 1)
	delivered := make(chan struct{}, 1)
	var callsToFailedAlternate atomic.Int64
	exp, lb := newTestLogsExporter(t, settings, tb, cfg, func(_ context.Context, endpoint string) (component.Component, error) {
		return newMockLogsExporter(func(_ context.Context, logs plog.Logs) error {
			id, _ := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Get("test.id")
			if endpoint == "endpoint-2:4317" {
				if id.Int() == 2 {
					callsToFailedAlternate.Add(1)
				}
				select {
				case failedAlternate <- struct{}{}:
				default:
				}
			}
			if endpoint != "endpoint-3:4317" {
				return status.Error(codes.Unavailable, "backend unavailable")
			}
			if id.Int() == 2 {
				select {
				case delivered <- struct{}{}:
				default:
				}
			}
			return nil
		}), nil
	})
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		require.NoError(t, exp.Shutdown(ctx))
	})
	consumeCentralQueueTestLogAtLane(t, exp, 1, 1, 3)
	select {
	case <-failedAlternate:
	case <-time.After(time.Second):
		t.Fatal("alternate failure was not observed")
	}
	require.Eventually(t, func() bool { return !lb.endpointHealth.canAcceptSpill("endpoint-2:4317") }, time.Second, time.Millisecond)
	consumeCentralQueueTestLogAtLane(t, exp, 2, 0, 3)
	select {
	case <-delivered:
	case <-time.After(12 * time.Second):
		t.Fatal("queued logs did not reach the healthy permitted alternate")
	}
	require.Zero(t, callsToFailedAlternate.Load(), "a known failed alternate must not receive rerouted windows")
}
