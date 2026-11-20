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
		_ = p.Shutdown(ctx)
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
