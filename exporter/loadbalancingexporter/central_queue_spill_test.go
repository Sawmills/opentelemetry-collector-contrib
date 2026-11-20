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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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
