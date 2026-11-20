// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLogsOldestItemAgeIncludesInflightUntilDelivery(t *testing.T) {
	ts, tb, reader := getTelemetryAssetsWithReader(t)
	cfg := twoEndpointLogConfig()
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxUncompressedBatchBytes: 1 << 20,
		MaxInflightUncompressedBytes: 1 << 20, TargetCompressedBytes: 1,
		NumConsumers: 1, LaneCount: 1,
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, _ plog.Logs) error {
			entered <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}), nil
	})
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
		t.Fatal("backend did not receive the accepted logs")
	}

	oldestAge := func() int64 {
		m, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
		require.NoError(t, err)
		gauge, ok := m.Data.(metricdata.Gauge[int64])
		require.True(t, ok)
		require.Len(t, gauge.DataPoints, 1)
		return gauge.DataPoints[0].Value
	}
	require.Eventually(t, func() bool { return oldestAge() >= 10 }, time.Second, 10*time.Millisecond,
		"accepted logs must keep aging while their backend send is in flight")
	finish()
	require.Eventually(t, func() bool { return oldestAge() == 0 }, time.Second, time.Millisecond,
		"delivered logs must leave the age gauge")
}
