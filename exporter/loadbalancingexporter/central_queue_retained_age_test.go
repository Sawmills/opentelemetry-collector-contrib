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
	"go.opentelemetry.io/otel/attribute"
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
	require.NoError(t, p.Shutdown(t.Context()))
	_, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
	require.Error(t, err, "the stopped queue unregisters its age observation")
}

func TestCentralQueueHeldOldestLeaseBoundsAgeMetadata(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, reader.Shutdown(context.Background())) })
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes: 16, maxInflightUncompressedBytes: 16,
		maxUncompressedBatchBytes: 1, targetCompressedBytes: 1, telemetry: telemetry,
	})
	t.Cleanup(q.stop)
	base := time.Now().Add(-time.Second)
	item := centralQueueItem{signal: signalKindLogs, compressedBytes: 1, uncompressedBytes: 1, count: 1}
	require.NoError(t, q.enqueueAt(item, base))
	held, err := q.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, held)
	for i := 1; i <= 1000; i++ {
		require.NoError(t, q.enqueueAt(item, base.Add(time.Duration(i)*time.Nanosecond)))
		lease, leaseErr := q.tryLease(time.Now())
		require.NoError(t, leaseErr)
		require.NotNil(t, lease)
		lease.done()
	}
	require.Equal(t, 1, q.oldestEnqueuedAt.Len(), "completed timestamps must not remain behind a held oldest lease")
	m, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
	require.NoError(t, err)
	gauge := m.Data.(metricdata.Gauge[int64])
	require.Len(t, gauge.DataPoints, 1)
	require.GreaterOrEqual(t, gauge.DataPoints[0].Value, int64(1000))
	held.done()
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_oldest_item_age", "ms",
		attribute.NewSet(attribute.String("signal", "logs")), 0)
}
