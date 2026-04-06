// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricBatcherFlushesOnMaxDataPoints(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	flushed := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 2, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, md pmetric.Metrics, _ string) error {
			flushed <- md.DataPointCount()
			return nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	select {
	case got := <-flushed:
		require.Equal(t, 2, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected metric batch flush on max_datapoints")
	}
}

func TestMetricBatcherFlushesOnInterval(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	flushed := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: 50 * time.Millisecond},
		func(_ context.Context, _ *wrappedExporter, md pmetric.Metrics, _ string) error {
			flushed <- md.DataPointCount()
			return nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	select {
	case got := <-flushed:
		require.Equal(t, 1, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected metric batch flush on interval")
	}
}

func TestMetricBatcherRemoveFlushesPending(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	var flushes atomic.Int64

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			flushes.Add(1)
			return nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))
	require.NoError(t, batcher.Remove(t.Context(), "endpoint-1:4317", exp))

	require.Equal(t, int64(1), flushes.Load())
}

func TestMetricBatcherShutdownFlushesPending(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	flushed := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, md pmetric.Metrics, reason string) error {
			if reason == metricFlushReasonShutdown {
				flushed <- md.DataPointCount()
			}
			return nil
		},
	)
	require.NoError(t, err)

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))
	require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))

	select {
	case got := <-flushed:
		require.Equal(t, 1, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected metric batch flush on shutdown")
	}
}

func TestMetricBatcherRemoveTimeoutSchedulesCleanup(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error { return nil },
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	batcher.mu.RLock()
	backend := batcher.backends["endpoint-1:4317"]
	batcher.mu.RUnlock()
	require.NotNil(t, backend)

	backend.inflight.Add(1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err = batcher.Remove(ctx, "endpoint-1:4317", exp)
	require.Error(t, err)

	go backend.inflight.Done()

	require.Eventually(t, func() bool {
		select {
		case <-backend.done:
			return true
		default:
			return false
		}
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMetricBatcherRestoresPendingOnTimeoutFlushFailure(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	var calls atomic.Int64

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: 30 * time.Millisecond},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			if calls.Add(1) == 1 {
				return errors.New("boom")
			}
			return nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMetricBatcherTryEnqueueReturnsFalseWhenBackendQueueIsFull(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	sendStarted := make(chan struct{}, 1)
	blockSend := make(chan struct{})

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			select {
			case sendStarted <- struct{}{}:
			default:
			}
			<-blockSend
			return nil
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		close(blockSend)
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	select {
	case <-sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected backend send to start")
	}

	for range 16 {
		enqueued, enqueueErr := batcher.TryEnqueue("endpoint-1:4317", exp, singleDataPointMetric("m1"))
		require.NoError(t, enqueueErr)
		require.True(t, enqueued)
	}

	enqueued, enqueueErr := batcher.TryEnqueue("endpoint-1:4317", exp, singleDataPointMetric("m1"))
	require.NoError(t, enqueueErr)
	require.False(t, enqueued)
}

func singleDataPointMetric(name string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()
	g.DataPoints().AppendEmpty().SetIntValue(1)
	return md
}

func requireMetricBatcherEnqueued(t *testing.T, batcher *metricBatcher, exp *wrappedExporter, md pmetric.Metrics) {
	t.Helper()
	enqueued, err := batcher.TryEnqueue("endpoint-1:4317", exp, md)
	require.NoError(t, err)
	require.True(t, enqueued)
}
