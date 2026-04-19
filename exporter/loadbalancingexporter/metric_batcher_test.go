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
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = batcher.Shutdown(context.WithoutCancel(t.Context()))
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
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = batcher.Shutdown(context.WithoutCancel(t.Context()))
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

func TestMetricBatcherFlushesOnMaxBytes(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	flushed := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, md pmetric.Metrics, _ string) error {
			flushed <- md.DataPointCount()
			return nil
		},
		nil,
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
		t.Fatal("expected metric batch flush on max_bytes")
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
		nil,
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
		nil,
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
		nil,
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
		nil,
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

func TestMetricBatcherRestoresPendingBytesFromMergedPayloadOnFailure(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	firstFailed := make(chan struct{}, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: 200 * time.Millisecond},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			select {
			case firstFailed <- struct{}{}:
			default:
			}
			return errors.New("boom")
		},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = batcher.Shutdown(context.WithoutCancel(t.Context()))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	md1 := singleDataPointMetric("m1")
	md2 := singleDataPointMetric("m1")
	requireMetricBatcherEnqueued(t, batcher, exp, md1)
	requireMetricBatcherEnqueued(t, batcher, exp, md2)

	select {
	case <-firstFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("expected first timeout flush failure")
	}

	expected := mergePendingMetricChunks([]pmetric.Metrics{singleDataPointMetric("m1"), singleDataPointMetric("m1")})
	expectedBytes := (&pmetric.ProtoMarshaler{}).MetricsSize(expected)

	require.Eventually(t, func() bool {
		for _, p := range batcher.snapshotPending().pending {
			if p.endpoint == "endpoint-1:4317" {
				return p.datapoints == 2 && int(p.bytes) == expectedBytes
			}
		}
		return false
	}, time.Second, 20*time.Millisecond)
}

func TestMetricBatcherRecordsPendingOldestAgeAndFlushAge(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, telemetry.Shutdown(context.Background()))
	})

	batcher, err := newMetricBatcher(
		componenttest.NewNopTelemetrySettings().Logger,
		telemetry.NewTelemetrySettings(),
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			return nil
		},
		nil,
	)
	require.NoError(t, err)

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))
	time.Sleep(25 * time.Millisecond)

	pendingMetric, err := telemetry.GetMetric("otelcol_loadbalancer_metric_batch_pending_oldest_datapoint_age")
	require.NoError(t, err)
	pendingGauge, ok := pendingMetric.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Greater(t, findGaugePointValue(t, pendingGauge.DataPoints, attribute.NewSet(attribute.String("endpoint", "endpoint-1:4317"))), int64(0))

	maxMetric, err := telemetry.GetMetric("otelcol_loadbalancer_metric_batch_pending_oldest_datapoint_age_max")
	require.NoError(t, err)
	maxGauge, ok := maxMetric.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Greater(t, maxGauge.DataPoints[0].Value, int64(0))

	require.NoError(t, batcher.Shutdown(t.Context()))

	flushMetric, err := telemetry.GetMetric("otelcol_loadbalancer_metric_batch_flush_oldest_datapoint_age")
	require.NoError(t, err)
	flushHistogram, ok := flushMetric.Data.(metricdata.Histogram[int64])
	require.True(t, ok)
	require.Len(t, flushHistogram.DataPoints, 1)
	require.Equal(t, uint64(1), flushHistogram.DataPoints[0].Count)
	require.Greater(t, flushHistogram.DataPoints[0].Sum, int64(0))
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
		nil,
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

func TestMetricBatcherRemoveReroutesOnResolverChangeFlushFailure(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	rerouted := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			return errors.New("flush failed")
		},
		func(_ context.Context, md pmetric.Metrics, reason string) error {
			require.Equal(t, metricFlushReasonResolverChange, reason)
			rerouted <- md.DataPointCount()
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

	select {
	case got := <-rerouted:
		require.Equal(t, 1, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected resolver-change drain reroute callback")
	}
}

func TestMetricBatcherDropsPendingWhenRetryBufferCapExceeded(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	var calls atomic.Int64
	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			calls.Add(1)
			return errors.New("still failing")
		},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))
	})

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	for range defaultMetricBatchRetryBufferMultiplier + 1 {
		requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))
	}

	require.Eventually(t, func() bool {
		return calls.Load() > 0
	}, 2*time.Second, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		for _, p := range batcher.snapshotPending().pending {
			if p.endpoint == "endpoint-1:4317" {
				return p.datapoints == 0
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMetricBatcherDropsPendingWhenRetryAgeExceeded(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	var calls atomic.Int64

	oldRetryAge := metricBatcherMaxRetryAge
	metricBatcherMaxRetryAge = 30 * time.Millisecond
	t.Cleanup(func() {
		metricBatcherMaxRetryAge = oldRetryAge
	})

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: 10 * time.Millisecond},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			calls.Add(1)
			return errors.New("still failing")
		},
		nil,
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

	require.Eventually(t, func() bool {
		for _, p := range batcher.snapshotPending().pending {
			if p.endpoint == "endpoint-1:4317" {
				return p.datapoints == 0
			}
		}
		return true
	}, 2*time.Second, 20*time.Millisecond)
}

func TestMetricBatcherShutdownReroutesOnShutdownFlushFailure(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	rerouted := make(chan int, 1)

	batcher, err := newMetricBatcher(
		ts.Logger,
		ts.TelemetrySettings,
		metricBatcherSettings{maxDataPoints: 1000, maxBytes: 1 << 20, flushInterval: time.Hour},
		func(_ context.Context, _ *wrappedExporter, _ pmetric.Metrics, _ string) error {
			return errors.New("flush failed")
		},
		func(_ context.Context, md pmetric.Metrics, reason string) error {
			require.Equal(t, metricFlushReasonShutdown, reason)
			rerouted <- md.DataPointCount()
			return nil
		},
	)
	require.NoError(t, err)

	exp := newWrappedExporter(newNopMockMetricsExporter(), "endpoint-1:4317")
	requireMetricBatcherEnqueued(t, batcher, exp, singleDataPointMetric("m1"))

	require.NoError(t, batcher.Shutdown(context.WithoutCancel(t.Context())))

	select {
	case got := <-rerouted:
		require.Equal(t, 1, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected shutdown drain reroute callback")
	}
}

func TestMergePendingMetricChunksMergesDistinctMetricNames(t *testing.T) {
	md1 := singleDataPointMetric("m1")
	md2 := singleDataPointMetric("m2")

	merged := mergePendingMetricChunks([]pmetric.Metrics{md1, md2})

	require.Equal(t, 2, merged.DataPointCount())
	require.Len(t, splitMetricsByMetricName(merged), 2)
}

func TestMergePendingMetricChunksMergesSameMetricNameDataPoints(t *testing.T) {
	md1 := singleDataPointMetric("m1")
	md2 := singleDataPointMetric("m1")

	merged := mergePendingMetricChunks([]pmetric.Metrics{md1, md2})

	require.Equal(t, 2, merged.DataPointCount())
	split := splitMetricsByMetricName(merged)
	require.Len(t, split, 1)
	require.Contains(t, split, "m1")
	require.Equal(t, 2, split["m1"].DataPointCount())
}

func TestMergePendingMetricChunksMergesByResourceScopeAndMetricIdentity(t *testing.T) {
	md1 := metricWithResourceScopeGauge("host-a", "scope-a", "m1", 1)
	md2 := metricWithResourceScopeGauge("host-a", "scope-a", "m1", 2)

	merged := mergePendingMetricChunks([]pmetric.Metrics{md1, md2})

	require.Equal(t, 1, merged.ResourceMetrics().Len())
	rm := merged.ResourceMetrics().At(0)
	require.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())
	g := sm.Metrics().At(0).Gauge()
	require.Equal(t, 2, g.DataPoints().Len())
}

func TestMergePendingMetricChunksKeepsDistinctResourcesSeparate(t *testing.T) {
	md1 := metricWithResourceScopeGauge("host-a", "scope-a", "m1", 1)
	md2 := metricWithResourceScopeGauge("host-b", "scope-a", "m1", 2)

	merged := mergePendingMetricChunks([]pmetric.Metrics{md1, md2})

	require.Equal(t, 2, merged.ResourceMetrics().Len())
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

func metricWithResourceScopeGauge(resourceValue, scopeName, metricName string, value int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("host.name", resourceValue)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetIntValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(value, 0)))
	return md
}

func requireMetricBatcherEnqueued(t *testing.T, batcher *metricBatcher, exp *wrappedExporter, md pmetric.Metrics) {
	t.Helper()
	enqueued, err := batcher.TryEnqueue("endpoint-1:4317", exp, md)
	require.NoError(t, err)
	require.True(t, enqueued)
}
