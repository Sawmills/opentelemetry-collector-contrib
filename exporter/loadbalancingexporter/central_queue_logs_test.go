// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestLogCentralQueueCoalescesSmallItemsBeforeSend(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := centralQueueLogsConfig()
	sink := new(consumertest.LogsSink)

	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})
	p.randomTraceID = func() pcommon.TraceID {
		return pcommon.TraceID([16]byte{1})
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("first")))
	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("second")))

	require.Eventually(t, func() bool {
		logs := sink.AllLogs()
		return len(logs) == 1 && logs[0].LogRecordCount() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestLogCentralQueueRandomRoutingDoesNotFragmentByLaneCount(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := centralQueueLogsConfig()
	cfg.CentralQueue.RequestBatching.LaneCount = 64
	sink := new(consumertest.LogsSink)

	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})
	traceIDs := distinctCentralQueueLaneTraceIDs(t, 8, cfg.CentralQueue.RequestBatching.LaneCount)
	var next atomic.Int64
	p.randomTraceID = func() pcommon.TraceID {
		index := int(next.Add(1)-1) % len(traceIDs)
		return traceIDs[index]
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	for i := 0; i < len(traceIDs); i++ {
		require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("fragment-proof")))
	}

	require.Eventually(t, func() bool {
		logs := sink.AllLogs()
		return len(logs) == 1 && logs[0].LogRecordCount() == len(traceIDs)
	}, time.Second, 10*time.Millisecond)
}

func TestLogCentralQueueRequeuesWindowAfterSendFailure(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := centralQueueLogsConfig()

	var calls atomic.Int64
	sink := new(consumertest.LogsSink)
	p, _ := newTestLogsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(ctx context.Context, ld plog.Logs) error {
			if calls.Add(1) == 1 {
				return errors.New("temporary backend failure")
			}
			return sink.ConsumeLogs(ctx, ld)
		}), nil
	})

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("retry-me")))

	require.Eventually(t, func() bool {
		logs := sink.AllLogs()
		return calls.Load() >= 2 && len(logs) == 1 && logs[0].LogRecordCount() == 1
	}, time.Second, 10*time.Millisecond)
}

func TestLogCentralQueueRecordsTelemetry(t *testing.T) {
	ts, _, telemetry := getTelemetryAssetsWithReader(t)
	cfg := centralQueueLogsConfig()
	sink := new(consumertest.LogsSink)

	p, err := newLogsExporter(ts, cfg)
	require.NoError(t, err)
	p.loadBalancer.componentFactory = func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	}
	p.randomTraceID = func() pcommon.TraceID {
		return pcommon.TraceID([16]byte{1})
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("first")))
	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("second")))
	require.Eventually(t, func() bool {
		return len(sink.AllLogs()) == 1
	}, time.Second, 10*time.Millisecond)

	signalAttrs := attribute.NewSet(attribute.String("signal", "logs"))
	enqueueAttrs := attribute.NewSet(attribute.String("signal", "logs"), attribute.String("result", "success"))
	flushAttrs := attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", string(centralQueueFlushReasonMaxDelayLowTraffic)))
	assertInt64CounterDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_enqueue_total", enqueueAttrs, 2)
	assertInt64CounterDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_flush_total", flushAttrs, 1)
	assertInt64CounterDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_underfilled_total", flushAttrs, 1)
	assertInt64HistogramDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_payloads", signalAttrs, 2)
	assertInt64HistogramDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_items", signalAttrs, 2)
	assertInt64GaugeDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_capacity_bytes", signalAttrs, cfg.CentralQueue.CapacityBytes)
	assertInt64GaugeDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_size_bytes", signalAttrs, 0)
}

func TestLogCentralQueueRecordsHardCapUnderfilledTelemetry(t *testing.T) {
	ts, _, telemetry := getTelemetryAssetsWithReader(t)
	cfg := centralQueueLogsConfig()
	cfg.CentralQueue.RequestBatching.MaxMergedItems = 1
	sink := new(consumertest.LogsSink)

	p, err := newLogsExporter(ts, cfg)
	require.NoError(t, err)
	p.loadBalancer.componentFactory = func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	}
	p.randomTraceID = func() pcommon.TraceID {
		return pcommon.TraceID([16]byte{1})
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("first")))
	require.NoError(t, p.ConsumeLogs(t.Context(), sharedScopeLogsWithoutTraceIDs("second")))
	require.Eventually(t, func() bool {
		return len(sink.AllLogs()) == 2
	}, time.Second, 10*time.Millisecond)

	hardCapAttrs := attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", string(centralQueueFlushReasonHardCap)))
	assertInt64CounterDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_flush_total", hardCapAttrs, 1)
	assertInt64CounterDataPoint(t, telemetry, "otelcol_loadbalancer_central_queue_window_underfilled_total", hardCapAttrs, 1)
}

func TestLogCentralQueueDisabledWhenTraceRoutingIsEnabled(t *testing.T) {
	cfg := centralQueueLogsConfig()
	cfg.LogRouting.IgnoreTraceID = false

	p, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	assert.Nil(t, p.centralQueue)
}

func centralQueueLogsConfig() *Config {
	cfg := simpleConfig()
	cfg.LogRouting.IgnoreTraceID = true
	cfg.CentralQueue.Enabled = true
	cfg.CentralQueue.PayloadCompression = QueuePayloadCompressionZstd
	cfg.CentralQueue.CapacityBytes = 1 << 20
	cfg.CentralQueue.NumConsumers = 1
	cfg.CentralQueue.RequestBatching.TargetCompressedBytes = 1 << 20
	cfg.CentralQueue.RequestBatching.MaxCompressedBytes = 2 << 20
	cfg.CentralQueue.RequestBatching.MaxUncompressedBytes = 4 << 20
	cfg.CentralQueue.RequestBatching.MaxMergedItems = 1000
	cfg.CentralQueue.RequestBatching.MaxDelay = 25 * time.Millisecond
	cfg.CentralQueue.RequestBatching.LaneCount = 64
	return cfg
}

func distinctCentralQueueLaneTraceIDs(t *testing.T, count, laneCount int) []pcommon.TraceID {
	t.Helper()
	traceIDs := make([]pcommon.TraceID, 0, count)
	seen := make(map[uint32]struct{}, count)
	for i := 1; len(traceIDs) < count && i < 10_000; i++ {
		traceID := pcommon.TraceID([16]byte{byte(i >> 8), byte(i)})
		lane := centralQueueLaneID(traceID[:], laneCount)
		if _, ok := seen[lane]; ok {
			continue
		}
		seen[lane] = struct{}{}
		traceIDs = append(traceIDs, traceID)
	}
	require.Len(t, traceIDs, count)
	return traceIDs
}
