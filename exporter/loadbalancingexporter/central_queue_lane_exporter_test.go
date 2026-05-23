// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/otel/attribute"
)

func TestLogExporterCentralQueueObservedBytesUpdateEffectiveLanes(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)

	controller := centralQueueLanePathTestController()
	p := &logExporterImp{
		loadBalancer:             loadBalancerWithBackendCount(4),
		centralQueue:             centralQueueLanePathTestQueue(telemetry),
		centralQueueLanes:        controller,
		centralQueueNumConsumers: 4,
		ignoreTraceID:            true,
	}
	ctx, cancel := context.WithCancel(t.Context())
	p.startCentralQueueConsumers(ctx)
	t.Cleanup(func() {
		cancel()
		p.centralQueue.stop()
		requireCentralQueueConsumersStopped(t, &p.centralWG)
	})

	now := time.Unix(10, 0)
	require.Equal(t, 4, p.effectiveCentralQueueLaneCount(now))
	requireCentralQueueLaneGauges(t, reader, signalKindLogs, 4)

	p.observeCentralQueueLaneBytes(4<<20, now)
	p.observeCentralQueueLaneBytes(4<<20, now.Add(time.Second))

	require.Equal(t, 8, p.effectiveCentralQueueLaneCount(now.Add(time.Second)))
	requireCentralQueueLaneGauges(t, reader, signalKindLogs, 8)
}

func TestLogExporterCentralQueueUsesRoutableBackendCountForDynamicLanes(t *testing.T) {
	controller := centralQueueLanePathTestController()
	p := &logExporterImp{
		loadBalancer:      loadBalancerWithRoutableBackendCount(2, 4),
		centralQueueLanes: controller,
		ignoreTraceID:     true,
	}

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueDynamicLanesUseLastEffectiveConsumers(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 3)
	p := &logExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(6, 6),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
		ignoreTraceID:         true,
	}
	decision := consumers.compute(1<<30, 6, false)
	require.Equal(t, 2, decision.effectiveConsumers)

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueDynamicLanesCollapseOnKnownZeroEffectiveConsumers(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 4)
	p := &logExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(3, 3),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
		ignoreTraceID:         true,
	}
	decision := consumers.compute(1<<30, 3, false)
	require.Zero(t, decision.effectiveConsumers)

	require.Equal(t, 1, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueTraceIDRoutingUsesStableLaneCount(t *testing.T) {
	controller := centralQueueLanePathTestController()
	p := &logExporterImp{
		loadBalancer:      loadBalancerWithBackendCount(4),
		centralQueueLanes: controller,
	}
	now := time.Unix(10, 0)
	traceID := traceIDWithDifferentCentralQueueLanes(t, 4, 8)

	first := centralQueueLaneRoutingKey(signalKindLogs, traceID[:], p.effectiveCentralQueueLaneCount(now))
	controller.observeCompressedBytes(16<<20, now)
	controller.observeCompressedBytes(16<<20, now.Add(time.Second))
	require.Equal(t, 8, controller.laneCount(4, now.Add(time.Second)))
	second := centralQueueLaneRoutingKey(signalKindLogs, traceID[:], p.effectiveCentralQueueLaneCount(now.Add(time.Second)))

	require.Equal(t, first, second)
}

func TestMetricExporterCentralQueueObservedBytesUpdateEffectiveLanes(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindMetrics)
	require.NoError(t, err)

	controller := centralQueueLanePathTestController()
	p := &metricExporterImp{
		loadBalancer:             loadBalancerWithBackendCount(4),
		centralQueue:             centralQueueLanePathTestQueue(telemetry),
		centralQueueLanes:        controller,
		centralQueueNumConsumers: 4,
	}
	ctx, cancel := context.WithCancel(t.Context())
	p.startCentralQueueConsumers(ctx)
	t.Cleanup(func() {
		cancel()
		p.centralQueue.stop()
		requireCentralQueueConsumersStopped(t, &p.centralWG)
	})

	now := time.Unix(10, 0)
	require.Equal(t, 4, p.effectiveCentralQueueLaneCount(now))
	requireCentralQueueLaneGauges(t, reader, signalKindMetrics, 4)

	p.observeCentralQueueLaneBytes(4<<20, now)
	p.observeCentralQueueLaneBytes(4<<20, now.Add(time.Second))

	require.Equal(t, 8, p.effectiveCentralQueueLaneCount(now.Add(time.Second)))
	requireCentralQueueLaneGauges(t, reader, signalKindMetrics, 8)
}

func TestMetricExporterCentralQueueObservationReusesCachedBackendCount(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindMetrics)
	require.NoError(t, err)

	controller := centralQueueLanePathTestController()
	p := &metricExporterImp{
		loadBalancer:      loadBalancerWithBackendCount(2),
		centralQueue:      centralQueueLanePathTestQueue(telemetry),
		centralQueueLanes: controller,
	}
	now := time.Unix(10, 0)
	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(now))

	p.loadBalancer = loadBalancerWithBackendCount(8)
	p.observeCentralQueueLaneBytes(16<<20, now)
	p.observeCentralQueueLaneBytes(16<<20, now.Add(time.Second))

	controller.mu.Lock()
	lastBackendCount := controller.lastBackendCount
	effectiveLaneCount := controller.effectiveLaneCount
	controller.mu.Unlock()
	require.Equal(t, 2, lastBackendCount)
	require.Equal(t, 4, effectiveLaneCount)
}

func TestMetricExporterCentralQueueUsesRoutableBackendCountForDynamicLanes(t *testing.T) {
	controller := centralQueueLanePathTestController()
	p := &metricExporterImp{
		loadBalancer:      loadBalancerWithRoutableBackendCount(2, 4),
		centralQueueLanes: controller,
	}

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestMetricExporterCentralQueueDynamicLanesUseLastEffectiveConsumers(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 3)
	p := &metricExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(6, 6),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
	}
	decision := consumers.compute(1<<30, 6, false)
	require.Equal(t, 2, decision.effectiveConsumers)

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestMetricExporterCentralQueueDynamicLanesUseConfiguredConsumerCapacity(t *testing.T) {
	controller := centralQueueLanePathTestController()
	p := &metricExporterImp{
		loadBalancer:             loadBalancerWithRoutableBackendCount(4, 4),
		centralQueueLanes:        controller,
		centralQueueNumConsumers: 2,
	}

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func centralQueueLanePathTestController() *centralQueueLaneController {
	cfg := createDefaultConfig().(*Config).CentralQueue
	cfg.LaneCount = 0
	cfg.MinLanes = 1
	cfg.MaxLanes = 64
	cfg.BackendLaneMultiplier = 2
	cfg.TargetCompressedBytes = 256 << 10
	cfg.TargetLaneFillDuration = 500 * time.Millisecond
	cfg.LaneHysteresisFactor = 2
	controller := newCentralQueueLaneController(cfg)
	controller.rateWindow = time.Second
	return controller
}

func centralQueueLanePathTestQueue(telemetry *centralQueueTelemetry) *centralQueue {
	return newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1 << 20,
		maxInflightUncompressedBytes: 1 << 20,
		maxUncompressedBatchBytes:    1 << 20,
		targetCompressedBytes:        256 << 10,
		maxBatchDelay:                time.Second,
		telemetry:                    telemetry,
	})
}

func loadBalancerWithBackendCount(count int) *loadBalancer {
	exporters := make(map[string]*wrappedExporter, count)
	for i := range count {
		endpoint := fmt.Sprintf("endpoint-%d", i)
		exporters[endpoint] = newWrappedExporter(mockComponent{}, endpoint)
	}
	return &loadBalancer{exporters: exporters}
}

func loadBalancerWithRoutableBackendCount(routableCount, exporterCount int) *loadBalancer {
	exporters := make(map[string]*wrappedExporter, exporterCount)
	endpoints := make([]string, 0, routableCount)
	for i := range exporterCount {
		endpoint := fmt.Sprintf("endpoint-%d:4317", i)
		exporters[endpoint] = newWrappedExporter(mockComponent{}, endpoint)
		if i < routableCount {
			endpoints = append(endpoints, endpoint)
		}
	}
	return &loadBalancer{
		exporters: exporters,
		ring:      newHashRing(endpoints),
	}
}

func traceIDWithDifferentCentralQueueLanes(t *testing.T, firstLaneCount, secondLaneCount int) pcommon.TraceID {
	t.Helper()
	for i := 1; i < 10000; i++ {
		traceID := pcommon.TraceID{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		first := centralQueueLaneRoutingKey(signalKindLogs, traceID[:], firstLaneCount)
		second := centralQueueLaneRoutingKey(signalKindLogs, traceID[:], secondLaneCount)
		if !bytes.Equal(first, second) {
			return traceID
		}
	}
	t.Fatal("expected trace ID with different lane routing keys")
	return pcommon.TraceID{}
}

func requireCentralQueueLaneGauges(t *testing.T, reader *componenttest.Telemetry, signal signalKind, effective int64) {
	t.Helper()
	attrs := attribute.NewSet(attribute.String("signal", string(signal)))
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_lanes", "{lanes}", attrs, 0)
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_effective_lanes", "{lanes}", attrs, effective)
}

func requireCentralQueueConsumersStopped(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
	defer cancel()
	require.NoError(t, waitForInflight(waitCtx, wg))
}
