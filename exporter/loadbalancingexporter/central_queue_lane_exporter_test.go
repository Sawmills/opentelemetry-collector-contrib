// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
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
		centralQueueNumConsumers: 1,
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
	requireCentralQueueLaneGauges(t, reader, signalKindLogs, 0, 4)

	p.observeCentralQueueLaneBytes(4<<20, now)
	p.observeCentralQueueLaneBytes(4<<20, now.Add(time.Second))

	require.Equal(t, 8, p.effectiveCentralQueueLaneCount(now.Add(time.Second)))
	requireCentralQueueLaneGauges(t, reader, signalKindLogs, 0, 8)
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
		centralQueueNumConsumers: 1,
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
	requireCentralQueueLaneGauges(t, reader, signalKindMetrics, 0, 4)

	p.observeCentralQueueLaneBytes(4<<20, now)
	p.observeCentralQueueLaneBytes(4<<20, now.Add(time.Second))

	require.Equal(t, 8, p.effectiveCentralQueueLaneCount(now.Add(time.Second)))
	requireCentralQueueLaneGauges(t, reader, signalKindMetrics, 0, 8)
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
		exporters[fmt.Sprintf("endpoint-%d", i)] = nil
	}
	return &loadBalancer{exporters: exporters}
}

func requireCentralQueueLaneGauges(t *testing.T, reader *componenttest.Telemetry, signal signalKind, configured, effective int64) {
	t.Helper()
	attrs := attribute.NewSet(attribute.String("signal", string(signal)))
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_lanes", "{lanes}", attrs, configured)
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_effective_lanes", "{lanes}", attrs, effective)
}

func requireCentralQueueConsumersStopped(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, waitForInflight(waitCtx, wg))
}
