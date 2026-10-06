// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
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

func TestCentralQueueLaneModesAcrossBackendChanges(t *testing.T) {
	for _, mode := range []string{"dynamic logs", "dynamic metrics", "fixed logs", "trace preserving logs"} {
		t.Run(mode, func(t *testing.T) {
			controller := centralQueueLanePathTestController()
			if mode == "fixed logs" {
				controller.fixedLaneCount = 3
			}
			logs := &logExporterImp{centralQueueLanes: controller, ignoreTraceID: mode != "trace preserving logs"}
			metrics := &metricExporterImp{centralQueueLanes: controller}
			for i, step := range []struct{ routable, resolved, want int }{
				{25, 25, 25}, // Initial membership.
				{35, 35, 35}, // Scale up inside the hysteresis band.
				{20, 35, 35}, // Quarantine retains hysteresis above the floor.
				{35, 35, 35}, // Recovery.
				{17, 17, 17}, // Large shrink crosses the hysteresis threshold.
				{25, 35, 25}, // Partial recovery raises the floor immediately.
				{35, 35, 35}, // Full recovery raises it again.
			} {
				lb := loadBalancerWithRoutableBackendCount(step.routable, step.resolved)
				logs.loadBalancer = lb
				metrics.loadBalancer = lb
				now := time.Unix(10+int64(i), 0)
				want := step.want
				switch mode {
				case "dynamic metrics":
					require.Equal(t, want, metrics.effectiveCentralQueueLaneCount(now))
				case "fixed logs":
					want = 3
				case "trace preserving logs":
					want = 64
				}
				if mode != "dynamic metrics" {
					if mode == "dynamic logs" || mode == "fixed logs" {
						want = step.routable
					}
					require.Equal(t, want, logs.effectiveCentralQueueLaneCount(now))
				}
			}
		})
	}
}

func TestConsumeLogsCentralQueueBackendChangesCoverEndpointsAndPreserveRecords(t *testing.T) {
	for _, striping := range []bool{false, true} {
		t.Run(fmt.Sprintf("record_striping=%t", striping), func(t *testing.T) {
			p := newCentralQueueLogExporter(t, 1<<20)
			p.centralQueueLaneCount = 0
			p.centralQueueLanes = centralQueueLanePathTestController()
			p.ignoreTraceID = true
			p.recordStripingEnabled = striping
			var wantBodies []string
			for phase, step := range []struct{ backends, lanes int }{{25, 25}, {35, 35}, {20, 35}, {35, 35}, {17, 17}, {25, 25}} {
				p.loadBalancer = loadBalancerWithRoutableBackendCount(step.backends, 35)
				ids := distinctCentralQueueLaneTraceIDs(t, step.backends, step.backends)
				next := 0
				p.randomTraceID = func() pcommon.TraceID {
					id := ids[next%len(ids)]
					next++
					return id
				}
				input := plog.NewLogs()
				resourceLogs := input.ResourceLogs().AppendEmpty()
				for i := range step.lanes * 2 {
					body := fmt.Sprintf("phase-%d-record-%d", phase, i)
					// Unstriped routing chooses one random key per scope.
					scope := resourceLogs.ScopeLogs().AppendEmpty()
					scope.LogRecords().AppendEmpty().Body().SetStr(body)
					wantBodies = append(wantBodies, body)
				}
				splitter, err := newCentralQueueLogSplitter(p, 1<<20, time.Unix(10+int64(phase), 0))
				require.NoError(t, err)
				require.NoError(t, splitter.consume(t.Context(), input))
				distribution := make(map[string]int)
				for _, item := range splitter.pending {
					distribution[p.loadBalancer.ring.endpointFor(item.routingKey)] += item.count
				}
				require.Len(t, distribution, step.backends, "phase %d must reach every routable endpoint", phase)
			}

			// Drain work queued before and after membership changes, including lanes
			// above the final effective count. Each input record must survive once.
			var gotBodies []string
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for p.centralQueue.len() > 0 {
				lease, err := p.centralQueue.lease(ctx)
				require.NoError(t, err)
				for _, item := range lease.window.items {
					logs, decodeErr := decodeCentralQueueLogsItem(item, p.centralCodec)
					require.NoError(t, decodeErr)
					for _, rl := range logs.ResourceLogs().All() {
						for _, sl := range rl.ScopeLogs().All() {
							for _, record := range sl.LogRecords().All() {
								gotBodies = append(gotBodies, record.Body().Str())
							}
						}
					}
				}
				lease.done()
			}
			require.ElementsMatch(t, wantBodies, gotBodies)
			require.Zero(t, p.centralQueue.compressedBytes())
		})
	}
}

func TestConsumeLogsCentralQueueWarmScaleInWrapsFirstSortedWorkers(t *testing.T) {
	p := newCentralQueueLogExporter(t, 1<<20)
	p.centralQueueLaneCount = 0
	p.centralQueueLanes = centralQueueLanePathTestController()
	p.ignoreTraceID = true
	p.recordStripingEnabled = true
	p.randomTraceID = func() pcommon.TraceID { return pcommon.TraceID{1} }

	p.loadBalancer = loadBalancerWithRoutableBackendCount(10, 10)
	require.Equal(t, 10, p.effectiveCentralQueueLaneCount(time.Now()))
	warm := plog.NewLogs()
	warm.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	require.NoError(t, p.ConsumeLogs(t.Context(), warm))

	// Keep the lane controller warm while replacing the queue for the delivery
	// phase. The real incident retained 10 lanes after DNS removed two workers.
	p.centralQueue = newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1 << 20,
		maxInflightUncompressedBytes: 1 << 20,
		maxUncompressedBatchBytes:    1 << 20,
	})
	p.loadBalancer = loadBalancerWithRoutableBackendCount(8, 10)
	require.Equal(t, 8, p.effectiveCentralQueueLaneCount(time.Now()), "assignment lanes follow the post-scale-in ring snapshot")

	input := plog.NewLogs()
	scope := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for i := range 80 {
		scope.LogRecords().AppendEmpty().Body().SetInt(int64(i))
	}
	require.NoError(t, p.ConsumeLogs(t.Context(), input))

	distribution := make(map[string]int)
	p.centralQueue.mu.Lock()
	for _, bucket := range p.centralQueue.buckets {
		for _, item := range bucket.items {
			distribution[p.loadBalancer.ring.endpointFor(item.routingKey)] += item.count
		}
	}
	p.centralQueue.mu.Unlock()

	endpoints := append([]string(nil), p.loadBalancer.ring.endpoints...)
	sort.Strings(endpoints)
	require.Len(t, distribution, len(endpoints))
	for _, endpoint := range endpoints {
		require.Equal(t, 10, distribution[endpoint], "endpoint %s received an unbalanced number of lanes", endpoint)
	}
}

func TestCentralQueueNoAffinityAssignmentsStayBalancedAcrossMembershipHistories(t *testing.T) {
	for _, workers := range []int{10, 8, 50, 45, 62, 55, 15, 17, 15, 65, 64} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			lb := loadBalancerWithRoutableBackendCount(workers, workers)
			snapshot := lb.centralQueueLogRoutingSnapshot()
			require.Len(t, snapshot.routingKeys, workers)

			distribution := make(map[string]int, workers)
			for lane, key := range snapshot.routingKeys {
				target := snapshot.endpoints[lane]
				require.Equal(t, target, lb.ring.endpointFor(key), "lane %d must target its sorted endpoint", lane)
				distribution[target]++
			}
			require.Len(t, distribution, workers)
			for _, endpoint := range snapshot.endpoints {
				require.Equal(t, 1, distribution[endpoint], "worker %s must receive exactly one lane", endpoint)
			}
		})
	}
}

func TestCentralQueueBalancedLaneRoutingKeySearchesPastLegacySaltLimit(t *testing.T) {
	seen := make(map[int]struct{}, 1025)
	for salt := uint32(0); salt <= 1024; salt++ {
		key := centralQueueLaneKey(signalKindLogs, 1, salt)
		seen[int(crc32.ChecksumIEEE(key)%maxPositions)] = struct{}{}
	}

	targetPosition := -1
	for salt := uint32(1025); salt < 1<<20; salt++ {
		position := int(crc32.ChecksumIEEE(centralQueueLaneKey(signalKindLogs, 1, salt)) % maxPositions)
		if _, exists := seen[position]; !exists {
			targetPosition = position
			break
		}
	}
	require.NotEqual(t, -1, targetPosition)

	items := make([]ringItem, maxPositions)
	for i := range items {
		items[i] = ringItem{pos: position(i), endpoint: "other"}
	}
	items[targetPosition].endpoint = "target"
	ring := &hashRing{items: items, endpoints: []string{"other", "target"}}

	key := centralQueueBalancedLaneRoutingKeyForRingUncached(ring, signalKindLogs, 1)
	require.Equal(t, "target", ring.endpointFor(key))
}

func TestCentralQueueBalancedLaneRoutingKeySearchFallbackIsBoundedAndReported(t *testing.T) {
	items := make([]ringItem, maxPositions)
	for i := range items {
		items[i] = ringItem{pos: position(i), endpoint: "other"}
	}
	ring := &hashRing{items: items, endpoints: []string{"other", "target"}}
	lb := &loadBalancer{ring: ring}

	_, err := lb.centralQueueLogRoutingSnapshotWithError()
	require.ErrorIs(t, err, errCentralQueueBalancedLaneRoutingKeySearch)
}

func TestCentralQueueNoAffinityEmptyRingKeepsRoutingBounded(t *testing.T) {
	p := newCentralQueueLogExporter(t, 1<<20)
	p.ignoreTraceID = true
	p.loadBalancer = &loadBalancer{ring: newHashRing(nil)}
	p.randomTraceID = func() pcommon.TraceID { return pcommon.TraceID{1} }

	input := plog.NewLogs()
	for range 100 {
		input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	}
	require.NoError(t, p.ConsumeLogs(t.Context(), input))
	require.LessOrEqual(t, p.centralQueue.len(), 100)
	require.Equal(t, 0, p.effectiveCentralQueueLaneCount(time.Now()))
}

func TestLogExporterCentralQueueRecordsAssignmentSnapshotLanes(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
	t.Cleanup(func() { require.NoError(t, codec.Close()) })
	p := &logExporterImp{
		centralQueue: newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           1 << 20,
			maxInflightUncompressedBytes: 1 << 20,
			maxUncompressedBatchBytes:    1 << 20,
			telemetry:                    telemetry,
		}),
		centralCodec:          codec,
		centralQueueLanes:     centralQueueLanePathTestController(),
		centralQueueLaneCount: 64,
		loadBalancer:          loadBalancerWithRoutableBackendCount(8, 8),
		ignoreTraceID:         true,
		randomTraceID:         func() pcommon.TraceID { return pcommon.TraceID{1} },
	}
	p.started.Store(true)

	input := plog.NewLogs()
	input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	require.NoError(t, p.ConsumeLogs(t.Context(), input))
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_effective_lanes", "{lanes}", attribute.NewSet(attribute.String("signal", string(signalKindLogs))), 8)
}

func TestLogExporterCentralQueueLaneBootstrapUsesHealthyBackendFloor(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 3)
	p := &logExporterImp{
		loadBalancer:             loadBalancerWithRoutableBackendCount(6, 6),
		centralQueueLanes:        controller,
		centralQueueConsumers:    consumers,
		centralQueueNumConsumers: 120,
		ignoreTraceID:            true,
	}

	require.Equal(t, 6, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueDynamicLanesKeepHealthyBackendFloor(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 3)
	p := &logExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(6, 6),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
		ignoreTraceID:         true,
	}
	var active atomic.Int64
	decision, acquired, _ := consumers.tryAcquire(&active, 1<<30, 6, false)
	require.True(t, acquired)
	require.Equal(t, 2, decision.effectiveConsumers)

	require.Equal(t, 6, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueDynamicLanesCapStaleEffectiveConsumersByCurrentBackends(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 1)
	var active atomic.Int64
	decision, acquired, _ := consumers.tryAcquire(&active, 1<<30, 8, false)
	require.True(t, acquired)
	require.Equal(t, 8, decision.effectiveConsumers)

	p := &logExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(2, 2),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
		ignoreTraceID:         true,
	}

	require.Equal(t, 2, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestLogExporterCentralQueueDynamicLanesDoNotCollapseToFractionalBackendShare(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 4)
	p := &logExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(3, 3),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
		ignoreTraceID:         true,
	}
	var active atomic.Int64
	decision, acquired, _ := consumers.tryAcquire(&active, 1<<30, 3, false)
	require.True(t, acquired)
	require.Equal(t, 1, decision.effectiveConsumers)

	require.Equal(t, 3, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
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

func TestMetricExporterCentralQueueDynamicLanesKeepHealthyBackendFloor(t *testing.T) {
	controller := centralQueueLanePathTestController()
	consumers := newCentralQueueConsumerController(120, 256<<10, 3)
	p := &metricExporterImp{
		loadBalancer:          loadBalancerWithRoutableBackendCount(6, 6),
		centralQueueLanes:     controller,
		centralQueueConsumers: consumers,
	}
	var active atomic.Int64
	decision, acquired, _ := consumers.tryAcquire(&active, 1<<30, 6, false)
	require.True(t, acquired)
	require.Equal(t, 2, decision.effectiveConsumers)

	require.Equal(t, 6, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
}

func TestMetricExporterCentralQueueDynamicLanesUseConfiguredConsumerCapacity(t *testing.T) {
	controller := centralQueueLanePathTestController()
	p := &metricExporterImp{
		loadBalancer:             loadBalancerWithRoutableBackendCount(4, 4),
		centralQueueLanes:        controller,
		centralQueueNumConsumers: 2,
	}

	require.Equal(t, 4, p.effectiveCentralQueueLaneCount(time.Unix(10, 0)))
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
