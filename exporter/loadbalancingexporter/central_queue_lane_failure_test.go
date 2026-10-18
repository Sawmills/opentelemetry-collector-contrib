// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCentralQueueLaneKeyFailureDoesNotBlockHealthyTraffic(t *testing.T) {
	exporter, _ := newLaneFailureTestExporter(t)
	exporter.randomTraceID = func() pcommon.TraceID { return traceIDForQueueLane(t, 0) }
	require.NoError(t, exporter.ConsumeLogs(t.Context(), sharedResourceScopeLog("healthy traffic")))
	require.Equal(t, []string{"healthy traffic"}, drainLaneFailureBodies(t, exporter))
}

func TestCentralQueueRandomRoutingSkipsFailedLane(t *testing.T) {
	exporter, reader := newLaneFailureTestExporter(t)
	logs, observed := observer.New(zap.WarnLevel)
	exporter.loadBalancer.logger = zap.New(logs)
	exporter.randomTraceID = func() pcommon.TraceID { return traceIDForQueueLane(t, 1) }
	input := plog.NewLogs()
	rl := input.ResourceLogs().AppendEmpty()
	var want []string
	for i := range 20 {
		body := fmt.Sprintf("random-scope-%d", i)
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(body)
		want = append(want, body)
	}
	require.NoError(t, exporter.ConsumeLogs(t.Context(), input))
	require.ElementsMatch(t, want, drainLaneFailureBodies(t, exporter))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_skips", "{skips}", attribute.NewSet(attribute.String("signal", "logs")), 1)
	// A cached failed lane is still skipped and counted, without repeating the
	// per-ring warning or rejecting the second intake request.
	require.NoError(t, exporter.ConsumeLogs(t.Context(), input))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_skips", "{skips}", attribute.NewSet(attribute.String("signal", "logs")), 2)
	require.Len(t, observed.All(), 1)
	require.Equal(t, int64(1), observed.All()[0].ContextMap()["failed_lanes"])
}

func TestCentralQueueNoUsableLaneRejectsAtomically(t *testing.T) {
	for _, striping := range []bool{false, true} {
		t.Run(fmt.Sprintf("striping=%t", striping), func(t *testing.T) {
			exporter, reader := newLaneFailureTestExporter(t)
			logs, observed := observer.New(zap.WarnLevel)
			exporter.logger = zap.New(logs)
			exporter.loadBalancer.logger = zap.New(logs)
			exporter.recordStripingEnabled = striping
			exporter.observeCentralQueueLaneAssignment(2)
			for i := range exporter.loadBalancer.ring.items {
				exporter.loadBalancer.ring.items[i].endpoint = "unlisted"
			}
			require.ErrorIs(t, exporter.ConsumeLogs(t.Context(), sharedResourceScopeLog("cannot route")), errCentralQueueBalancedLaneRoutingKeySearch)
			require.Zero(t, exporter.centralQueue.len())
			requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_failures", "{failures}", attribute.NewSet(attribute.String("signal", "logs")), 1)
			requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_effective_lanes", "{lanes}", attribute.NewSet(attribute.String("signal", "logs")), 0)
			requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_skips", "{skips}", attribute.NewSet(attribute.String("signal", "logs")), 2)
			require.ErrorIs(t, exporter.ConsumeLogs(t.Context(), sharedResourceScopeLog("still cannot route")), errCentralQueueBalancedLaneRoutingKeySearch)
			require.Zero(t, exporter.centralQueue.len())
			requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_failures", "{failures}", attribute.NewSet(attribute.String("signal", "logs")), 2)
			requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_skips", "{skips}", attribute.NewSet(attribute.String("signal", "logs")), 4)
			require.Len(t, observed.All(), 1)
			require.Equal(t, int64(2), observed.All()[0].ContextMap()["failed_lanes"])
		})
	}
}

func TestCentralQueueLaneSkipKeepsAdmissionAtomic(t *testing.T) {
	ids := make([]pcommon.TraceID, 20)
	for i := range ids {
		ids[i] = pcommon.TraceID{byte(i + 1)}
	}
	request := func() plog.Logs { return centralQueueLaneLogs(ids, 1000) }
	measure, _ := newLaneFailureTestExporter(t)
	measure.centralQueue.settings.maxUncompressedBatchBytes = 2048
	measure.randomTraceID = func() pcommon.TraceID { return traceIDForQueueLane(t, 1) }
	require.NoError(t, measure.ConsumeLogs(t.Context(), request()))
	require.Greater(t, measure.centralQueue.len(), 1)
	requestBytes := measure.centralQueue.compressedBytes()

	exporter, _ := newLaneFailureTestExporter(t)
	exporter.centralQueue.settings.maxUncompressedBatchBytes = 2048
	exporter.centralQueue.settings.maxCompressedBytes = requestBytes + 100
	exporter.randomTraceID = func() pcommon.TraceID { return traceIDForQueueLane(t, 1) }
	require.NoError(t, exporter.ConsumeLogs(t.Context(), centralQueueLaneLogs(ids[:1], 150)))
	beforeItems, beforeBytes := exporter.centralQueue.len(), exporter.centralQueue.compressedBytes()
	require.Greater(t, beforeBytes, int64(100))
	require.ErrorIs(t, exporter.ConsumeLogs(t.Context(), request()), errCentralQueueFull)
	require.Equal(t, beforeItems, exporter.centralQueue.len())
	require.Equal(t, beforeBytes, exporter.centralQueue.compressedBytes())
}

func TestCentralQueueTraceAffinityDoesNotSkipFailedLane(t *testing.T) {
	exporter, _ := newLaneFailureTestExporter(t)
	exporter.ignoreTraceID = false
	exporter.centralQueueLaneCount = 2
	input := sharedResourceScopeLog("affinity-bound")
	input.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetTraceID(traceIDForQueueLane(t, 1))
	require.ErrorIs(t, exporter.ConsumeLogs(t.Context(), input), errCentralQueueBalancedLaneRoutingKeySearch)
	require.Zero(t, exporter.centralQueue.len())
}

func TestCentralQueueStripingSkipsFailedLane(t *testing.T) {
	exporter, reader := newLaneFailureTestExporter(t)
	exporter.recordStripingEnabled = true
	exporter.randomTraceID = func() pcommon.TraceID { return traceIDForQueueLane(t, 0) }
	input := plog.NewLogs()
	records := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	var want []string
	for i := range 16 {
		body := fmt.Sprintf("striped-record-%d", i)
		records.AppendEmpty().Body().SetStr(body)
		want = append(want, body)
	}
	require.NoError(t, exporter.ConsumeLogs(t.Context(), input))
	require.ElementsMatch(t, want, drainLaneFailureBodies(t, exporter))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_routing_key_skips", "{skips}", attribute.NewSet(attribute.String("signal", "logs")), 1)
}

func TestCentralQueueRandomRoutingBalancesUsableLanes(t *testing.T) {
	exporter, reader := newLaneFailureTestExporter(t)
	ring := newHashRing([]string{"a", "bad", "c", "d"})
	for i := range ring.items {
		if endpointWithPort(ring.items[i].endpoint) == endpointWithPort("bad") {
			ring.items[i].endpoint = "a"
		}
	}
	exporter.loadBalancer = &loadBalancer{ring: ring, logger: zap.NewNop()}
	var next uint64
	exporter.randomTraceID = func() pcommon.TraceID {
		next++
		var id pcommon.TraceID
		binary.LittleEndian.PutUint64(id[:8], next)
		return id
	}
	input := plog.NewLogs()
	rl := input.ResourceLogs().AppendEmpty()
	for range 6000 {
		rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("balance")
	}
	require.NoError(t, exporter.ConsumeLogs(t.Context(), input))
	counts := make(map[string]int)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for exporter.centralQueue.len() > 0 {
		lease, err := exporter.centralQueue.lease(ctx)
		require.NoError(t, err)
		for _, item := range lease.window.items {
			logs, decodeErr := decodeCentralQueueLogsItem(item, exporter.centralCodec)
			require.NoError(t, decodeErr)
			counts[endpointWithPort(ring.endpointFor(item.routingKey))] += logs.LogRecordCount()
		}
		lease.done()
	}
	for _, endpoint := range []string{"a", "c", "d"} {
		require.InDelta(t, 2000, counts[endpointWithPort(endpoint)], 400)
	}
	require.Zero(t, counts[endpointWithPort("bad")])
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_effective_lanes", "{lanes}", attribute.NewSet(attribute.String("signal", "logs")), 3)
}

func newLaneFailureTestExporter(t *testing.T) (*logExporterImp, *componenttest.Telemetry) {
	t.Helper()
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context()))) })
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	exporter := newPreflightTestExporter(t, 1<<20)
	exporter.logger = zap.NewNop()
	exporter.centralQueue.settings.telemetry = telemetry
	ring := newHashRing([]string{"healthy", "target"})
	// Model a resolver-listed endpoint with no hash-ring positions. Only that
	// endpoint's bounded lane-key search fails.
	for i := range ring.items {
		ring.items[i].endpoint = "healthy"
	}
	exporter.loadBalancer = &loadBalancer{ring: ring, logger: zap.NewNop()}
	return exporter, reader
}

func traceIDForQueueLane(t *testing.T, lane uint32) pcommon.TraceID {
	t.Helper()
	for value := 1; value < 256; value++ {
		id := pcommon.TraceID{byte(value)}
		if centralQueueLaneIndex(signalKindLogs, id[:], 2) == lane {
			return id
		}
	}
	t.Fatal("no trace ID for requested test lane")
	return pcommon.TraceID{}
}

func drainLaneFailureBodies(t *testing.T, exporter *logExporterImp) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var bodies []string
	for exporter.centralQueue.len() > 0 {
		lease, err := exporter.centralQueue.lease(ctx)
		require.NoError(t, err)
		for _, item := range lease.window.items {
			require.Equal(t, "healthy", exporter.loadBalancer.ring.endpointFor(item.routingKey))
			logs, decodeErr := decodeCentralQueueLogsItem(item, exporter.centralCodec)
			require.NoError(t, decodeErr)
			for _, rl := range logs.ResourceLogs().All() {
				for _, sl := range rl.ScopeLogs().All() {
					for _, record := range sl.LogRecords().All() {
						bodies = append(bodies, record.Body().Str())
					}
				}
			}
		}
		lease.done()
	}
	return bodies
}
