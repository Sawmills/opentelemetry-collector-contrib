// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// centralQueueLaneLogs builds one log record per trace id (each a distinct
// routing key -> distinct central-queue lane/item) with high-entropy bodies so
// per-item byte accounting is predictable under compression.
func centralQueueLaneLogs(ids []pcommon.TraceID, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for n, id := range ids {
		lr := sl.LogRecords().AppendEmpty()
		b := make([]byte, bodyLen)
		for k := range b {
			b[k] = byte((n*131 + k*17 + k*k) % 251)
		}
		lr.Body().SetEmptyBytes().FromRaw(b)
		lr.SetTraceID(id)
	}
	return ld
}

// centralQueueServiceMetrics builds one gauge datapoint per service name (each a
// distinct routing key under svcRouting -> distinct lane/item) with a
// high-entropy padding attribute to make per-item byte accounting predictable.
func centralQueueServiceMetrics(services []string, padLen int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for n, svc := range services {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr(serviceNameKey, svc)
		g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		g.SetName("m")
		dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(int64(n))
		b := make([]byte, padLen)
		for k := range b {
			b[k] = byte((n*131 + k*17 + k*k) % 251)
		}
		dp.Attributes().PutStr("pad", string(b))
	}
	return md
}

func newCentralQueueLogExporter(t *testing.T, maxCompressedBytes int64) *logExporterImp {
	t.Helper()
	codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
	t.Cleanup(func() { require.NoError(t, codec.Close()) })
	p := &logExporterImp{
		centralQueue: newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           maxCompressedBytes,
			maxInflightUncompressedBytes: 1 << 20,
			maxUncompressedBatchBytes:    1 << 20,
		}),
		centralCodec:          codec,
		centralQueueLaneCount: 64,
		randomTraceID:         func() pcommon.TraceID { return pcommon.TraceID{1} },
	}
	p.started.Store(true)
	return p
}

func newCentralQueueMetricExporter(t *testing.T, maxCompressedBytes int64) *metricExporterImp {
	t.Helper()
	codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
	t.Cleanup(func() { require.NoError(t, codec.Close()) })
	p := &metricExporterImp{
		centralQueue: newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           maxCompressedBytes,
			maxInflightUncompressedBytes: 1 << 20,
			maxUncompressedBatchBytes:    1 << 20,
		}),
		centralCodec: codec,
		routingKey:   svcRouting,
	}
	p.started.Store(true)
	return p
}

// TestConsumeLogsCentralQueue_NoPartialCommitOnQueueFull is the SAW-10951
// regression for the incident's actual case: the queue is near capacity, every
// item in a multi-lane request fits on its own, but the request as a whole no
// longer fits the *remaining* capacity. Committing item-by-item would accept some
// lanes and reject a later one while still returning an error; a retrying client
// then re-delivers the accepted lanes as duplicates. Admission must be
// all-or-nothing: the request commits zero items and returns a transient
// errCentralQueueFull.
func TestConsumeLogsCentralQueue_NoPartialCommitOnQueueFull(t *testing.T) {
	const laneCount = 64
	ids := distinctCentralQueueLaneTraceIDs(t, 7, laneCount)
	fillerIDs, reqIDs := ids[:1], ids[1:]
	request := func() plog.Logs { return centralQueueLaneLogs(reqIDs, 500) }

	// Measure the request in a spacious queue: every lane commits, proving each
	// item fits on its own, and gives the exact committed size.
	measure := newCentralQueueLogExporter(t, 1<<20)
	require.NoError(t, measure.ConsumeLogs(t.Context(), request()))
	require.Equal(t, len(reqIDs), measure.centralQueue.len(), "each lane item must fit on its own")
	requestBytes := measure.centralQueue.compressedBytes()
	require.Positive(t, requestBytes)

	// Size a fresh queue so the request fits in an empty queue (<= max) but not
	// once a small filler is already queued -> transient full, not too-large.
	p := newCentralQueueLogExporter(t, requestBytes+100)
	require.NoError(t, p.ConsumeLogs(t.Context(), centralQueueLaneLogs(fillerIDs, 150)))
	preLen := p.centralQueue.len()
	preBytes := p.centralQueue.compressedBytes()
	require.Positive(t, preBytes)

	err := p.ConsumeLogs(t.Context(), request())
	require.ErrorIs(t, err, errCentralQueueFull, "the full request no longer fits the remaining capacity")
	require.False(t, consumererror.IsPermanent(err), "a full queue is transient backpressure, safe to retry")
	require.Equal(t, preLen, p.centralQueue.len(), "on overflow NOTHING from the request is committed (SAW-10951)")
	require.Equal(t, preBytes, p.centralQueue.compressedBytes(), "queue byte accounting must be unchanged")
}

// TestConsumeLogsCentralQueue_RejectsRequestLargerThanQueuePermanently covers the
// distinct terminal case: a request larger than the whole queue can never succeed
// by retrying, so it must be rejected permanently rather than as transient
// backpressure (which would make a retry-forever client loop forever).
func TestConsumeLogsCentralQueue_RejectsRequestLargerThanQueuePermanently(t *testing.T) {
	const laneCount = 64
	reqIDs := distinctCentralQueueLaneTraceIDs(t, 6, laneCount)
	p := newCentralQueueLogExporter(t, 500) // far smaller than the request below

	err := p.ConsumeLogs(t.Context(), centralQueueLaneLogs(reqIDs, 1000))
	require.ErrorIs(t, err, errCentralQueueRequestTooLarge)
	require.True(t, consumererror.IsPermanent(err), "a request larger than the queue can never succeed by retrying")
	require.Equal(t, 0, p.centralQueue.len())
	require.EqualValues(t, 0, p.centralQueue.compressedBytes())
}

// TestConsumeMetricsCentralQueue_NoPartialCommitOnQueueFull is the metrics-side
// SAW-10951 regression: a multi-routing-key metrics request that overflows the
// remaining capacity must leave the item count and compressed-byte count
// unchanged (all-or-nothing admission).
func TestConsumeMetricsCentralQueue_NoPartialCommitOnQueueFull(t *testing.T) {
	services := make([]string, 0, 6)
	for i := range 6 {
		services = append(services, fmt.Sprintf("svc-%02d", i))
	}
	request := func() pmetric.Metrics { return centralQueueServiceMetrics(services, 500) }

	measure := newCentralQueueMetricExporter(t, 1<<20)
	require.NoError(t, measure.ConsumeMetrics(t.Context(), request()))
	require.Equal(t, len(services), measure.centralQueue.len(), "each service item must fit on its own")
	requestBytes := measure.centralQueue.compressedBytes()
	require.Positive(t, requestBytes)

	p := newCentralQueueMetricExporter(t, requestBytes+100)
	require.NoError(t, p.ConsumeMetrics(t.Context(), centralQueueServiceMetrics([]string{"filler"}, 150)))
	preLen := p.centralQueue.len()
	preBytes := p.centralQueue.compressedBytes()
	require.Positive(t, preBytes)

	err := p.ConsumeMetrics(t.Context(), request())
	require.ErrorIs(t, err, errCentralQueueFull, "the full metrics request no longer fits the remaining capacity")
	require.Equal(t, preLen, p.centralQueue.len(), "on overflow NOTHING from the request is committed (SAW-10951)")
	require.Equal(t, preBytes, p.centralQueue.compressedBytes(), "queue byte accounting must be unchanged")
}
