// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// multiLaneLogsForTest builds n log records, each with a distinct (non-empty)
// trace id -> distinct routing key -> distinct central-queue lane/item, with
// poorly-compressible bodies so per-item byte accounting is predictable.
func multiLaneLogsForTest(n, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for i := range n {
		lr := sl.LogRecords().AppendEmpty()
		b := make([]byte, bodyLen)
		for k := range b {
			b[k] = byte((i*131 + k*17 + k*k) % 251) // high-entropy, resists zstd
		}
		lr.Body().SetEmptyBytes().FromRaw(b)
		var tid [16]byte
		tid[0] = byte(i + 1)
		tid[15] = byte(i*7 + 1)
		lr.SetTraceID(tid)
	}
	return ld
}

// TestConsumeLogsCentralQueue_NoPartialCommitOnFull is the SAW-10951 regression:
// when a multi-lane request overflows the central queue, ConsumeLogs must commit
// EITHER all items OR none — never a partial subset. A partial commit means the
// caller returns an error (HTTP 500) to the client while some records were
// actually accepted and will be delivered; the client's retry then re-delivers
// them -> duplicates downstream. Fails before the all-or-nothing fix (some items
// commit), passes after it (nothing commits on overflow).
func TestConsumeLogsCentralQueue_NoPartialCommitOnFull(t *testing.T) {
	codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
	t.Cleanup(func() { require.NoError(t, codec.Close()) })

	p := &logExporterImp{
		centralQueue: newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           1500, // holds ~1-2 of the 6 lanes below
			maxInflightUncompressedBytes: 1 << 20,
			maxUncompressedBatchBytes:    1 << 20,
		}),
		centralCodec:          codec,
		centralQueueLaneCount: 64,
		randomTraceID:         func() pcommon.TraceID { return pcommon.TraceID{1} },
	}
	p.started.Store(true)

	// 6 lanes, ~1KB each of high-entropy bytes -> total far exceeds 1500 bytes.
	err := p.ConsumeLogs(t.Context(), multiLaneLogsForTest(6, 1000))

	require.Error(t, err, "a request that overflows the central queue must return an error to the caller")
	require.Equal(t, 0, p.centralQueue.len(),
		"on overflow NOTHING must be committed; a partial commit + client retry causes duplicate delivery (SAW-10951)")
	require.EqualValues(t, 0, p.centralQueue.compressedBytes(),
		"queue byte accounting must be zero when nothing was committed")
}
