// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestCentralQueuePreflightAcceptsExactSerializedLimit(t *testing.T) {
	logs := sharedResourceScopeLog(strings.Repeat("boundary☺", 32))
	limit := mustMarshalLogsSize(t, logs)
	exporter := newPreflightTestExporter(t, limit)

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.NoError(t, err)
	require.Equal(t, 1, exporter.centralQueue.len())
}

func TestCentralQueuePreflightRejectsOneByteAboveLimit(t *testing.T) {
	logs := sharedResourceScopeLog(strings.Repeat("boundary☺", 32))
	limit := mustMarshalLogsSize(t, logs) - 1
	exporter := newPreflightTestExporter(t, limit)

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.ErrorIs(t, err, errCentralQueueItemTooLarge)
	require.Zero(t, exporter.centralQueue.len())
}

func newPreflightTestExporter(tb testing.TB, limit int) *logExporterImp {
	tb.Helper()
	codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
	tb.Cleanup(func() { require.NoError(tb, codec.Close()) })
	exporter := &logExporterImp{
		centralQueue: newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           64 << 20,
			maxInflightUncompressedBytes: 64 << 20,
			maxUncompressedBatchBytes:    limit,
			targetCompressedBytes:        1 << 20,
		}),
		centralCodec:          codec,
		ignoreTraceID:         true,
		centralQueueLaneCount: 64,
		randomTraceID:         func() pcommon.TraceID { return pcommon.TraceID{7} },
	}
	exporter.started.Store(true)
	return exporter
}

func BenchmarkCentralQueueLogPreflight(b *testing.B) {
	for _, records := range []int{32, 256} {
		for _, bodySize := range []int{512, 2048} {
			b.Run(fmt.Sprintf("records=%d/body=%d", records, bodySize), func(b *testing.B) {
				logs := sharedScopeLogsWithoutTraceIDs(repeatedStrings(records, strings.Repeat("x", bodySize))...)
				exporter := newPreflightTestExporter(b, 1<<20)
				settings := exporter.centralQueue.settings
				b.ReportAllocs()
				b.SetBytes(int64((&plog.ProtoMarshaler{}).LogsSize(logs)))
				b.ResetTimer()
				for range b.N {
					exporter.centralQueue = newCentralQueue(settings)
					if err := exporter.ConsumeLogs(b.Context(), logs); err != nil {
						b.Fatal(err)
					}
					if exporter.centralQueue.len() != 1 {
						b.Fatal("expected one complete queued batch")
					}
				}
			})
		}
	}
}
