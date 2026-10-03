// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestCentralQueuePreflightKeepsRecordSizesSeparate(t *testing.T) {
	first := sharedResourceScopeLog("small")
	first.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().PutStr("large", strings.Repeat("x", 1024))
	second := sharedResourceScopeLog(strings.Repeat("y", 1024))
	limit := max(mustMarshalLogsSize(t, first), mustMarshalLogsSize(t, second))
	second.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).CopyTo(first.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty())
	marshaler := &plog.ProtoMarshaler{}
	before, err := marshaler.MarshalLogs(first)
	require.NoError(t, err)
	exporter := newPreflightTestExporter(t, limit)

	require.NoError(t, exporter.ConsumeLogs(t.Context(), first))

	require.Equal(t, 2, exporter.centralQueue.len())
	after, err := marshaler.MarshalLogs(first)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestCentralQueuePreflightPreservesWireSizeBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 120, 127, 128, 255, 16370, 16383, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			logs := sharedResourceScopeLog(strings.Repeat("x", size))
			rl := logs.ResourceLogs().At(0)
			rl.SetSchemaUrl(strings.Repeat("r", size))
			rl.Resource().SetDroppedAttributesCount(123)
			sl := rl.ScopeLogs().At(0)
			sl.SetSchemaUrl(strings.Repeat("s", size))
			sl.Scope().SetDroppedAttributesCount(456)
			sl.Scope().SetName(strings.Repeat("scope", size/5))
			sl.LogRecords().At(0).Attributes().PutEmptySlice("nested").AppendEmpty().SetEmptyMap().PutStr("value", "escaped\n☺")
			limit := mustMarshalLogsSize(t, logs)
			exporter := newPreflightTestExporter(t, limit)
			splitter := newCentralQueueLogSplitter(exporter, limit, time.Time{})

			require.NoError(t, splitter.rejectUnsplittableRecords(t.Context(), logs))
			splitter.hardLimit = limit - 1
			require.ErrorIs(t, splitter.rejectUnsplittableRecords(t.Context(), logs), errCentralQueueItemTooLarge)
			require.Equal(t, limit, mustMarshalLogsSize(t, logs))
		})
	}
}

func TestCentralQueuePreflightEmptyMetadataAndRecord(t *testing.T) {
	logs := plog.NewLogs()
	logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	limit := mustMarshalLogsSize(t, logs)
	exporter := newPreflightTestExporter(t, limit)
	splitter := newCentralQueueLogSplitter(exporter, limit, time.Time{})

	require.NoError(t, splitter.rejectUnsplittableRecords(t.Context(), logs))
	splitter.hardLimit = limit - 1
	require.ErrorIs(t, splitter.rejectUnsplittableRecords(t.Context(), logs), errCentralQueueItemTooLarge)
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
