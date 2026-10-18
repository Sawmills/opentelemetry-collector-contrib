// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
)

func TestConsumeLogsCentralQueueAdmitsOversizedSingletonBelowRecordCap(t *testing.T) {
	logs := sharedResourceScopeLog(strings.Repeat("x", 1024))
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.NoError(t, err)
	decoded, decodeErr := decodeCentralQueueLogsItem(exporter.centralQueue.queuedItems()[0], exporter.centralCodec)
	require.NoError(t, decodeErr)
	require.NoError(t, plogtest.CompareLogs(logs, decoded))
}

func TestConsumeLogsCentralQueueUsesConfiguredRecordCap(t *testing.T) {
	cfg := simpleConfig()
	cfg.CentralQueue = CentralQueueConfig{
		Enabled: true, PayloadCompression: QueuePayloadCompressionZstd,
		MaxCompressedBytes: 1 << 20, MaxInflightUncompressedBytes: 1 << 20,
		MaxUncompressedBatchBytes: 256, MaxUncompressedLogRecordBytes: 2048,
	}
	exporter, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, exporter.centralCodec.Close()) })
	exporter.started.Store(true)

	err = exporter.ConsumeLogs(t.Context(), sharedResourceScopeLog(strings.Repeat("x", 1024)))

	require.NoError(t, err)
}

func TestConsumeLogsCentralQueueRejectsOnlyRecordsAboveRecordCap(t *testing.T) {
	logs := sharedScopeLogsWithoutTraceIDs("small-before", strings.Repeat("x", 4096), "small-after")
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.True(t, consumererror.IsPermanent(err))
	require.Equal(t, 2, exporter.centralQueue.queuedItems()[0].count)
	require.Equal(t, 3, logs.LogRecordCount())
}

func TestConsumeLogsCentralQueueHonorsRecordCapBelowBatchCap(t *testing.T) {
	exporter := newPreflightTestExporter(t, 2048)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 256

	err := exporter.ConsumeLogs(t.Context(), sharedResourceScopeLog(strings.Repeat("x", 1024)))

	require.True(t, consumererror.IsPermanent(err))
	require.Zero(t, exporter.centralQueue.len())
}

func TestConsumeLogsCentralQueuePartialRejectionAvoidsDuplicatePayloadCopies(t *testing.T) {
	const requests = 32
	logs := sharedScopeLogsWithoutTraceIDs("small", "")
	values := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(1).Body().SetEmptySlice()
	values.EnsureCapacity(8192)
	for range 8192 {
		values.AppendEmpty().SetStr("synthetic")
	}
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048
	// Initialize compression before measuring allocation at the public seam.
	require.True(t, consumererror.IsPermanent(exporter.ConsumeLogs(t.Context(), logs)))
	allocationPerRequest := func(operation func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range requests {
			operation()
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / requests
	}
	copyBytes := allocationPerRequest(func() {
		copied := plog.NewLogs()
		logs.CopyTo(copied)
		require.Equal(t, 2, copied.LogRecordCount())
	})
	allocated := allocationPerRequest(func() {
		require.True(t, consumererror.IsPermanent(exporter.ConsumeLogs(t.Context(), logs)))
	})
	t.Logf("one pdata copy %d bytes; partial rejection %d bytes per request", copyBytes, allocated)
	// Allow one owned partial-error copy and bounded pipeline overhead, but
	// not a second full value-tree copy in the temporary accepted request.
	require.Less(t, allocated, copyBytes*3/2+(64<<10))
	require.Equal(t, 2, logs.LogRecordCount())
	require.Equal(t, 8192, values.Len())
}

func TestConsumeLogsCentralQueuePartialRejectionPreservesScopeRouting(t *testing.T) {
	logs := sharedScopeLogsWithoutTraceIDs("small-before", strings.Repeat("x", 4096))
	logs.ResourceLogs().At(0).CopyTo(logs.ResourceLogs().AppendEmpty())
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048
	exporter.ignoreTraceID = false
	exporter.centralQueueLaneCount = 0
	var sequence byte
	exporter.randomTraceID = func() pcommon.TraceID {
		sequence++
		return pcommon.TraceID{sequence}
	}

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.True(t, consumererror.IsPermanent(err))
	// Equal metadata does not combine distinct input scopes into one fallback route.
	require.Len(t, exporter.centralQueue.queuedItems(), 2)
	for _, item := range exporter.centralQueue.queuedItems() {
		require.Equal(t, 1, item.count)
	}
	require.Equal(t, 4, logs.LogRecordCount())
}

func TestConsumeLogsCentralQueueAdmitsRecordWithOversizedMetadata(t *testing.T) {
	logs := sharedResourceScopeLog("small-body")
	logs.ResourceLogs().At(0).Resource().Attributes().PutStr("synthetic", strings.Repeat("m", 1024))
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.NoError(t, err)
	require.Greater(t, exporter.centralQueue.queuedItems()[0].uncompressedBytes, 256)
}

func TestConsumeLogsCentralQueueDoesNotRecombineBeyondSmallerTransportCap(t *testing.T) {
	exporter := newPreflightTestExporter(t, 2048)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 256
	logs := sharedScopeLogsWithoutTraceIDs(strings.Repeat("x", 160), strings.Repeat("y", 160))
	require.NoError(t, exporter.ConsumeLogs(t.Context(), logs))

	lease, err := exporter.centralQueue.tryLease(time.Now())

	require.NoError(t, err)
	require.Equal(t, 1, lease.window.count)
	require.LessOrEqual(t, lease.window.uncompressedBytes, 256)
	lease.done()
}

func TestConsumeLogsCentralQueueKeepsOversizedRecordsWithinSharedInflightBudget(t *testing.T) {
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048
	exporter.centralQueue.settings.maxInflightUncompressedBytes = 1800
	logs := sharedScopeLogsWithoutTraceIDs(strings.Repeat("x", 1024), strings.Repeat("y", 1024))
	require.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
	first, err := exporter.centralQueue.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := exporter.centralQueue.tryLease(time.Now())

	require.Nil(t, second)
	require.ErrorIs(t, err, errCentralQueueInflightFull)
	first.done()
	second, err = exporter.centralQueue.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 1, second.window.count)
	second.done()
}

func TestConsumeLogsCentralQueueReportsExplicitZeroSizeRefusals(t *testing.T) {
	_, reader, _ := newSizeAdmissionTelemetryExporter(t)

	metric, err := reader.GetMetric("otelcol_loadbalancer_central_queue_log_record_admission_total")

	require.NoError(t, err)
	require.Equal(t, map[string]int64{"admitted": 0, "rejected": 0}, sizeAdmissionCounts(metric))
}

func TestConsumeLogsCentralQueueCountsAndWarnsOnlyRejectedRecords(t *testing.T) {
	exporter, reader, warnings := newSizeAdmissionTelemetryExporter(t)
	large := strings.Repeat("x", 4096)
	logs := sharedScopeLogsWithoutTraceIDs("small", large)
	expectedSize := int64(mustMarshalLogsSize(t, sharedScopeLogsWithoutTraceIDs(large)))

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.True(t, consumererror.IsPermanent(err))
	metric, metricErr := reader.GetMetric("otelcol_loadbalancer_central_queue_log_record_admission_total")
	require.NoError(t, metricErr)
	require.Equal(t, map[string]int64{"admitted": 0, "rejected": 1}, sizeAdmissionCounts(metric))
	requireCentralQueueIntHistogram(t, reader,
		"otelcol_loadbalancer_central_queue_oversized_log_record_uncompressed_bytes", "By",
		attribute.NewSet(attribute.String("signal", "logs"), attribute.String("outcome", "rejected")), expectedSize)
	require.Len(t, warnings.All(), 1)
	require.Equal(t, map[string]any{"rejected_log_records": int64(1), "max_uncompressed_log_record_bytes": int64(2048)}, warnings.All()[0].ContextMap())
}

func TestConsumeLogsCentralQueueDoesNotCountPartialRejectionsWhenAdmissionFails(t *testing.T) {
	exporter, reader, warnings := newSizeAdmissionTelemetryExporter(t)
	exporter.centralQueue.settings.maxCompressedBytes = 1
	logs := sharedScopeLogsWithoutTraceIDs("small", strings.Repeat("x", 4096))

	err := exporter.ConsumeLogs(t.Context(), logs)

	require.ErrorIs(t, err, errCentralQueueRequestTooLarge)
	require.Zero(t, exporter.centralQueue.len())
	metric, metricErr := reader.GetMetric("otelcol_loadbalancer_central_queue_log_record_admission_total")
	require.NoError(t, metricErr)
	require.Equal(t, map[string]int64{"admitted": 0, "rejected": 0}, sizeAdmissionCounts(metric))
	require.Empty(t, warnings.All())
}

func newSizeAdmissionTelemetryExporter(t *testing.T) (*logExporterImp, *componenttest.Telemetry, *observer.ObservedLogs) {
	t.Helper()
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context()))) })
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	exporter := newPreflightTestExporter(t, 256)
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048
	exporter.centralQueue.settings.telemetry = telemetry
	core, warnings := observer.New(zap.WarnLevel)
	exporter.logger = zap.New(core)
	return exporter, reader, warnings
}

func sizeAdmissionCounts(metric metricdata.Metrics) map[string]int64 {
	counts := make(map[string]int64)
	for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
		outcome, _ := point.Attributes.Value("outcome")
		counts[outcome.AsString()] = point.Value
	}
	return counts
}

func TestConsumeLogsCentralQueueReservesBudgetForAgedOversizedSingleton(t *testing.T) {
	testAgedOversizedSingletonProgress(t, nil, 0)
}

func TestConsumeLogsCentralQueueReservesBudgetWithAdaptiveConsumers(t *testing.T) {
	testAgedOversizedSingletonProgress(t, func(int64, centralQueueWindow) (func(), bool) { return nil, true }, 0)
}

func TestConsumeLogsCentralQueueReservesBudgetWithBatchDelay(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		testAgedOversizedSingletonProgress(t, nil, 100*time.Millisecond)
	})
	t.Run("adaptive", func(t *testing.T) {
		testAgedOversizedSingletonProgress(t, func(int64, centralQueueWindow) (func(), bool) { return nil, true }, 100*time.Millisecond)
	})
}

func TestConsumeLogsCentralQueueDoesNotReserveAheadOfOlderSmallWindow(t *testing.T) {
	exporter := newSizeAdmissionReservationExporter(t)
	consumeSizeAdmissionRecord(t, exporter, 1, 1024)
	primer, err := exporter.centralQueue.tryLease(time.Now())
	require.NoError(t, err)
	consumeSizeAdmissionRecord(t, exporter, 2, 64)
	consumeSizeAdmissionRecord(t, exporter, 3, 1024)

	olderSmall, err := exporter.centralQueue.tryLease(time.Now().Add(2 * time.Second))

	require.NoError(t, err)
	require.NotNil(t, olderSmall)
	require.LessOrEqual(t, olderSmall.window.uncompressedBytes, 256)
	olderSmall.done()
	primer.done()
}

func testAgedOversizedSingletonProgress(t *testing.T, acquire centralQueueLeaseAcquireFunc, batchDelay time.Duration) {
	t.Helper()
	exporter := newSizeAdmissionReservationExporter(t)
	exporter.centralQueue.settings.maxBatchDelay = batchDelay
	consumeSizeAdmissionRecord(t, exporter, 1, 1024)
	primer, err := exporter.centralQueue.tryLeaseWithAcquire(time.Now().Add(batchDelay), acquire)
	require.NoError(t, err)
	require.NotNil(t, primer)
	consumeSizeAdmissionRecord(t, exporter, 2, 1024)
	aged := time.Now().Add(2 * time.Second)
	for id := byte(3); id < 6; id++ {
		consumeSizeAdmissionRecord(t, exporter, id, 64)
		if batchDelay > 0 {
			// Reaching the batch cap makes fresh traffic immediately ready,
			// while the compressible singleton waits for its batch delay.
			consumeSizeAdmissionRecord(t, exporter, id, 64)
		}
		// New arrivals behind the singleton must not reset its age priority.
		consumeSizeAdmissionRecord(t, exporter, 2, 64)
		lease, leaseErr := exporter.centralQueue.tryLeaseWithAcquire(aged, acquire)
		require.Nil(t, lease, "new small traffic must wait for the older oversized singleton")
		require.ErrorIs(t, leaseErr, errCentralQueueInflightFull)
		exporter.centralQueue.mu.Lock()
		snapshot := exporter.centralQueue.schedulerSnapshotLocked(aged)
		exporter.centralQueue.mu.Unlock()
		require.Equal(t, centralQueueSchedulerStateInflightBytes, snapshot.state)
	}
	primer.done()
	expectedKey := pcommon.TraceID{2}

	large, err := exporter.centralQueue.tryLeaseWithAcquire(aged, acquire)

	require.NoError(t, err)
	require.NotNil(t, large)
	require.Equal(t, expectedKey[:], large.window.routingKey)
	require.Greater(t, large.window.uncompressedBytes, 256)
	large.done()
}

func newSizeAdmissionReservationExporter(t *testing.T) *logExporterImp {
	t.Helper()
	exporter := newPreflightTestExporter(t, 256)
	exporter.ignoreTraceID = false
	exporter.centralQueueLaneCount = 0
	exporter.centralQueue.settings.maxUncompressedLogRecordBytes = 2048
	exporter.centralQueue.settings.maxInflightUncompressedBytes = 1800
	exporter.centralQueue.settings.forceScheduleAge = time.Second
	return exporter
}

func consumeSizeAdmissionRecord(t *testing.T, exporter *logExporterImp, id byte, size int) {
	t.Helper()
	logs := sharedScopeLogsWithoutTraceIDs(strings.Repeat("x", size))
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetTraceID(pcommon.TraceID{id})
	require.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}
