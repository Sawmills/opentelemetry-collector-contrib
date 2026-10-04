// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newFailureCoverageQueue(t *testing.T) (*centralQueue, *componenttest.Telemetry) {
	t.Helper()
	_, _, reader := getTelemetryAssetsWithReader(t)
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	return newCentralQueue(centralQueueSettings{maxCompressedBytes: 10, telemetry: telemetry}), reader
}

func TestCentralQueueSuccessExposesZeroRejectedBytes(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	require.NoError(t, q.enqueue(centralQueueItem{compressedBytes: 4, count: 1}))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", "By", attribute.NewSet(attribute.String("signal", "logs")), 0)
}

func TestCentralQueueAtomicSuccessExposesZeroRejectedBytes(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	require.NoError(t, q.enqueueAll([]centralQueueItem{{compressedBytes: 4, count: 1}, {compressedBytes: 3, count: 1}}))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", "By", attribute.NewSet(attribute.String("signal", "logs")), 0)
}

func TestCentralQueueEmptyAdmissionDoesNotInventCoverage(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	require.NoError(t, q.enqueueAll(nil))
	require.NoError(t, q.enqueue(centralQueueItem{}))
	_, err := reader.GetMetric("otelcol_loadbalancer_central_queue_rejected_compressed_bytes")
	require.Error(t, err)
}

func TestCentralQueueSuccessPreservesRejectedByteCount(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	require.NoError(t, q.enqueue(centralQueueItem{compressedBytes: 4, count: 1}))
	require.ErrorIs(t, q.enqueueAll([]centralQueueItem{{compressedBytes: 5, count: 1}, {compressedBytes: 3, count: 1}}), errCentralQueueFull)
	require.NoError(t, q.enqueue(centralQueueItem{compressedBytes: 1, count: 1}))
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", "By", attribute.NewSet(attribute.String("signal", "logs")), 8)
}

func TestCentralQueueStoppedAdmissionCountsRejectedBytes(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	q.stop()
	require.ErrorIs(t, q.enqueue(centralQueueItem{compressedBytes: 4, count: 1}), errCentralQueueStopped)
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", "By", attribute.NewSet(attribute.String("signal", "logs")), 4)
}

func TestCentralQueueStoppedAtomicAdmissionCountsRejectedBytes(t *testing.T) {
	q, reader := newFailureCoverageQueue(t)
	q.stop()
	require.ErrorIs(t, q.enqueueAll([]centralQueueItem{{compressedBytes: 4, count: 1}, {compressedBytes: 3, count: 1}}), errCentralQueueStopped)
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", "By", attribute.NewSet(attribute.String("signal", "logs")), 7)
}

func newFailureCoverageBackend(t *testing.T, consume func(context.Context, plog.Logs) error) (*logExporterImp, *wrappedExporter, *componenttest.Telemetry) {
	t.Helper()
	settings, tb, reader := getTelemetryAssetsWithReader(t)
	return &logExporterImp{telemetry: tb, logger: settings.Logger}, newWrappedExporter(newMockLogsExporter(consume), "endpoint-1:4317"), reader
}

func requireBackendFailureCount(t *testing.T, reader *componenttest.Telemetry, endpoint string, want int64) {
	t.Helper()
	m, err := reader.GetMetric("otelcol_loadbalancer_backend_outcome")
	require.NoError(t, err)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	for _, dp := range sum.DataPoints {
		success, hasSuccess := dp.Attributes.Value("success")
		ep, _ := dp.Attributes.Value("endpoint")
		if hasSuccess && !success.AsBool() && ep.AsString() == endpoint {
			require.Equal(t, want, dp.Value)
			return
		}
	}
	t.Fatal("missing failed backend outcome")
}

func TestLogBackendSuccessExposesZeroFailuresBeforeMutation(t *testing.T) {
	e, backend, reader := newFailureCoverageBackend(t, func(_ context.Context, logs plog.Logs) error {
		logs.ResourceLogs().RemoveIf(func(plog.ResourceLogs) bool { return true })
		return nil
	})
	_, err := e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	requireBackendFailureCount(t, reader, "endpoint-1:4317", 0)
}

func TestLogBackendSuccessPreservesFailures(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return backendErr })
	_, err := e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, backendErr)
	backendErr = nil
	_, err = e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	requireBackendFailureCount(t, reader, "endpoint-1:4317", 1)
}

func TestLogBackendIdleDoesNotInventCoverage(t *testing.T) {
	_, _, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return nil })
	_, err := reader.GetMetric("otelcol_loadbalancer_backend_outcome")
	require.Error(t, err)
}

func TestLogBackendEmptySuccessDoesNotInventFailureCoverage(t *testing.T) {
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return nil })
	_, err := e.consumeBatchWithDecision(t.Context(), backend, plog.NewLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	m, err := reader.GetMetric("otelcol_loadbalancer_backend_outcome")
	require.NoError(t, err)
	require.Len(t, m.Data.(metricdata.Sum[int64]).DataPoints, 1)
}

func TestLogBackendSuccessThenRepeatedFailuresCountsAttempts(t *testing.T) {
	var backendErr error
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return backendErr })
	_, err := e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	backendErr = errors.New("backend unavailable")
	_, err = e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, backendErr)
	_, err = e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, backendErr)
	requireBackendFailureCount(t, reader, "endpoint-1:4317", 2)
}

func TestLogBackendReplacementHasIndependentFailureCoverage(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	e, oldBackend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return backendErr })
	_, err := e.consumeBatchWithDecision(t.Context(), oldBackend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, backendErr)
	replacement := newWrappedExporter(newMockLogsExporter(func(context.Context, plog.Logs) error { return nil }), "endpoint-2:4317")
	_, err = e.consumeBatchWithDecision(t.Context(), replacement, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	requireBackendFailureCount(t, reader, "endpoint-1:4317", 1)
	requireBackendFailureCount(t, reader, "endpoint-2:4317", 0)
}
