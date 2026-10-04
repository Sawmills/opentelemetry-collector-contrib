// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func startConcurrentLogCalls(ctx context.Context, t *testing.T, calls int, result error) (*componenttest.Telemetry, func()) {
	t.Helper()
	started := make(chan struct{}, calls)
	release := make(chan struct{})
	results := make(chan error, calls)
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error {
		started <- struct{}{}
		<-release
		return result
	})
	var once sync.Once
	finish := func() {
		once.Do(func() {
			close(release)
			for range calls {
				require.ErrorIs(t, <-results, result)
			}
		})
	}
	t.Cleanup(finish)
	for range calls {
		go func() {
			_, err := e.consumeBatchWithDecision(ctx, backend, simpleLogs(), logFlushReasonDirect, false, false, false)
			results <- err
		}()
	}
	for range calls {
		<-started
	}
	return reader, finish
}

func requireActiveLogCalls(t *testing.T, reader *componenttest.Telemetry, want int64) {
	t.Helper()
	m, err := reader.GetMetric("otelcol_loadbalancer_backend_log_requests_in_flight")
	require.NoError(t, err)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.False(t, sum.IsMonotonic)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, attribute.NewSet(attribute.String("endpoint", "endpoint-1:4317")), sum.DataPoints[0].Attributes)
	require.Equal(t, want, sum.DataPoints[0].Value)
}

func TestBackendLogConcurrencyCountsOverlappingCalls(t *testing.T) {
	reader, finish := startConcurrentLogCalls(t.Context(), t, 2, nil)
	requireActiveLogCalls(t, reader, 2)
	finish()
	requireActiveLogCalls(t, reader, 0)
}

func TestBackendLogConcurrencyReturnsToZeroAfterFailures(t *testing.T) {
	reader, finish := startConcurrentLogCalls(t.Context(), t, 2, errors.New("backend unavailable"))
	requireActiveLogCalls(t, reader, 2)
	finish()
	requireActiveLogCalls(t, reader, 0)
}

func TestBackendLogConcurrencyDoesNotCountStoppedExporter(t *testing.T) {
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error { return nil })
	require.NoError(t, backend.Shutdown(t.Context()))
	_, err := e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, errLogBatcherExporterStopping)
	_, err = reader.GetMetric("otelcol_loadbalancer_backend_log_requests_in_flight")
	require.Error(t, err)
}

func TestBackendLogConcurrencyReturnsToZeroWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader, finish := startConcurrentLogCalls(ctx, t, 1, context.Canceled)
	requireActiveLogCalls(t, reader, 1)
	finish()
	requireActiveLogCalls(t, reader, 0)
}
