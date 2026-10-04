// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackendFailedOutcomeRecordsMeasuredZeroAndClassifiedFailures(t *testing.T) {
	_, telemetryBuilder, reader := getTelemetryAssetsWithReader(t)
	endpoint := attribute.NewSet(attribute.String("signal", "logs"))

	recordBackendFailedOutcome(context.Background(), telemetryBuilder, endpoint, nil)
	recordBackendFailedOutcome(context.Background(), telemetryBuilder, endpoint, status.Error(codes.Unavailable, "backend unavailable"))

	metric, err := reader.GetMetric("otelcol_loadbalancer_backend_failed")
	require.NoError(t, err)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 2)

	want := map[string]int64{"success": 0, "unavailable": 1}
	for _, point := range sum.DataPoints {
		reason, found := point.Attributes.Value("reason")
		require.True(t, found)
		require.Equal(t, want[reason.AsString()], point.Value)
	}
}

func TestBackendFailedOutcomeDoesNotCreateUnboundedReasons(t *testing.T) {
	_, telemetryBuilder, reader := getTelemetryAssetsWithReader(t)
	endpoint := attribute.NewSet(attribute.String("signal", "logs"))

	recordBackendFailedOutcome(context.Background(), telemetryBuilder, endpoint, errors.New("secret customer payload"))

	metric, err := reader.GetMetric("otelcol_loadbalancer_backend_failed")
	require.NoError(t, err)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	reason, found := sum.DataPoints[0].Attributes.Value("reason")
	require.True(t, found)
	require.Equal(t, "other", reason.AsString())
}

func TestLogBackendFailedOutcomeCountsAttemptsOnceAndKeepsCleanZero(t *testing.T) {
	backendErr := status.Error(codes.Unavailable, "backend unavailable")
	e, backend, reader := newFailureCoverageBackend(t, func(context.Context, plog.Logs) error {
		return backendErr
	})

	backendErr = nil
	_, err := e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.NoError(t, err)
	backendErr = status.Error(codes.Unavailable, "backend unavailable")
	_, err = e.consumeBatchWithDecision(t.Context(), backend, simpleLogs(), logFlushReasonDirect, false, false, false)
	require.ErrorIs(t, err, backendErr)

	metric, err := reader.GetMetric("otelcol_loadbalancer_backend_failed")
	require.NoError(t, err)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	values := make(map[string]int64, len(sum.DataPoints))
	for _, point := range sum.DataPoints {
		reason, found := point.Attributes.Value("reason")
		require.True(t, found)
		values[reason.AsString()] = point.Value
	}
	require.Equal(t, int64(0), values["success"])
	require.Equal(t, int64(1), values["unavailable"])
}
