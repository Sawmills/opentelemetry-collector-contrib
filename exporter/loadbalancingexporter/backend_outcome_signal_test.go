// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLogBackendOutcomeIdentifiesSignal(t *testing.T) {
	settings, tb, reader := getTelemetryAssetsWithReader(t)
	exp, _ := newTestLogsExporter(t, settings, tb, serviceBasedRoutingConfig(), func(context.Context, string) (component.Component, error) {
		return newMockLogsExporter(func(context.Context, plog.Logs) error { return nil }), nil
	})
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
	require.NoError(t, exp.ConsumeLogs(t.Context(), simpleLogs()))
	metric, err := reader.GetMetric("otelcol_loadbalancer_backend_outcome")
	require.NoError(t, err)
	points := metric.Data.(metricdata.Sum[int64]).DataPoints
	require.NotEmpty(t, points)
	for _, point := range points {
		signal, found := point.Attributes.Value("signal")
		require.True(t, found, "readiness must distinguish logs from other signal successes")
		require.Equal(t, "logs", signal.AsString())
	}
}
