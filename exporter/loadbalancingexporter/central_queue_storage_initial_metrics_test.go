// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestDurableCentralQueuePublishesEmptyStoreMetricsBeforeIntake(t *testing.T) {
	for _, signal := range []string{"logs", "metrics"} {
		t.Run(signal, func(t *testing.T) {
			exp, store, reader := newDurableSignalTestExporter(t, t.TempDir(), []string{"127.0.0.1:1"}, signal)
			t.Cleanup(func() {
				require.NoError(t, exp.Shutdown(context.WithoutCancel(t.Context())))
				require.NoError(t, store.Shutdown(context.WithoutCancel(t.Context())))
			})
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &data))
			expected := map[string]int64{
				"otelcol_loadbalancer_central_queue_durable_pending_items":           0,
				"otelcol_loadbalancer_central_queue_durable_pending_bytes":           0,
				"otelcol_loadbalancer_central_queue_durable_oldest_enqueued_time":    0,
				"otelcol_loadbalancer_central_queue_durable_terminal_rejected_items": 0,
			}
			observed := make(map[string]int64)
			for _, scope := range data.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if _, relevant := expected[metric.Name]; !relevant {
						continue
					}
					gauge, ok := metric.Data.(metricdata.Gauge[int64])
					require.True(t, ok)
					require.Len(t, gauge.DataPoints, 1)
					observed[metric.Name] = gauge.DataPoints[0].Value
				}
			}
			require.Equal(t, expected, observed, "an empty store must publish zero values before its first admission")
		})
	}
}
