// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func getTelemetryAssets(t require.TestingT) (exporter.Settings, *metadata.TelemetryBuilder) {
	s := exportertest.NewNopSettings(metadata.Type)
	tb, err := metadata.NewTelemetryBuilder(s.TelemetrySettings)
	require.NoError(t, err)
	return s, tb
}

func getTelemetryAssetsWithReader(t *testing.T) (exporter.Settings, *metadata.TelemetryBuilder, *componenttest.Telemetry) {
	t.Helper()

	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, telemetry.Shutdown(context.WithoutCancel(t.Context())))
	})

	s := exportertest.NewNopSettings(metadata.Type)
	s.TelemetrySettings = telemetry.NewTelemetrySettings()
	tb, err := metadata.NewTelemetryBuilder(s.TelemetrySettings)
	require.NoError(t, err)
	return s, tb, telemetry
}

func assertInt64CounterDataPoint(
	t *testing.T,
	telemetry *componenttest.Telemetry,
	metricName string,
	attrs attribute.Set,
	value int64,
) {
	t.Helper()

	got, err := telemetry.GetMetric(metricName)
	require.NoError(t, err)
	sum, ok := got.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&attrs) {
			require.Equal(t, value, dp.Value)
			return
		}
	}
	require.Failf(t, "missing data point", "metric=%s attrs=%v", metricName, attrs)
}

func assertInt64HistogramDataPoint(
	t *testing.T,
	telemetry *componenttest.Telemetry,
	metricName string,
	attrs attribute.Set,
	sumValue int64,
) {
	t.Helper()

	got, err := telemetry.GetMetric(metricName)
	require.NoError(t, err)
	histogram, ok := got.Data.(metricdata.Histogram[int64])
	require.True(t, ok)
	for _, dp := range histogram.DataPoints {
		if dp.Attributes.Equals(&attrs) {
			require.Equal(t, uint64(1), dp.Count)
			require.Equal(t, sumValue, dp.Sum)
			return
		}
	}
	require.Failf(t, "missing data point", "metric=%s attrs=%v", metricName, attrs)
}

func assertInt64GaugeDataPoint(
	t *testing.T,
	telemetry *componenttest.Telemetry,
	metricName string,
	attrs attribute.Set,
	value int64,
) {
	t.Helper()

	got, err := telemetry.GetMetric(metricName)
	require.NoError(t, err)
	gauge, ok := got.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	for _, dp := range gauge.DataPoints {
		if dp.Attributes.Equals(&attrs) {
			require.Equal(t, value, dp.Value)
			return
		}
	}
	require.Failf(t, "missing data point", "metric=%s attrs=%v", metricName, attrs)
}
