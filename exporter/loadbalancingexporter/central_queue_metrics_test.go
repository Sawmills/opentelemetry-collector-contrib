// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestMetricCentralQueueCoalescesByLaneBeforeEndpointSelection(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := centralQueueMetricsConfig()
	ring := newHashRing(cfg.Resolver.Static.Get().Hostnames)
	serviceForEndpoint1 := findRoutingIDForEndpoint(t, ring, "endpoint-1:4317")
	serviceForEndpoint2 := findRoutingIDForEndpoint(t, ring, "endpoint-2:4317")
	require.NotEqual(t, serviceForEndpoint1, serviceForEndpoint2)

	sink := new(consumertest.MetricsSink)
	p, _ := newTestMetricsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockMetricsExporter(sink.ConsumeMetrics), nil
	})

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeMetrics(t.Context(), metricWithServiceName("first", serviceForEndpoint1)))
	require.NoError(t, p.ConsumeMetrics(t.Context(), metricWithServiceName("second", serviceForEndpoint2)))

	require.Eventually(t, func() bool {
		allMetrics := sink.AllMetrics()
		return len(allMetrics) == 1 && allMetrics[0].DataPointCount() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestMetricCentralQueueRequeuesWindowAfterSendFailure(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := centralQueueMetricsConfig()

	var calls atomic.Int64
	sink := new(consumertest.MetricsSink)
	p, _ := newTestMetricsExporter(t, ts, tb, cfg, func(context.Context, string) (component.Component, error) {
		return newMockMetricsExporter(func(ctx context.Context, md pmetric.Metrics) error {
			if calls.Add(1) == 1 {
				return errors.New("temporary backend failure")
			}
			return sink.ConsumeMetrics(ctx, md)
		}), nil
	})

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.WithoutCancel(t.Context())))
	})

	require.NoError(t, p.ConsumeMetrics(t.Context(), metricWithServiceName("retry-me", serviceName1)))

	require.Eventually(t, func() bool {
		allMetrics := sink.AllMetrics()
		return calls.Load() >= 2 && len(allMetrics) == 1 && allMetrics[0].DataPointCount() == 1
	}, time.Second, 10*time.Millisecond)
}

func centralQueueMetricsConfig() *Config {
	cfg := endpoint2Config()
	cfg.CentralQueue.Enabled = true
	cfg.CentralQueue.PayloadCompression = QueuePayloadCompressionZstd
	cfg.CentralQueue.CapacityBytes = 1 << 20
	cfg.CentralQueue.NumConsumers = 1
	cfg.CentralQueue.RequestBatching.TargetCompressedBytes = 1 << 20
	cfg.CentralQueue.RequestBatching.MaxCompressedBytes = 2 << 20
	cfg.CentralQueue.RequestBatching.MaxUncompressedBytes = 4 << 20
	cfg.CentralQueue.RequestBatching.MaxMergedItems = 1000
	cfg.CentralQueue.RequestBatching.MaxDelay = 25 * time.Millisecond
	cfg.CentralQueue.RequestBatching.LaneCount = 1
	return cfg
}

func metricWithServiceName(metricName, serviceName string) pmetric.Metrics {
	md := singleDataPointMetric(metricName)
	md.ResourceMetrics().At(0).Resource().Attributes().PutStr(serviceNameKey, serviceName)
	return md
}

func newTestMetricsExporter(
	t *testing.T,
	ts exporter.Settings,
	tb *metadata.TelemetryBuilder,
	cfg *Config,
	componentFactory func(context.Context, string) (component.Component, error),
) (*metricExporterImp, *loadBalancer) {
	t.Helper()

	lb, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)
	lb.addMissingExporters(t.Context(), cfg.Resolver.Static.Get().Hostnames)
	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(context.Context) ([]string, error) {
			return cfg.Resolver.Static.Get().Hostnames, nil
		},
	}

	p, err := newMetricsExporter(ts, cfg)
	require.NoError(t, err)
	p.loadBalancer = lb
	return p, lb
}
