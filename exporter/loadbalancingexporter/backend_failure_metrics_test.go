// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackendFailedOutcomeRecordsMeasuredZeroAndClassifiedFailures(t *testing.T) {
	_, telemetryBuilder, reader := getTelemetryAssetsWithReader(t)
	endpoint := attribute.NewSet(attribute.String("signal", "logs"))

	recordBackendFailedOutcome(t.Context(), telemetryBuilder, endpoint, nil)
	recordBackendFailedOutcome(t.Context(), telemetryBuilder, endpoint, status.Error(codes.Unavailable, "backend unavailable"))

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

	recordBackendFailedOutcome(t.Context(), telemetryBuilder, endpoint, errors.New("secret customer payload"))

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

func TestMetricBackendEmptyOutcome(t *testing.T) {
	for _, path := range []string{"batch", "central-queue"} {
		for _, failed := range []bool{false, true} {
			name := "success"
			var backendErr error
			if failed {
				name = "failure"
				backendErr = status.Error(codes.Unavailable, "backend unavailable")
			}
			t.Run(path+"/"+name, func(t *testing.T) {
				settings, tb, reader := getTelemetryAssetsWithReader(t)
				calls := 0
				backend := newWrappedExporter(newMockMetricsExporter(func(_ context.Context, md pmetric.Metrics) error {
					calls++
					require.Zero(t, md.DataPointCount())
					return backendErr
				}), "endpoint-1:4317")
				e := &metricExporterImp{telemetry: tb, logger: settings.Logger}
				var err error
				if path == "batch" {
					err = e.consumeBatch(t.Context(), backend, pmetric.NewMetrics(), metricFlushReasonShutdown)
				} else {
					codec := newQueuePayloadCodec(QueuePayloadCompressionZstd)
					t.Cleanup(func() { require.NoError(t, codec.Close()) })
					e.centralCodec = codec
					e.loadBalancer = &loadBalancer{
						ring:           newHashRing([]string{backend.endpoint}),
						exporters:      map[string]*wrappedExporter{backend.endpoint: backend},
						endpointHealth: newEndpointHealthManager(endpointHealthSettings{}),
					}
					item, encodeErr := newCentralQueueMetricsItem([]byte("lane-a"), pmetric.NewMetrics(), codec, time.Now())
					require.NoError(t, encodeErr)
					err = e.consumeCentralQueueMetricWindow(t.Context(), centralQueueWindow{routingKey: []byte("lane-a"), items: []centralQueueItem{item}})
				}
				require.ErrorIs(t, err, backendErr)
				require.Equal(t, 1, calls)
				metric, err := reader.GetMetric("otelcol_loadbalancer_backend_failed")
				if !failed {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				points := metric.Data.(metricdata.Sum[int64]).DataPoints
				require.Len(t, points, 1)
				require.Equal(t, int64(1), points[0].Value)
				require.Equal(t, attribute.NewSet(attribute.String("signal", "metrics"), attribute.String("reason", "unavailable")), points[0].Attributes)
			})
		}
	}
}

func TestTraceBackendOutcomePreservesPreConsumeCount(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			name := "nonempty"
			if empty {
				name = "empty"
			}
			if failed {
				name += "/failure"
			} else {
				name += "/success"
			}
			t.Run(name, func(t *testing.T) {
				settings, tb, reader := getTelemetryAssetsWithReader(t)
				cfg := serviceBasedRoutingConfig()
				calls := 0
				var backendErr error
				if failed {
					backendErr = status.Error(codes.Unavailable, "backend unavailable")
				}
				factory := func(context.Context, string) (component.Component, error) {
					return newMockTracesExporter(func(_ context.Context, td ptrace.Traces) error {
						calls++
						require.Positive(t, td.SpanCount())
						td.ResourceSpans().RemoveIf(func(ptrace.ResourceSpans) bool { return true })
						return backendErr
					}), nil
				}
				lb, err := newLoadBalancer(settings.Logger, cfg, factory, tb)
				require.NoError(t, err)
				lb.ring = newHashRing([]string{"endpoint-1:4317"})
				lb.addMissingExporters(t.Context(), []string{"endpoint-1:4317"})
				e, err := newTracesExporter(settings, cfg)
				require.NoError(t, err)
				e.loadBalancer = lb
				input := ptrace.NewTraces()
				if !empty {
					input = tracesWithServiceNames("service-1")
				}
				err = e.ConsumeTraces(t.Context(), input)
				if empty {
					require.NoError(t, err)
					require.Zero(t, calls)
					_, err = reader.GetMetric("otelcol_loadbalancer_backend_failed")
					require.Error(t, err)
					return
				}
				require.ErrorIs(t, err, backendErr)
				require.Equal(t, 1, calls)
				metric, err := reader.GetMetric("otelcol_loadbalancer_backend_failed")
				require.NoError(t, err)
				points := metric.Data.(metricdata.Sum[int64]).DataPoints
				require.Len(t, points, 1)
				reason, count := "success", int64(0)
				if failed {
					reason, count = "unavailable", 1
				}
				require.Equal(t, count, points[0].Value)
				require.Equal(t, attribute.NewSet(attribute.String("signal", "traces"), attribute.String("reason", reason)), points[0].Attributes)
			})
		}
	}
}
