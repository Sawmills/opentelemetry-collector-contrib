// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Substitute a supported gRPC message which the acknowledgement reader cannot
// interpret. The real backend still accepts the original OTLP request.
type unsupportedAckReplyMiddleware struct{}

func (unsupportedAckReplyMiddleware) Start(context.Context, component.Host) error { return nil }
func (unsupportedAckReplyMiddleware) Shutdown(context.Context) error              { return nil }
func (unsupportedAckReplyMiddleware) GetGRPCClientOptions(context.Context) ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, _ any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(ctx, method, req, &emptypb.Empty{}, conn, opts...)
	})}, nil
}

func TestDurableCentralQueueRetainsLocalAcknowledgementFailure(t *testing.T) {
	for _, signal := range []string{"logs", "metrics"} {
		t.Run(signal, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			logsDestination := &durableTestDestination{}
			metricsDestination := &durableMetricDestination{}
			server := grpc.NewServer()
			plogotlp.RegisterGRPCServer(server, logsDestination)
			pmetricotlp.RegisterGRPCServer(server, metricsDestination)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			dir := t.TempDir()
			middlewareID := component.MustNewID("unsupported_ack_reply")
			exp, store, reader := newDurableSignalTestExporterWithMiddlewares(t, dir, []string{listener.Addr().String()}, signal, map[component.ID]component.Component{middlewareID: unsupportedAckReplyMiddleware{}})
			t.Cleanup(func() {
				_ = exp.Shutdown(context.WithoutCancel(t.Context()))
				_ = store.Shutdown(context.WithoutCancel(t.Context()))
			})
			if signal == "logs" {
				input := plog.NewLogs()
				records := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
				for _, id := range []int64{71, 72} {
					record := records.AppendEmpty()
					record.Attributes().PutInt("test.id", id)
					record.Body().SetStr("accepted before local acknowledgement error")
				}
				require.NoError(t, exp.(exporter.Logs).ConsumeLogs(t.Context(), input))
			} else {
				require.NoError(t, exp.(exporter.Metrics).ConsumeMetrics(t.Context(), durableMetricInput(71, 72)))
			}
			require.Eventually(t, func() bool {
				return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_storage_failures") == 1
			}, time.Second, 10*time.Millisecond)
			require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
			require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items"))
			require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_backend_failed"), "a local response error must not count as a failed destination attempt")
			attempts := func() int64 {
				if signal == "logs" {
					return logsDestination.attempts.Load()
				}
				return metricsDestination.attempts.Load()
			}
			require.Equal(t, int64(1), attempts())
			require.Never(t, func() bool { return attempts() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
			require.ErrorContains(t, exp.Shutdown(t.Context()), "unsupported durable central queue OTLP response")
			require.NoError(t, store.Shutdown(t.Context()))
			recovered, recoveredStore, recoveredReader := newDurableSignalTestExporter(t, dir, []string{listener.Addr().String()}, signal)
			require.Eventually(t, func() bool {
				return durableMetricValue(t, recoveredReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
			}, time.Second, 10*time.Millisecond)
			require.Zero(t, durableMetricValue(t, recoveredReader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items"))
			require.NoError(t, recovered.Shutdown(t.Context()))
			require.NoError(t, recoveredStore.Shutdown(t.Context()))
			var ids []int64
			if signal == "logs" {
				logsDestination.mu.Lock()
				ids = append(ids, logsDestination.ids...)
				logsDestination.mu.Unlock()
			} else {
				metricsDestination.mu.Lock()
				ids = append(ids, metricsDestination.ids...)
				metricsDestination.mu.Unlock()
			}
			require.ElementsMatch(t, []int64{71, 72, 71, 72}, ids, "uncertain local acknowledgement must replay accepted work, with duplicates counted separately")
		})
	}
}
