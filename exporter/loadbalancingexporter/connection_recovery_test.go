// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"errors"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
)

// The first connection from the affected LB never recovers. Other source
// connections, including another LB using the same endpoint, remain healthy.
type connectionRecoveryDestination struct {
	plogotlp.UnimplementedGRPCServer
	mu         sync.Mutex
	failedPeer string
	released   bool
	ids        map[string][]int64
	peers      map[string]map[string]struct{}
	responses  []codes.Code
}

// This adapter exercises the public transport-middleware seam. The first
// child has a permanently failing transport; replacement startup can fail or
// wait, while the child exporter, queue and lifecycle remain real.
type connectionRecoveryMiddleware struct {
	calls     atomic.Int64
	failStart bool
	entered   chan struct{}
	release   chan struct{}
}

func (*connectionRecoveryMiddleware) Start(context.Context, component.Host) error { return nil }
func (*connectionRecoveryMiddleware) Shutdown(context.Context) error              { return nil }
func (m *connectionRecoveryMiddleware) GetGRPCClientOptions(ctx context.Context) ([]grpc.DialOption, error) {
	generation := m.calls.Add(1)
	if generation == 2 {
		if m.entered != nil {
			close(m.entered)
		}
		if m.release != nil {
			select {
			case <-m.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if m.failStart {
			return nil, errors.New("synthetic transport setup failure")
		}
	}
	if generation == 1 {
		return []grpc.DialOption{grpc.WithChainUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			return status.Error(codes.DeadlineExceeded, "synthetic stale transport")
		})}, nil
	}
	return nil, nil
}

func TestCentralQueueRetainsLogsWhenConnectionRebuildStartFails(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	middleware := &connectionRecoveryMiddleware{failStart: true}
	id := component.MustNewID("recovery_transport")
	exp, store, reader := newDurableSignalTestExporterWithQueueConfig(t, t.TempDir(), []string{listener.Addr().String()}, "logs",
		map[component.ID]component.Component{id: middleware}, map[string]any{"reconnect_after_timeouts": 1}, 3*time.Second)
	t.Cleanup(func() {
		require.NoError(t, exp.Shutdown(context.WithoutCancel(t.Context())))
		require.NoError(t, store.Shutdown(context.WithoutCancel(t.Context())))
	})
	require.NoError(t, exp.(exporter.Logs).ConsumeLogs(t.Context(), connectionRecoveryInput(71, 72)))
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, int64(3), middleware.calls.Load(), "retry creation after one failed startup without discarding queue ownership")
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_backend_connection_rebuilds"))
	require.Zero(t, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_terminal_rejected_items"))
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.ElementsMatch(t, []int64{71, 72}, destination.ids)
}

func TestCentralQueueCoalescesConnectionRebuildsWhileStartupWaits(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	middleware := &connectionRecoveryMiddleware{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(middleware.release) }) }
	id := component.MustNewID("recovery_transport")
	exp, store, reader := newDurableSignalTestExporterWithQueueConfig(t, t.TempDir(), []string{listener.Addr().String()}, "logs",
		map[component.ID]component.Component{id: middleware}, map[string]any{"reconnect_after_timeouts": 1}, 3*time.Second)
	t.Cleanup(func() {
		release()
		require.NoError(t, exp.Shutdown(context.WithoutCancel(t.Context())))
		require.NoError(t, store.Shutdown(context.WithoutCancel(t.Context())))
	})
	require.NoError(t, exp.(exporter.Logs).ConsumeLogs(t.Context(), connectionRecoveryInput(81, 82)))
	select {
	case <-middleware.entered:
	case <-time.After(time.Second):
		t.Fatal("replacement startup did not reach the transport seam")
	}
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_backend_timeout_total") >= 3
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, int64(2), middleware.calls.Load(), "pending rebuilds must coalesce despite more timeout results")
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	release()
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, 3*time.Second, 5*time.Millisecond)
	require.Equal(t, int64(2), middleware.calls.Load(), "retired-generation results must not rebuild the current child")
	require.Equal(t, int64(1), durableMetricValue(t, reader, "otelcol_loadbalancer_backend_connection_rebuilds"))
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.ElementsMatch(t, []int64{81, 82}, destination.ids)
}

func TestCentralQueueShutdownCancelsPendingConnectionRebuild(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	middleware := &connectionRecoveryMiddleware{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(middleware.release) }) }
	id := component.MustNewID("recovery_transport")
	dir := t.TempDir()
	exp, store, reader := newDurableSignalTestExporterWithQueueConfig(t, dir, []string{listener.Addr().String()}, "logs",
		map[component.ID]component.Component{id: middleware}, map[string]any{"reconnect_after_timeouts": 1}, 100*time.Millisecond)
	closed := false
	t.Cleanup(func() {
		release()
		if !closed {
			_ = exp.Shutdown(context.WithoutCancel(t.Context()))
			_ = store.Shutdown(context.WithoutCancel(t.Context()))
		}
	})
	require.NoError(t, exp.(exporter.Logs).ConsumeLogs(t.Context(), connectionRecoveryInput(91, 92)))
	select {
	case <-middleware.entered:
	case <-time.After(time.Second):
		t.Fatal("replacement startup did not reach the transport seam")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, exp.Shutdown(ctx), "the durable backlog must remain owned at the drain deadline")
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_backend_connection_rebuilds") == 1
	}, time.Second, 5*time.Millisecond, "shutdown must cancel pending transport startup without a manual release")
	require.Zero(t, destination.attempts.Load(), "a late replacement must not send after shutdown")
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	require.NoError(t, store.Shutdown(t.Context()))
	closed = true
	recovered, recoveredStore, recoveredReader := newDurableSignalTestExporterWithQueueConfig(t, dir, []string{listener.Addr().String()}, "logs", nil, nil, 3*time.Second)
	t.Cleanup(func() {
		require.NoError(t, recovered.Shutdown(context.WithoutCancel(t.Context())))
		require.NoError(t, recoveredStore.Shutdown(context.WithoutCancel(t.Context())))
	})
	require.Eventually(t, func() bool {
		return durableMetricValue(t, recoveredReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, time.Second, 5*time.Millisecond)
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.ElementsMatch(t, []int64{91, 92}, destination.ids)
}

func (d *connectionRecoveryDestination) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	sources := md.Get("x-test-lb")
	if len(sources) != 1 {
		return plogotlp.NewExportResponse(), status.Error(codes.InvalidArgument, "missing synthetic LB source")
	}
	source := sources[0]
	p, ok := peer.FromContext(ctx)
	if !ok {
		return plogotlp.NewExportResponse(), status.Error(codes.Internal, "missing synthetic connection peer")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.peers[source] == nil {
		d.peers[source] = make(map[string]struct{})
	}
	d.peers[source][p.Addr.String()] = struct{}{}
	if d.responses != nil {
		code := codes.OK
		if len(d.responses) > 0 {
			code, d.responses = d.responses[0], d.responses[1:]
		}
		if code != codes.OK && !d.released {
			return plogotlp.NewExportResponse(), status.Error(code, "scripted backend result")
		}
	} else if source == "stuck" && !d.released {
		if d.failedPeer == "" {
			d.failedPeer = p.Addr.String()
		}
		if p.Addr.String() == d.failedPeer {
			return plogotlp.NewExportResponse(), status.Error(codes.DeadlineExceeded, "per-source connection timeout")
		}
	}
	for _, rl := range req.Logs().ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, record := range sl.LogRecords().All() {
				id, _ := record.Attributes().Get("test.id")
				d.ids[source] = append(d.ids[source], id.Int())
			}
		}
	}
	return plogotlp.NewExportResponse(), nil
}

func (d *connectionRecoveryDestination) accepted(source string) []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.ids[source])
}

func newConnectionRecoveryExporter(t *testing.T, endpoint, source string, queueOptions ...map[string]any) (exporter.Logs, *sdkmetric.ManualReader) {
	t.Helper()
	factory := loadbalancingexporter.NewFactory()
	cfg := factory.CreateDefaultConfig()
	queueConfig := map[string]any{
		"enabled": true, "max_compressed_bytes": 1 << 20,
		"max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20,
		"target_compressed_bytes": 512, "max_batch_delay": "5ms", "num_consumers": 2, "lane_count": 1,
	}
	for _, options := range queueOptions {
		maps.Copy(queueConfig, options)
	}
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"protocol": map[string]any{"otlp": map[string]any{
			"tls":     map[string]any{"insecure": true},
			"headers": map[string]any{"x-test-lb": source},
			"timeout": "100ms",
		}},
		"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{endpoint}}},
		"log_routing":   map[string]any{"ignore_trace_id": true},
		"central_queue": queueConfig,
	}).Unmarshal(cfg))
	require.NoError(t, cfg.(*loadbalancingexporter.Config).Validate())
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	settings := exportertest.NewNopSettings(factory.Type())
	settings.ID = component.NewIDWithName(factory.Type(), source)
	settings.MeterProvider = provider
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	return exp, reader
}

func TestCentralQueueReconnectTimeoutThresholdBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value int
		valid bool
	}{
		{name: "minimum", value: 1, valid: true},
		{name: "maximum", value: 100, valid: true},
		{name: "zero", value: 0},
		{name: "negative", value: -1},
		{name: "above maximum", value: 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadbalancingexporter.NewFactory().CreateDefaultConfig()
			require.NoError(t, confmap.NewFromStringMap(map[string]any{
				"resolver": map[string]any{"static": map[string]any{"hostnames": []string{"127.0.0.1:4317"}}},
				"central_queue": map[string]any{
					"enabled": true, "max_compressed_bytes": 1 << 20,
					"reconnect_after_timeouts": tc.value,
				},
			}).Unmarshal(cfg))
			err := cfg.(*loadbalancingexporter.Config).Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "reconnect_after_timeouts must be between 1 and 100")
			}
		})
	}
}

func connectionRecoveryInput(ids ...int64) plog.Logs {
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, id := range ids {
		record := records.AppendEmpty()
		record.Attributes().PutInt("test.id", id)
		record.Body().SetStr("retained through per-LB connection failure")
	}
	return logs
}

func TestCentralQueueRecoversPerLBConnectionWithoutLosingAcceptedLogs(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &connectionRecoveryDestination{ids: make(map[string][]int64), peers: make(map[string]map[string]struct{})}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	stuck, reader := newConnectionRecoveryExporter(t, listener.Addr().String(), "stuck")
	healthy, _ := newConnectionRecoveryExporter(t, listener.Addr().String(), "healthy")
	t.Cleanup(func() {
		destination.mu.Lock()
		destination.released = true
		destination.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		require.NoError(t, stuck.Shutdown(ctx))
		require.NoError(t, healthy.Shutdown(ctx))
	})
	require.NoError(t, stuck.ConsumeLogs(t.Context(), connectionRecoveryInput(41, 42)))
	require.NoError(t, healthy.ConsumeLogs(t.Context(), connectionRecoveryInput(51)))
	require.Eventually(t, func() bool { return len(destination.accepted("healthy")) == 1 }, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_backend_timeout_total") >= 3
	}, 3*time.Second, 5*time.Millisecond, "the affected LB must reach the worker and accumulate actual timeout results")
	require.Eventually(t, func() bool { return len(destination.accepted("stuck")) == 2 }, 4*time.Second, 5*time.Millisecond,
		"accepted logs must leave the permanent per-source timeout loop through a fresh connection")
	require.ElementsMatch(t, []int64{41, 42}, destination.accepted("stuck"))
	require.Equal(t, []int64{51}, destination.accepted("healthy"))
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_backend_connection_rebuilds") == 1
	}, time.Second, 5*time.Millisecond, "one replacement must be observable without endpoint labels")
	destination.mu.Lock()
	defer destination.mu.Unlock()
	require.Len(t, destination.peers["stuck"], 2, "replace only the failed LB connection")
	require.Len(t, destination.peers["healthy"], 1, "leave the healthy LB connection intact")
}

func TestCentralQueueTimeoutStreakResetsOnOtherResults(t *testing.T) {
	for _, tc := range []struct {
		name             string
		responses        []codes.Code
		separateRequests bool
		retries          int64
	}{
		{name: "success", responses: []codes.Code{codes.DeadlineExceeded, codes.OK, codes.DeadlineExceeded, codes.DeadlineExceeded, codes.OK}, separateRequests: true, retries: 3},
		{name: "non timeout error", responses: []codes.Code{codes.DeadlineExceeded, codes.Unavailable, codes.DeadlineExceeded, codes.DeadlineExceeded, codes.OK}, retries: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			destination := &connectionRecoveryDestination{ids: make(map[string][]int64), peers: make(map[string]map[string]struct{}), responses: tc.responses}
			server := grpc.NewServer()
			plogotlp.RegisterGRPCServer(server, destination)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			exp, reader := newConnectionRecoveryExporter(t, listener.Addr().String(), "stuck")
			t.Cleanup(func() {
				destination.mu.Lock()
				destination.released = true
				destination.mu.Unlock()
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
				defer cancel()
				require.NoError(t, exp.Shutdown(ctx))
			})
			require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(61)))
			require.Eventually(t, func() bool { return len(destination.accepted("stuck")) == 1 }, 5*time.Second, 5*time.Millisecond)
			if tc.separateRequests {
				require.NoError(t, exp.ConsumeLogs(t.Context(), connectionRecoveryInput(62)))
				require.Eventually(t, func() bool { return len(destination.accepted("stuck")) == 2 }, 5*time.Second, 5*time.Millisecond)
			}
			require.Equal(t, int64(3), durableMetricValue(t, reader, "otelcol_loadbalancer_backend_timeout_total"))
			require.Equal(t, int64(5), durableMetricValue(t, reader, "otelcol_loadbalancer_backend_request_total"))
			require.Equal(t, tc.retries, durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_retries"))
			destination.mu.Lock()
			defer destination.mu.Unlock()
			require.Len(t, destination.peers["stuck"], 1, "only consecutive timeouts may replace a connection")
		})
	}
}
