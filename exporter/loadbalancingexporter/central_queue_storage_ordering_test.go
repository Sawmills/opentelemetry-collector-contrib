// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
)

func TestDurableCentralQueueRecoversAfterOutOfOrderAcknowledgements(t *testing.T) {
	const count = 64
	destinations := []*durableTestDestination{{}, {}}
	var endpoints []string
	for _, destination := range destinations {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		destination.blocked.Store(true)
		server := grpc.NewServer()
		plogotlp.RegisterGRPCServer(server, destination)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		endpoints = append(endpoints, listener.Addr().String())
	}
	dir := t.TempDir()
	component, store, reader := newDurableSignalTestExporter(t, dir, endpoints, "logs", 50*time.Millisecond)
	exp := component.(exporter.Logs)
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		_ = store.Shutdown(context.WithoutCancel(t.Context()))
	})
	var expected []int64
	for i := range count {
		logs := plog.NewLogs()
		record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		record.Attributes().PutInt("test.id", int64(i))
		record.Body().SetStr("independent durable routing lane")
		traceID := pcommon.TraceID{}
		traceID[15] = byte(i + 1)
		record.SetTraceID(traceID)
		require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
		expected = append(expected, int64(i))
	}
	seen := func(destination *durableTestDestination) map[int64]struct{} {
		destination.mu.Lock()
		defer destination.mu.Unlock()
		result := make(map[int64]struct{}, len(destination.seen))
		for id := range destination.seen {
			result[id] = struct{}{}
		}
		return result
	}
	var first, second map[int64]struct{}
	require.Eventually(t, func() bool {
		first, second = seen(destinations[0]), seen(destinations[1])
		return len(first) > 0 && len(second) > 0 && len(first)+len(second) == count
	}, 5*time.Second, 10*time.Millisecond, "both backends must attempt their retained subset before releasing either")
	release, held := destinations[0], destinations[1]
	acknowledged, retained := first, second
	if _, hasLast := first[count-1]; !hasLast {
		release, held = held, release
		acknowledged, retained = second, first
	}
	require.Contains(t, acknowledged, int64(count-1))
	require.NotEmpty(t, retained, "at least one earlier admission must remain while the final admission is acknowledged")
	release.blocked.Store(false)
	require.Eventually(t, func() bool {
		release.mu.Lock()
		delivered := len(release.ids)
		release.mu.Unlock()
		return delivered == len(acknowledged) && durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items") == int64(len(retained))
	}, 5*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, exp.Shutdown(t.Context()), context.DeadlineExceeded)
	require.NoError(t, store.Shutdown(t.Context()))
	held.blocked.Store(false)
	recoveredComponent, recoveredStore, recoveredReader := newDurableSignalTestExporter(t, dir, endpoints, "logs")
	recovered := recoveredComponent.(exporter.Logs)
	require.Eventually(t, func() bool {
		return durableMetricValue(t, recoveredReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, recovered.Shutdown(t.Context()))
	require.NoError(t, recoveredStore.Shutdown(t.Context()))
	var delivered []int64
	for _, destination := range destinations {
		destination.mu.Lock()
		delivered = append(delivered, destination.ids...)
		destination.mu.Unlock()
	}
	require.ElementsMatch(t, expected, delivered, "acknowledged entries must stay removed and every retained entry must recover exactly once")
}
