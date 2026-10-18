// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
)

func TestDurableCentralQueueReplaysOversizedSingletonWithinRecordLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	destination.blocked.Store(true)
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	queue := map[string]any{"max_uncompressed_batch_bytes": 256, "max_uncompressed_log_record_bytes": 2048}
	exp, store, _ := newDurableSignalTestExporterWithQueueConfig(t, dir, []string{listener.Addr().String()}, "logs", nil, queue)
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr(strings.Repeat("x", 1024))
	record.Attributes().PutInt("test.id", 42)
	require.NoError(t, exp.(exporter.Logs).ConsumeLogs(t.Context(), logs))
	require.ErrorIs(t, exp.Shutdown(t.Context()), context.DeadlineExceeded)
	require.NoError(t, store.Shutdown(t.Context()))
	destination.blocked.Store(false)

	reopened, reopenedStore, _ := newDurableSignalTestExporterWithQueueConfig(t, dir, []string{listener.Addr().String()}, "logs", nil, queue, 2*time.Second)

	t.Cleanup(func() {
		_ = reopened.Shutdown(context.WithoutCancel(t.Context()))
		_ = reopenedStore.Shutdown(context.WithoutCancel(t.Context()))
	})
	require.Eventually(t, func() bool {
		destination.mu.Lock()
		defer destination.mu.Unlock()
		return len(destination.ids) == 1 && destination.ids[0] == 42
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, reopened.Shutdown(t.Context()))
	require.NoError(t, reopenedStore.Shutdown(t.Context()))
}
