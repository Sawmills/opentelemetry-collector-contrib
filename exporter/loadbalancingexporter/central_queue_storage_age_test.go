// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
)

func TestDurableCentralQueueReportsOldestTimestampAcrossAdmissionOrder(t *testing.T) {
	dir := t.TempDir()
	exp, store, _ := newDurableTestExporter(t, dir, "127.0.0.1:1")
	for range 2 {
		logs := plog.NewLogs()
		logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("retained timestamp fixture")
		require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
	}
	require.Error(t, exp.Shutdown(t.Context()))
	require.NoError(t, store.Shutdown(t.Context()))

	// Encode a valid version-1 store in which an earlier-starting request
	// committed second. Concurrent encoding/admission can produce this order.
	factory := filestorage.NewFactory()
	cfg := factory.CreateDefaultConfig().(*filestorage.Config)
	cfg.Directory = dir
	cfg.FSync = true
	ext, err := factory.Create(t.Context(), extensiontest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	client, err := ext.(storage.Extension).GetClient(t.Context(), component.KindExporter, component.NewIDWithName(loadbalancingexporter.NewFactory().Type(), "ordinal-0"), "central_queue_logs")
	require.NoError(t, err)
	manifestBytes, err := client.Get(t.Context(), "central_queue_v1")
	require.NoError(t, err)
	var manifest struct{ Head, Tail uint64 }
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.NotEqual(t, manifest.Head, manifest.Tail)
	times := []int64{1700000100, 1700000000}
	var ops []*storage.Operation
	for i, id := range []uint64{manifest.Head, manifest.Tail} {
		key := fmt.Sprintf("item/%d", id)
		frame, err := client.Get(t.Context(), key)
		require.NoError(t, err)
		var envelope map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(frame[4:], &envelope))
		stamp, err := json.Marshal(times[i] * 1e9)
		require.NoError(t, err)
		envelope["EnqueuedAt"] = stamp
		encoded, err := json.Marshal(envelope)
		require.NoError(t, err)
		updated := make([]byte, len(encoded)+4)
		binary.BigEndian.PutUint32(updated, crc32.ChecksumIEEE(encoded))
		copy(updated[4:], encoded)
		ops = append(ops, storage.SetOperation(key, updated))
	}
	require.NoError(t, client.Batch(t.Context(), ops...))
	require.NoError(t, client.Close(t.Context()))
	require.NoError(t, ext.Shutdown(t.Context()))
	reopened, reopenedStore, reader := newDurableTestExporter(t, dir, "127.0.0.1:1")
	defer func() { _ = reopened.Shutdown(t.Context()); require.NoError(t, reopenedStore.Shutdown(t.Context())) }()
	require.Equal(t, int64(1700000000), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_oldest_enqueued_time"), "oldest enqueue time must not assume persistence order")
}
