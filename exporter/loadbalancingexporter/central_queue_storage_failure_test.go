// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
)

func TestDurableCentralQueueRejectsUndecodableRetainedPayload(t *testing.T) {
	runDurableRecovery(t, false, "corrupt-codec")
}

type corruptCodecStorage struct{ storage.Extension }

func (e corruptCodecStorage) GetClient(ctx context.Context, kind component.Kind, id component.ID, name string) (storage.Client, error) {
	client, err := e.Extension.GetClient(ctx, kind, id, name)
	if err != nil {
		return nil, err
	}
	return corruptCodecClient{Client: client}, nil
}

type corruptCodecClient struct{ storage.Client }

func (c corruptCodecClient) Batch(ctx context.Context, ops ...*storage.Operation) error {
	if err := c.Client.Batch(ctx, ops...); err != nil {
		return err
	}
	for _, op := range ops {
		if op.Type != storage.Get || !strings.HasPrefix(op.Key, "item/") {
			continue
		}
		// Preserve the storage envelope, checksum, counts and length while modeling
		// a retained payload that this codec version cannot interpret.
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(op.Value[4:], &envelope); err != nil {
			return err
		}
		var payload []byte
		if err := json.Unmarshal(envelope["Payload"], &payload); err != nil {
			return err
		}
		clear(payload)
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		envelope["Payload"] = encoded
		encoded, err = json.Marshal(envelope)
		if err != nil {
			return err
		}
		op.Value = make([]byte, len(encoded)+4)
		binary.BigEndian.PutUint32(op.Value, crc32.ChecksumIEEE(encoded))
		copy(op.Value[4:], encoded)
	}
	return nil
}

func TestDurableCentralQueueRetainsInvalidAcknowledgementWithoutResending(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	destination := &durableTestDestination{}
	destination.partial.Store(3) // Impossible: the request contains only two records.
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, destination)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	dir := t.TempDir()
	exp, ext, reader := newDurableTestExporter(t, dir, listener.Addr().String(), time.Second)
	t.Cleanup(func() {
		_ = exp.Shutdown(context.WithoutCancel(t.Context()))
		_ = ext.Shutdown(context.WithoutCancel(t.Context()))
	})
	input := plog.NewLogs()
	records := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := range 2 {
		r := records.AppendEmpty()
		r.Attributes().PutInt("test.id", int64(i))
		r.Body().SetStr("uncertain backend acknowledgement")
	}
	require.NoError(t, exp.ConsumeLogs(t.Context(), input))
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_storage_failures") == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), durableMetricValue(t, reader, "otelcol_loadbalancer_central_queue_durable_pending_items"))
	attempts := destination.attempts.Load()
	require.Never(t, func() bool { return destination.attempts.Load() > attempts }, time.Second, 10*time.Millisecond)
	started := time.Now()
	require.ErrorContains(t, exp.Shutdown(t.Context()), "invalid durable central queue terminal rejection count")
	require.Less(t, time.Since(started), 250*time.Millisecond, "paused durable ownership must defer recovery without spending the drain budget")
	require.NoError(t, ext.Shutdown(t.Context()))
	destination.partial.Store(0)
	reopened, store, reopenedReader := newDurableTestExporter(t, dir, listener.Addr().String())
	require.Eventually(t, func() bool {
		return durableMetricValue(t, reopenedReader, "otelcol_loadbalancer_central_queue_durable_pending_items") == 0
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, reopened.Shutdown(t.Context()))
	require.NoError(t, store.Shutdown(t.Context()))
	destination.mu.Lock()
	ids := append([]int64(nil), destination.ids...)
	destination.mu.Unlock()
	require.ElementsMatch(t, []int64{0, 1}, ids)
}

func TestDurableCentralQueueRejectsReducedDecodeCapacityBeforeDecoding(t *testing.T) {
	runDurableRecovery(t, false, "reduced-uncompressed")
}
