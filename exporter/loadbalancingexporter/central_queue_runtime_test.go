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
)

func TestCentralQueueRuntimeShutdownStopsRetryingFailedWindow(t *testing.T) {
	cfg := CentralQueueConfig{
		Enabled:            true,
		PayloadCompression: QueuePayloadCompressionNone,
		CapacityBytes:      4096,
		NumConsumers:       1,
		RequestBatching: CentralQueueBatchingConfig{
			TargetCompressedBytes: 100,
			MaxCompressedBytes:    1000,
			MaxUncompressedBytes:  1000,
			MaxMergedItems:        10,
			MaxDelay:              time.Millisecond,
			LaneCount:             1,
		},
	}
	runtime := newCentralQueueRuntime(cfg, centralQueueSignalLogs, nil)
	var attempts atomic.Int64
	runtime.start(func(context.Context, centralQueueWindow) error {
		attempts.Add(1)
		return errors.New("backend failed")
	}, nil)

	require.NoError(t, runtime.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.Eventually(t, func() bool {
		return attempts.Load() > 0
	}, time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err := runtime.shutdown(ctx)
	require.ErrorContains(t, err, "backend failed")
	require.Less(t, attempts.Load(), int64(10))
}
