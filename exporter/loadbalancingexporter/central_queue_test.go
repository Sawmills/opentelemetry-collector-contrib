// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueAdmissionUsesCompressedBytes(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(100))
	item := centralQueueTestItem(centralQueueSignalLogs, 1, 60, 600, 10)

	require.NoError(t, q.enqueue(t.Context(), item))
	require.Equal(t, int64(60), q.currentBytes())
	require.ErrorIs(t, q.enqueue(t.Context(), item), errCentralQueueFull)
	require.Equal(t, int64(60), q.currentBytes())
}

func TestCentralQueueWindowCoalescesByLane(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(4096))
	keyA := centralQueueKey{signal: centralQueueSignalMetrics, backendID: defaultCentralQueueBackendID, laneID: 1}
	keyB := centralQueueKey{signal: centralQueueSignalMetrics, backendID: defaultCentralQueueBackendID, laneID: 2}

	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{key: keyA, compressedBytes: 100, uncompressedBytes: 200, itemCount: 1, enqueuedAt: time.Unix(1, 0)}))
	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{key: keyB, compressedBytes: 100, uncompressedBytes: 200, itemCount: 1, enqueuedAt: time.Unix(2, 0)}))
	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{key: keyA, compressedBytes: 100, uncompressedBytes: 200, itemCount: 1, enqueuedAt: time.Unix(3, 0)}))

	window, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Equal(t, keyA, window.key)
	require.Len(t, window.items, 2)
	require.Equal(t, 200, window.compressedBytes)
	require.Equal(t, time.Unix(1, 0), window.oldestEnqueuedAt)

	window, ok = q.nextWindow(t.Context())
	require.True(t, ok)
	require.Equal(t, keyB, window.key)
	require.Len(t, window.items, 1)
}

func TestCentralQueueWindowStopsAtTargetCompressedBytes(t *testing.T) {
	settings := centralQueueTestSettings(4096)
	settings.batching.TargetCompressedBytes = 250
	settings.batching.MaxCompressedBytes = 1000
	q := newCentralQueueForTest(settings)

	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))

	window, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Len(t, window.items, 3)
	require.Equal(t, 300, window.compressedBytes)
	require.Equal(t, int64(100), q.currentBytes())
}

func TestCentralQueueWindowHonorsHardCaps(t *testing.T) {
	settings := centralQueueTestSettings(4096)
	settings.batching.TargetCompressedBytes = 1000
	settings.batching.MaxCompressedBytes = 250
	settings.batching.MaxUncompressedBytes = 1000
	settings.batching.MaxMergedItems = 10
	q := newCentralQueueForTest(settings)

	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))

	window, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Len(t, window.items, 2)
	require.Equal(t, 200, window.compressedBytes)
	require.Equal(t, int64(100), q.currentBytes())
}

func TestCentralQueueRejectsOversizedItem(t *testing.T) {
	settings := centralQueueTestSettings(4096)
	settings.batching.MaxCompressedBytes = 250
	q := newCentralQueueForTest(settings)

	err := q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 251, 300, 1))
	require.ErrorIs(t, err, errCentralQueueItemTooLarge)
	require.Zero(t, q.currentBytes())
}

func TestCentralQueueRequeueFrontPreservesBytesAndOrder(t *testing.T) {
	settings := centralQueueTestSettings(4096)
	settings.batching.TargetCompressedBytes = 200
	q := newCentralQueueForTest(settings)

	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))
	window, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Equal(t, int64(0), q.currentBytes())

	q.requeueFront(window)
	require.Equal(t, int64(200), q.currentBytes())

	next, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Equal(t, window.key, next.key)
	require.Len(t, next.items, 2)
}

func TestCentralQueueWaitsForMaxDelayBeforeSmallWindow(t *testing.T) {
	settings := centralQueueTestSettings(4096)
	settings.batching.TargetCompressedBytes = 1000
	settings.batching.MaxDelay = 20 * time.Millisecond
	q := newCentralQueueForTest(settings)

	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{
		key:               centralQueueKey{signal: centralQueueSignalLogs, backendID: defaultCentralQueueBackendID, laneID: 1},
		compressedBytes:   100,
		uncompressedBytes: 200,
		itemCount:         1,
		enqueuedAt:        time.Now(),
	}))

	done := make(chan centralQueueWindow, 1)
	go func() {
		window, ok := q.nextWindow(t.Context())
		require.True(t, ok)
		done <- window
	}()

	select {
	case <-done:
		require.Fail(t, "window flushed before max delay")
	case <-time.After(5 * time.Millisecond):
	}

	select {
	case window := <-done:
		require.Len(t, window.items, 1)
	case <-time.After(250 * time.Millisecond):
		require.Fail(t, "window did not flush after max delay")
	}
}

func TestCentralQueueStopDrainsThenStops(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(4096))
	require.NoError(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)))

	q.stop()
	require.ErrorIs(t, q.enqueue(t.Context(), centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1)), errCentralQueueStopped)

	window, ok := q.nextWindow(t.Context())
	require.True(t, ok)
	require.Len(t, window.items, 1)

	_, ok = q.nextWindow(t.Context())
	require.False(t, ok)
}

func TestCentralQueueNextWindowReturnsOnContextCancel(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(4096))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, ok := q.nextWindow(ctx)
	require.False(t, ok)
}

func TestCentralQueueOldestAgeAndLaneCount(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(4096))
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{
		key:               centralQueueKey{signal: centralQueueSignalLogs, backendID: defaultCentralQueueBackendID, laneID: 1},
		compressedBytes:   100,
		uncompressedBytes: 200,
		itemCount:         1,
		enqueuedAt:        now.Add(-5 * time.Second),
	}))
	require.NoError(t, q.enqueue(t.Context(), centralQueueItem{
		key:               centralQueueKey{signal: centralQueueSignalLogs, backendID: defaultCentralQueueBackendID, laneID: 2},
		compressedBytes:   100,
		uncompressedBytes: 200,
		itemCount:         1,
		enqueuedAt:        now.Add(-3 * time.Second),
	}))

	require.Equal(t, 2, q.activeLaneCount(centralQueueSignalLogs))
	require.Equal(t, 5*time.Second, q.oldestItemAge(centralQueueSignalLogs, now))
	require.Zero(t, q.oldestItemAge(centralQueueSignalMetrics, now))
}

func TestCentralQueueEnqueueHonorsContext(t *testing.T) {
	q := newCentralQueueForTest(centralQueueTestSettings(4096))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := q.enqueue(ctx, centralQueueTestItem(centralQueueSignalLogs, 1, 100, 200, 1))
	require.True(t, errors.Is(err, context.Canceled))
	require.Zero(t, q.currentBytes())
}

func newCentralQueueForTest(settings centralQueueSettings) *centralQueue {
	if settings.now == nil {
		settings.now = time.Now
	}
	return newCentralQueue(settings)
}

func centralQueueTestSettings(capacity int64) centralQueueSettings {
	return centralQueueSettings{
		capacityBytes: capacity,
		batching: CentralQueueBatchingConfig{
			TargetCompressedBytes: 256,
			MaxCompressedBytes:    1024,
			MaxUncompressedBytes:  2048,
			MaxMergedItems:        10,
			MaxDelay:              250 * time.Millisecond,
			LaneCount:             64,
		},
		now: time.Now,
	}
}

func centralQueueTestItem(signal centralQueueSignal, laneID uint32, compressedBytes, uncompressedBytes, itemCount int) centralQueueItem {
	return centralQueueItem{
		key: centralQueueKey{
			signal:    signal,
			backendID: defaultCentralQueueBackendID,
			laneID:    laneID,
		},
		compressedBytes:   compressedBytes,
		uncompressedBytes: uncompressedBytes,
		itemCount:         itemCount,
		enqueuedAt:        time.Now().Add(-time.Second),
	}
}
