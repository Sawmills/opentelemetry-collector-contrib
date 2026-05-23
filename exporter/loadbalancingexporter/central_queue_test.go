// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestCentralQueueAdmitsByCompressedBytes(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           10,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})

	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 4, uncompressedBytes: 40, count: 1}))
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 6, uncompressedBytes: 60, count: 1}))
	require.ErrorIs(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 1, uncompressedBytes: 10, count: 1}), errCentralQueueFull)
	require.EqualValues(t, 10, q.compressedBytes())
}

func TestCentralQueueLeaseReservesInflightUncompressedBytes(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 50,
		maxUncompressedBatchBytes:    100,
	})
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindMetrics, compressedBytes: 10, uncompressedBytes: 40, count: 1}))
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindMetrics, compressedBytes: 10, uncompressedBytes: 40, count: 1}))

	lease, err := q.lease(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 40, q.inflightUncompressedBytes())

	_, err = q.tryLease(time.Now())
	require.ErrorIs(t, err, errCentralQueueInflightFull)

	lease.done()
	require.EqualValues(t, 0, q.inflightUncompressedBytes())
}

func TestCentralQueueLeaseTreatsZeroInflightBudgetAsUnlimited(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:        100,
		maxUncompressedBatchBytes: 100,
		targetCompressedBytes:     10,
		maxBatchDelay:             time.Second,
	})
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 40, count: 1}))
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 40, count: 1}))

	first, err := q.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, first)
	require.EqualValues(t, 40, q.inflightUncompressedBytes())

	second, err := q.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, second)
	require.EqualValues(t, 80, q.inflightUncompressedBytes())

	first.done()
	second.done()
}

func TestCentralQueueEnqueueSignalsLeaseWaiters(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:        100,
		maxUncompressedBatchBytes: 100,
		targetCompressedBytes:     10,
		maxBatchDelay:             time.Second,
	})
	now := time.Unix(10, 0)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	waiter := startCentralQueueLeaseWithoutPolling(ctx, q)
	requireNoCentralQueueLeaseResult(t, waiter)

	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("lane-a"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, now))

	lease := requireCentralQueueLeaseResult(t, waiter)
	require.Equal(t, []byte("lane-a"), lease.window.routingKey)
	lease.done()
}

func TestCentralQueueDoneSignalsInflightWaiters(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 10,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	waiter := startCentralQueueLeaseWithoutPolling(ctx, q)
	requireNoCentralQueueLeaseResult(t, waiter)

	lease.done()
	secondLease := requireCentralQueueLeaseResult(t, waiter)
	require.Equal(t, []byte("lane-b"), secondLease.window.routingKey)
	secondLease.done()
}

func TestCentralQueueTryLeaseWithAcquireBlocksWhenConsumersFull(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	lease, err := q.tryLeaseWithAcquire(now, func(queueCompressedBytes int64) (func(), bool) {
		require.EqualValues(t, 10, queueCompressedBytes)
		return nil, false
	})

	require.ErrorIs(t, err, errCentralQueueConsumersFull)
	require.Nil(t, lease)
}

func TestCentralQueueTryLeaseWithAcquireDoesNotHoldQueueLockWhileAcquiring(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	acquireStarted := make(chan struct{})
	releaseAcquire := make(chan struct{})
	result := make(chan centralQueueLeaseResult, 1)
	go func() {
		lease, err := q.tryLeaseWithAcquire(now, func(int64) (func(), bool) {
			close(acquireStarted)
			<-releaseAcquire
			return nil, false
		})
		result <- centralQueueLeaseResult{lease: lease, err: err}
	}()

	<-acquireStarted
	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now)
	}()

	select {
	case err := <-enqueueDone:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		close(releaseAcquire)
		t.Fatal("enqueue blocked while acquire callback was waiting")
	}

	close(releaseAcquire)
	select {
	case leaseResult := <-result:
		require.ErrorIs(t, leaseResult.err, errCentralQueueConsumersFull)
		require.Nil(t, leaseResult.lease)
	case <-time.After(time.Second):
		t.Fatal("expected central queue acquire result")
	}
}

func TestCentralQueueTryLeaseWithAcquireSucceedsWhenConsumersAvailable(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	lease, err := q.tryLeaseWithAcquire(now, func(queueCompressedBytes int64) (func(), bool) {
		require.EqualValues(t, 10, queueCompressedBytes)
		return func() {}, true
	})

	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("lane-a"), lease.window.routingKey)
	lease.done()
}

func TestCentralQueueTryLeaseWithAcquirePreparesOnlyOneReadyWindow(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	for _, lane := range []string{"lane-a", "lane-b", "lane-c", "lane-d"} {
		require.NoError(t, q.enqueueAt(centralQueueItem{
			signal:            signalKindLogs,
			routingKey:        []byte(lane),
			compressedBytes:   10,
			uncompressedBytes: 10,
			count:             1,
		}, now))
	}

	lease, err := q.tryLeaseWithAcquire(now, func(int64) (func(), bool) { return func() {}, true })

	require.NoError(t, err)
	require.NotNil(t, lease)
	require.EqualValues(t, 10, q.inflightUncompressedBytes())
	requireCentralQueueReadyWindows(t, q, 0)
	lease.done()
}

func TestCentralQueueLeaseWithAcquireRetriesWhenConsumersBecomeAvailable(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	select {
	case <-q.notify:
	default:
	}

	var attempts atomic.Int64
	result := make(chan centralQueueLeaseResult, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	go func() {
		lease, err := q.leaseWithPollIntervalAndAcquire(ctx, time.Hour, func(int64) (func(), bool) {
			if attempts.Add(1) >= 2 {
				return func() {}, true
			}
			return nil, false
		})
		result <- centralQueueLeaseResult{lease: lease, err: err}
	}()

	require.Eventually(t, func() bool {
		return attempts.Load() == 1
	}, time.Second, 10*time.Millisecond)
	requireNoCentralQueueLeaseResult(t, result)
	q.notifyLeaseWaiters()

	lease := requireCentralQueueLeaseResult(t, result)
	require.Equal(t, []byte("lane-a"), lease.window.routingKey)
	lease.done()
}

func TestCentralQueuePrunesEmptyBucketsAfterScheduling(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:        100,
		maxUncompressedBatchBytes: 100,
		targetCompressedBytes:     10,
		maxBatchDelay:             time.Second,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	requireCentralQueueBucketCounts(t, q, 2, 2, 2)

	first, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, first)
	requireCentralQueueBucketCounts(t, q, 1, 1, 1)

	first.done()
	second, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, second)
	requireCentralQueueBucketCounts(t, q, 0, 0, 0)

	second.done()
}

func TestCentralQueueLeaseReservesCompressedBytesUntilDoneOrRequeue(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           10,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 40, count: 1}))

	lease, err := q.lease(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 10, q.compressedBytes())
	require.ErrorIs(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 1, uncompressedBytes: 1, count: 1}), errCentralQueueFull)

	require.NoError(t, lease.requeue(time.Now()))
	require.Equal(t, 1, q.len())
	require.EqualValues(t, 10, q.compressedBytes())

	retryLease, err := q.tryLease(time.Now().Add(centralQueueRetryDelayUpperBound(0)))
	require.NoError(t, err)
	require.NotNil(t, retryLease)
	retryLease.done()
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueLeaseCoalescesReadyItemsByRoutingKey(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("lane-a"), lease.window.routingKey)
	require.Len(t, lease.window.items, 2)
	require.Equal(t, 20, lease.window.compressedBytes)
	require.Equal(t, 40, lease.window.uncompressedBytes)
	require.Equal(t, centralQueueFlushReasonTargetReached, lease.window.flushReason)
	require.Equal(t, 1, q.len())
	require.EqualValues(t, 30, q.compressedBytes())

	lease.done()
	require.EqualValues(t, 10, q.compressedBytes())
}

func TestCentralQueueReadyWindowsGiveEachReadyLaneATurnBeforeHotLaneRepeats(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              2,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("hot"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("hot"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("hot"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("hot"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("cold"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("cold"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	first, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, []byte("hot"), first.window.routingKey)
	require.Equal(t, centralQueueFlushReasonTargetReached, first.window.flushReason)

	second, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, []byte("cold"), second.window.routingKey)
	require.Equal(t, centralQueueFlushReasonTargetReached, second.window.flushReason)

	first.done()
	second.done()
}

func TestCentralQueueReadyWindowsHandleInterleavedLaneItems(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              2,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	first, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, []byte("lane-a"), first.window.routingKey)
	require.Len(t, first.window.items, 2)

	second, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, []byte("lane-b"), second.window.routingKey)
	require.Len(t, second.window.items, 2)

	first.done()
	second.done()
	require.Zero(t, q.len())
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueCollectWindowCandidatesGroupsInterleavedKeys(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Hour,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("a-1"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), payload: []byte("b-1"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("a-2"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), payload: []byte("b-2"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	q.mu.Lock()
	targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now)
	materializeCentralQueueCandidatesLocked(targetCandidates)
	q.mu.Unlock()

	require.True(t, hasReady)
	require.Empty(t, fallbackCandidates)
	require.Len(t, targetCandidates, 2)
	require.Equal(t, []byte("lane-a"), targetCandidates[0].window.routingKey)
	require.Equal(t, []int{0, 1}, targetCandidates[0].indexes)
	require.Equal(t, []string{"a-1", "a-2"}, centralQueuePayloadStrings(targetCandidates[0].window.items))
	require.Equal(t, centralQueueFlushReasonTargetReached, targetCandidates[0].window.flushReason)
	require.Equal(t, []byte("lane-b"), targetCandidates[1].window.routingKey)
	require.Equal(t, []int{0, 1}, targetCandidates[1].indexes)
	require.Equal(t, []string{"b-1", "b-2"}, centralQueuePayloadStrings(targetCandidates[1].window.items))
	require.Equal(t, centralQueueFlushReasonTargetReached, targetCandidates[1].window.flushReason)
}

func TestCentralQueueCollectWindowCandidatesMarksHardCapBeforeTarget(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    30,
		targetCompressedBytes:        100,
		maxBatchDelay:                time.Hour,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("first"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("second"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	q.mu.Lock()
	targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now)
	materializeCentralQueueCandidatesLocked(fallbackCandidates)
	q.mu.Unlock()

	require.True(t, hasReady)
	require.Empty(t, targetCandidates)
	require.Len(t, fallbackCandidates, 1)
	require.Equal(t, centralQueueFlushReasonHardCap, fallbackCandidates[0].window.flushReason)
	require.Equal(t, []int{0}, fallbackCandidates[0].indexes)
	require.Equal(t, []string{"first"}, centralQueuePayloadStrings(fallbackCandidates[0].window.items))
}

func TestCentralQueueCollectWindowCandidatesUsesMaxDelayForLowTraffic(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        100,
		maxBatchDelay:                250 * time.Millisecond,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	q.mu.Lock()
	targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now.Add(249 * time.Millisecond))
	q.mu.Unlock()
	require.True(t, hasReady)
	require.Empty(t, targetCandidates)
	require.Empty(t, fallbackCandidates)

	q.mu.Lock()
	targetCandidates, fallbackCandidates, hasReady = q.collectWindowCandidatesLocked(now.Add(250 * time.Millisecond))
	q.mu.Unlock()
	require.True(t, hasReady)
	require.Empty(t, targetCandidates)
	require.Len(t, fallbackCandidates, 1)
	require.Equal(t, centralQueueFlushReasonMaxDelayLowTraffic, fallbackCandidates[0].window.flushReason)
}

func TestCentralQueueCollectWindowCandidatesShutdownOverridesWaitingAndHardCap(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    30,
		targetCompressedBytes:        100,
		maxBatchDelay:                time.Hour,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("first"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte("second"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	q.stop()

	q.mu.Lock()
	targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now)
	materializeCentralQueueCandidatesLocked(fallbackCandidates)
	q.mu.Unlock()

	require.True(t, hasReady)
	require.Empty(t, targetCandidates)
	require.Len(t, fallbackCandidates, 1)
	require.Equal(t, centralQueueFlushReasonShutdown, fallbackCandidates[0].window.flushReason)
	require.Equal(t, []int{0}, fallbackCandidates[0].indexes)
	require.Equal(t, []string{"first"}, centralQueuePayloadStrings(fallbackCandidates[0].window.items))
}

func TestCentralQueueMaterializeAndRemoveWindowKeepsSparseIndexSurvivors(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        100,
	})
	now := time.Unix(10, 0)
	for _, payload := range []string{"keep-0", "drop-1", "keep-2", "drop-3", "keep-4"} {
		require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), payload: []byte(payload), compressedBytes: 1, uncompressedBytes: 1, count: 1}, now))
	}

	q.mu.Lock()
	candidate := centralQueueWindowCandidate{
		bucket:  q.buckets[0],
		indexes: []int{1, 3},
	}
	materializeWindowCandidateItemsLocked(&candidate)
	q.removeWindowFromBucketLocked(candidate.bucket, candidate.indexes, false)
	remainingItems := q.queuedItemsLocked()
	q.mu.Unlock()

	require.Equal(t, []string{"drop-1", "drop-3"}, centralQueuePayloadStrings(candidate.window.items))
	require.Equal(t, []string{"keep-0", "keep-2", "keep-4"}, centralQueuePayloadStrings(remainingItems))
}

func TestCentralQueueReadyWindowsRecollectFallbackAfterTargetRemoval(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("fallback"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	lease, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("target"), lease.window.routingKey)
	require.Len(t, lease.window.items, 2)
	require.Equal(t, centralQueueFlushReasonTargetReached, lease.window.flushReason)

	fallbackLease, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, fallbackLease)
	require.Equal(t, []byte("fallback"), fallbackLease.window.routingKey)
	require.Len(t, fallbackLease.window.items, 1)
	require.Equal(t, centralQueueFlushReasonMaxDelayLowTraffic, fallbackLease.window.flushReason)

	lease.done()
	fallbackLease.done()
	require.Zero(t, q.len())
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueReadyWindowsDoNotLetHotTargetLaneStarveFallbackLane(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})
	now := time.Unix(10, 0)
	for range 4 {
		require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("hot"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	}
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("fallback"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))

	first, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, []byte("hot"), first.window.routingKey)
	require.Equal(t, centralQueueFlushReasonTargetReached, first.window.flushReason)

	second, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, []byte("fallback"), second.window.routingKey)
	require.Equal(t, centralQueueFlushReasonMaxDelayLowTraffic, second.window.flushReason)

	first.done()
	second.done()
}

func TestCentralQueueReadyWindowsBuildTargetSizedWindowsBeforeConsumersLease(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              4,
	})
	now := time.Unix(10, 0)
	for range 8 {
		require.NoError(t, q.enqueueAt(centralQueueItem{
			signal:            signalKindLogs,
			routingKey:        []byte("lane-a"),
			compressedBytes:   10,
			uncompressedBytes: 10,
			count:             1,
		}, now))
	}

	for range 4 {
		lease, err := q.tryLease(now)
		require.NoError(t, err)
		require.NotNil(t, lease)
		require.Equal(t, []byte("lane-a"), lease.window.routingKey)
		require.Len(t, lease.window.items, 2)
		require.Equal(t, 20, lease.window.compressedBytes)
		require.Equal(t, centralQueueFlushReasonTargetReached, lease.window.flushReason)
		lease.done()
	}
	require.Zero(t, q.len())
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueReadyWindowsReserveInflightBudgetUntilLeasedWindowDone(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              2,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.EqualValues(t, 80, q.inflightUncompressedBytes())
	requireCentralQueueReadyWindows(t, q, 1)

	lease.done()
	require.EqualValues(t, 40, q.inflightUncompressedBytes())
	requireCentralQueueReadyWindows(t, q, 1)
}

func TestCentralQueueLeasePrefersTargetWindowOverOlderUnderfilledWindow(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("older"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now.Add(10*time.Millisecond)))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now.Add(20*time.Millisecond)))

	lease, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("target"), lease.window.routingKey)
	require.Len(t, lease.window.items, 2)
	require.Equal(t, centralQueueFlushReasonTargetReached, lease.window.flushReason)
	require.Equal(t, 1, q.len())

	lease.done()
	require.EqualValues(t, 10, q.compressedBytes())
}

func TestCentralQueueLeaseDoesNotLeaseUnderfilledWindowWhenTargetWindowIsInflightBlocked(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 50,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("inflight"), compressedBytes: 20, uncompressedBytes: 30, count: 1}, now))
	inflightLease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, inflightLease)

	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("older"), compressedBytes: 10, uncompressedBytes: 10, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 15, count: 1}, now.Add(10*time.Millisecond)))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("target"), compressedBytes: 10, uncompressedBytes: 15, count: 1}, now.Add(20*time.Millisecond)))

	lease, err := q.tryLease(now.Add(250 * time.Millisecond))
	require.ErrorIs(t, err, errCentralQueueInflightFull)
	require.Nil(t, lease)
	require.Equal(t, 3, q.len())
	require.EqualValues(t, 30, q.inflightUncompressedBytes())

	inflightLease.done()
}

func TestCentralQueueLeaseDoesNotCoalesceDifferentRoutingKeys(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        100,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-b"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("lane-a"), lease.window.routingKey)
	require.Len(t, lease.window.items, 1)
	require.Equal(t, centralQueueFlushReasonMaxDelayLowTraffic, lease.window.flushReason)
}

func TestCentralQueueLeaseMarksHardCapFlush(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    30,
		targetCompressedBytes:        100,
		maxBatchDelay:                time.Second,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Len(t, lease.window.items, 1)
	require.Equal(t, centralQueueFlushReasonHardCap, lease.window.flushReason)
}

func TestCentralQueueLeaseWaitsForSmallWindowMaxDelay(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        100,
		maxBatchDelay:                250 * time.Millisecond,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now.Add(100 * time.Millisecond))
	require.NoError(t, err)
	require.Nil(t, lease)

	lease, err = q.tryLease(now.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Len(t, lease.window.items, 1)
	require.Equal(t, centralQueueFlushReasonMaxDelayLowTraffic, lease.window.flushReason)
}

func TestCentralQueueRequeuesWholeCoalescedWindow(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Len(t, lease.window.items, 2)
	require.NoError(t, lease.requeue(now))
	require.Equal(t, 2, q.len())
	require.EqualValues(t, 20, q.compressedBytes())

	lease, err = q.tryLease(now)
	require.NoError(t, err)
	require.Nil(t, lease)

	lease, err = q.tryLease(now.Add(centralQueueRetryDelayUpperBound(0)))
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Len(t, lease.window.items, 2)
	require.Equal(t, 1, lease.window.maxAttempt)
}

func TestCentralQueueRequeueUsesPerItemRetryDelay(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
	})
	now := time.Unix(10, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1, attempt: 3}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindMetrics, routingKey: []byte("lane-a"), compressedBytes: 10, uncompressedBytes: 20, count: 1}, now))

	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Len(t, lease.window.items, 2)
	require.Equal(t, 3, lease.window.maxAttempt)

	require.NoError(t, lease.requeue(now))

	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.queuedItemsLocked()
	require.Len(t, items, 2)
	require.Equal(t, 4, items[0].attempt)
	requireNextAttemptWithinRetryDelay(t, now, 3, items[0].nextAttemptUnixNano)
	require.Equal(t, 1, items[1].attempt)
	requireNextAttemptWithinRetryDelay(t, now, 0, items[1].nextAttemptUnixNano)
}

func TestCentralQueueSnapshotReportsOldestQueuedItemAge(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})
	base := time.Unix(10, 0)
	snapshotAt := func(now time.Time) centralQueueSnapshot {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.snapshotLockedAt(now)
	}

	require.Zero(t, snapshotAt(base).oldestItemAgeMillis)
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 10, count: 1}, base))
	require.NoError(t, q.enqueueAt(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 10, count: 1}, base.Add(100*time.Millisecond)))

	require.EqualValues(t, 250, snapshotAt(base.Add(250*time.Millisecond)).oldestItemAgeMillis)

	lease, err := q.tryLease(base.Add(250 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, lease)

	require.EqualValues(t, 150, snapshotAt(base.Add(250*time.Millisecond)).oldestItemAgeMillis)

	require.NoError(t, lease.requeue(base.Add(time.Second)))
	require.EqualValues(t, 300, snapshotAt(base.Add(300*time.Millisecond)).oldestItemAgeMillis)

	secondLease, err := q.tryLease(base.Add(300 * time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, secondLease)
	secondLease.done()
	require.EqualValues(t, 300, snapshotAt(base.Add(300*time.Millisecond)).oldestItemAgeMillis)

	retryReadyAt := base.Add(time.Second + centralQueueRetryDelayUpperBound(0))
	retryLease, err := q.tryLease(retryReadyAt)
	require.NoError(t, err)
	require.NotNil(t, retryLease)
	retryLease.done()
	require.Zero(t, snapshotAt(retryReadyAt).oldestItemAgeMillis)
}

func TestCentralQueueTelemetryOldestItemAgeAdvancesWithoutQueueMutation(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		telemetry:                    telemetry,
	})

	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 10, count: 1}))

	require.Eventually(t, func() bool {
		metric, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
		if err != nil {
			return false
		}
		gauge, ok := metric.Data.(metricdata.Gauge[int64])
		if !ok || len(gauge.DataPoints) != 1 {
			return false
		}
		return gauge.DataPoints[0].Value >= 10
	}, time.Second, 10*time.Millisecond)
}

func TestCentralQueueStopUnregistersOldestItemAgeObserver(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		telemetry:                    telemetry,
	})

	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 10, count: 1}))
	require.Eventually(t, func() bool {
		metric, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
		if err != nil {
			return false
		}
		gauge, ok := metric.Data.(metricdata.Gauge[int64])
		return ok && len(gauge.DataPoints) == 1
	}, time.Second, 10*time.Millisecond)

	q.stop()

	require.Eventually(t, func() bool {
		metric, err := reader.GetMetric("otelcol_loadbalancer_central_queue_oldest_item_age")
		if err != nil {
			return true
		}
		gauge, ok := metric.Data.(metricdata.Gauge[int64])
		return ok && len(gauge.DataPoints) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestCentralQueueStopUnregistersConsumerDecisionObserver(t *testing.T) {
	reader := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, reader.Shutdown(context.WithoutCancel(t.Context())))
	})
	telemetry, err := newCentralQueueTelemetry(reader.NewTelemetrySettings(), signalKindLogs)
	require.NoError(t, err)
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		telemetry:                    telemetry,
	})
	telemetry.recordConsumerDecision(t.Context(), centralQueueConsumerResult{
		effectiveConsumers:        2,
		queueDemandConsumers:      4,
		backendSafeConsumersPerLB: 2,
		limitReason:               centralQueueConsumerLimitReasonBackendCapacity,
		pressureState:             centralQueueConsumerPressureStable,
	})
	requireCentralQueueConsumerLimitReason(t, reader, centralQueueConsumerLimitReasonBackendCapacity)

	q.stop()

	require.Eventually(t, func() bool {
		metric, err := reader.GetMetric("otelcol_loadbalancer_central_queue_consumer_limit_reason")
		if err != nil {
			return true
		}
		gauge, ok := metric.Data.(metricdata.Gauge[int64])
		return ok && len(gauge.DataPoints) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestCentralQueueRetriesDoNotGrowOldestEnqueuedHeap(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})
	base := time.Unix(10, 0)
	blockedOldest := centralQueueItem{
		signal:              signalKindLogs,
		compressedBytes:     10,
		uncompressedBytes:   10,
		count:               1,
		nextAttemptUnixNano: base.Add(time.Hour).UnixNano(),
		enqueuedAtUnixNano:  base.UnixNano(),
	}
	retryingItem := centralQueueItem{
		signal:             signalKindLogs,
		compressedBytes:    10,
		uncompressedBytes:  10,
		count:              1,
		enqueuedAtUnixNano: base.Add(time.Millisecond).UnixNano(),
	}
	require.NoError(t, q.enqueueAt(blockedOldest, base))
	require.NoError(t, q.enqueueAt(retryingItem, base))

	now := base.Add(time.Second)
	for range 10 {
		lease, err := q.tryLease(now)
		require.NoError(t, err)
		require.NotNil(t, lease)
		require.NoError(t, lease.requeue(now))
		now = now.Add(centralQueueMaxRetryDelay + centralQueueMaxRetryJitter)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	require.Len(t, q.enqueuedAtCounts, 2)
	require.Len(t, q.oldestEnqueuedAt, 2)
}

func TestCentralQueueRejectsOversizedUncompressedItem(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    50,
	})

	err := q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 51, count: 1})
	require.ErrorIs(t, err, errCentralQueueItemTooLarge)
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueRejectsItemLargerThanInflightBudget(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 50,
		maxUncompressedBatchBytes:    100,
	})

	err := q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 51, count: 1})
	require.ErrorIs(t, err, errCentralQueueItemTooLarge)
	require.Zero(t, q.compressedBytes())
}

func TestCentralQueueLeaseReturnsContextErrorWhenEmpty(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := q.lease(ctx)
	require.ErrorIs(t, err, ctx.Err())
}

func TestCentralQueueStopAllowsDrainingExistingItems(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        100,
	})
	require.NoError(t, q.enqueue(centralQueueItem{signal: signalKindLogs, compressedBytes: 10, uncompressedBytes: 10, count: 1}))
	q.stop()

	lease, err := q.lease(t.Context())
	require.NoError(t, err)
	require.Equal(t, centralQueueFlushReasonShutdown, lease.window.flushReason)
	lease.done()

	_, err = q.lease(t.Context())
	require.ErrorIs(t, err, errCentralQueueStopped)
}

func TestCentralQueueStopDrainsBackedOffRetryWithoutWaiting(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
	})

	now := time.Now()
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("retrying"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, now))
	lease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, lease.requeue(now))

	q.stop()

	lease, err = q.tryLease(time.Now())
	require.NoError(t, err)
	require.NotNil(t, lease, "stopped queue must bypass retry backoff and drain immediately")
	require.Equal(t, centralQueueFlushReasonShutdown, lease.window.flushReason)
	lease.done()
}

func TestCentralQueueRotatesReadyBucketsWhenReadyWindowLimitSaturated(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1000,
		maxInflightUncompressedBytes: 1000,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              1,
	})

	now := time.Unix(1000, 0)
	for _, lane := range []string{"lane-a", "lane-b", "lane-c"} {
		for range 4 {
			require.NoError(t, q.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        []byte(lane),
				compressedBytes:   10,
				uncompressedBytes: 10,
				count:             1,
			}, now))
		}
	}

	leased := map[string]struct{}{}
	for range 3 {
		lease, err := q.tryLease(now)
		require.NoError(t, err)
		require.NotNil(t, lease)
		leased[string(lease.window.routingKey)] = struct{}{}
		lease.done()
	}

	require.Len(t, leased, 3, "ready buckets with equal readiness should rotate instead of repeatedly leasing the same hot bucket")
}

// TestCentralQueueStaleFallbackSchedulingDoesNotCorruptTargetIndexes guards the
// coderabbitai feedback: after stale-fallback scheduling removes items from
// queue buckets, the previously-collected target/fresh-fallback candidate indexes
// would point at the wrong rows. The scheduler must recollect those slices
// against the post-mutation queue before reusing them, or bucket removal
// will drop unrelated items.
func TestCentralQueueStaleFallbackSchedulingDoesNotCorruptTargetIndexes(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           10000,
		maxInflightUncompressedBytes: 10000,
		maxUncompressedBatchBytes:    1000,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})

	t0 := time.Unix(1000, 0)

	// Old cold lane — single item, will become stale.
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("cold-stale"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, t0))

	// Target lane: two items recent, each below target individually, together
	// reaching target_reached (compressed=20). Each carries a unique sentinel
	// in its payload so we can prove the right items survived after removal.
	target := t0.Add(2 * time.Second)
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("target-lane"),
		payload:           []byte("target-payload-1"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, target.Add(-5*time.Millisecond)))
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("target-lane"),
		payload:           []byte("target-payload-2"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, target.Add(-1*time.Millisecond)))

	// First lease: stale cold lane wins (anti-starvation).
	first, err := q.tryLease(target)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, []byte("cold-stale"), first.window.routingKey)
	first.done()

	// Second lease: target lane should be served and its window must contain
	// BOTH original target-lane items — not arbitrary items removed because
	// of corrupted post-mutation indexes.
	second, err := q.tryLease(target)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, []byte("target-lane"), second.window.routingKey)
	require.Len(t, second.window.items, 2)
	payloads := []string{string(second.window.items[0].payload), string(second.window.items[1].payload)}
	require.ElementsMatch(t, []string{"target-payload-1", "target-payload-2"}, payloads,
		"target-lane window must contain both original target-payload items; stale indexes would have removed the wrong rows")
	second.done()

	require.Zero(t, q.len(), "queue should be drained — no orphan items left from index mis-removal")
}

// TestCentralQueueStaleFallbackBlockedByInflightDoesNotPreemptTargetScheduling
// covers the architect-review feedback on the first SAW-7548 fix attempt: when
// a stale fallback candidate cannot fit under the inflight cap but a smaller
// target or fresh-fallback window still could, the scheduler must NOT return
// `InflightBytes` early and skip those later groups.
func TestCentralQueueStaleFallbackBlockedByInflightDoesNotPreemptTargetScheduling(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1000,
		maxInflightUncompressedBytes: 50,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})

	now := time.Unix(1000, 0)

	// Pre-occupy 30 bytes of inflight budget so the inflight cap (50) is
	// "loaded" — the remaining 20 bytes will gate which subsequent windows fit.
	// Primer reaches target on its own (compressedBytes >= targetCompressedBytes)
	// so it can be leased immediately and reserve inflight.
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("primer"),
		compressedBytes:   20,
		uncompressedBytes: 30,
		count:             1,
	}, now))
	primerLease, err := q.tryLease(now)
	require.NoError(t, err)
	require.NotNil(t, primerLease)
	require.EqualValues(t, 30, q.inflightUncompressedBytes())

	// Stale fallback: oldest, but 25 uncompressed bytes — when added to
	// primer's 30 inflight = 55 > 50 cap → blocked.
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("stale-too-big"),
		compressedBytes:   10,
		uncompressedBytes: 25,
		count:             1,
	}, now))

	// Fresh target: 10 + 10 = 20 uncompressed bytes total; combined with
	// primer's 30 inflight = exactly 50 = cap → fits.
	target := now.Add(2 * time.Second)
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("target"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, target.Add(-10*time.Millisecond)))
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("target"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, target.Add(-5*time.Millisecond)))

	lease, err := q.tryLease(target)
	require.NoError(t, err, "stale fallback being inflight-blocked must not bail before target scheduling")
	require.NotNil(t, lease)
	require.Equal(t, []byte("target"), lease.window.routingKey,
		"target window should be leased because stale fallback could not fit; SAW-7548 fix must not silently throttle the target group")
	lease.done()
	primerLease.done()
}

// TestCentralQueueForceScheduleAgeDefaultsToMaxBatchDelayMultiple verifies the
// derived default for the SAW-7548 starvation guard: when not explicitly set,
// forceScheduleAge = maxBatchDelay × centralQueueDefaultForceScheduleAgeMultiplier.
func TestCentralQueueForceScheduleAgeDefaultsToMaxBatchDelayMultiple(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxBatchDelay: 250 * time.Millisecond,
	})
	require.Equal(t, 250*time.Millisecond*centralQueueDefaultForceScheduleAgeMultiplier, q.settings.forceScheduleAge)
}

// TestCentralQueueForceScheduleAgeNegativeDisablesAntiStarvation verifies that a
// negative forceScheduleAge opts out of the SAW-7548 anti-starvation behavior
// and restores the legacy strict-target-first scheduling.
func TestCentralQueueForceScheduleAgeNegativeDisablesAntiStarvation(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1000,
		maxInflightUncompressedBytes: 1000,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              1,
		forceScheduleAge:             -1,
	})
	now := time.Unix(1000, 0)
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal: signalKindLogs, routingKey: []byte("old-fallback"),
		compressedBytes: 10, uncompressedBytes: 10, count: 1,
	}, now))
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal: signalKindLogs, routingKey: []byte("fresh-target"),
		compressedBytes: 10, uncompressedBytes: 10, count: 1,
	}, now.Add(2*time.Second)))
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal: signalKindLogs, routingKey: []byte("fresh-target"),
		compressedBytes: 10, uncompressedBytes: 10, count: 1,
	}, now.Add(2*time.Second)))

	lease, err := q.tryLease(now.Add(2 * time.Second))
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("fresh-target"), lease.window.routingKey,
		"with forceScheduleAge<0, target should win even over very-old fallback (legacy behavior)")
	lease.done()
}

// TestCentralQueuePromotesStaleFallbackOverFreshTargets demonstrates that when a
// fallback candidate has been waiting beyond the starvation threshold (default
// 5 * maxBatchDelay), it must be scheduled before fresh target candidates even
// though target_reached normally wins. This is the SAW-7548 anti-starvation
// guarantee: oldest_item_age is bounded so cold lanes cannot be indefinitely
// starved by continuous hot-key target arrivals.
func TestCentralQueuePromotesStaleFallbackOverFreshTargets(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1000,
		maxInflightUncompressedBytes: 1000,
		maxUncompressedBatchBytes:    100,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})

	enqueueAt := time.Unix(1000, 0)
	// Cold lane: a single 10-byte item, below targetCompressedBytes=20.
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("cold"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, enqueueAt))

	// Wait long enough that the cold item exceeds the starvation threshold
	// (default = 5 × maxBatchDelay = 1250ms). At 2s of waiting the cold lane
	// is definitively stale.
	leaseTime := enqueueAt.Add(2 * time.Second)

	// Fresh hot lanes arrive at lease time, each producing a target window.
	for h := range 5 {
		laneKey := fmt.Appendf(nil, "hot-%d", h)
		for range 2 {
			require.NoError(t, q.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        laneKey,
				compressedBytes:   10,
				uncompressedBytes: 10,
				count:             1,
			}, leaseTime.Add(-10*time.Millisecond)))
		}
	}

	lease, err := q.tryLease(leaseTime)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []byte("cold"), lease.window.routingKey,
		"stale fallback (waited %s) must be prioritized over fresh targets; otherwise oldest_item_age grows unbounded as in SAW-7548",
		leaseTime.Sub(enqueueAt))
	lease.done()
}

// TestCentralQueueDoesNotStarveColdLaneUnderContinuousHotKeyArrivals exercises
// the production-shaped scenario: continuous arrivals of new hot-routing-key
// lanes whose target candidates always exceed maxReadyWindows, while a single
// cold lane has been waiting. Under SAW-7548-buggy scheduling, the cold lane
// is never served — oldest_item_age grows unbounded until the inflight or
// compressed caps fire upstream refusals. After the fix, the cold lane MUST
// be served within a bounded number of rounds.
func TestCentralQueueDoesNotStarveColdLaneUnderContinuousHotKeyArrivals(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           10000,
		maxInflightUncompressedBytes: 10000,
		maxUncompressedBatchBytes:    1000,
		targetCompressedBytes:        20,
		maxBatchDelay:                250 * time.Millisecond,
		maxReadyWindows:              2,
	})

	now := time.Unix(1000, 0)
	// Cold lane: one item at t0.
	require.NoError(t, q.enqueueAt(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("cold"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}, now))

	const totalRounds = 30
	coldServedAtRound := -1

	for round := range totalRounds {
		roundTime := now.Add(time.Duration(round+1) * 100 * time.Millisecond)
		// Each round, 5 brand-new hot lanes each fill a target window.
		for h := range 5 {
			laneKey := fmt.Appendf(nil, "hot-r%d-l%d", round, h)
			for range 2 {
				require.NoError(t, q.enqueueAt(centralQueueItem{
					signal:            signalKindLogs,
					routingKey:        laneKey,
					compressedBytes:   10,
					uncompressedBytes: 10,
					count:             1,
				}, roundTime))
			}
		}

		// Workers lease 2 windows per round.
		for range 2 {
			lease, err := q.tryLease(roundTime.Add(250 * time.Millisecond))
			require.NoError(t, err)
			if lease == nil {
				continue
			}
			if string(lease.window.routingKey) == "cold" && coldServedAtRound == -1 {
				coldServedAtRound = round
			}
			lease.done()
		}

		if coldServedAtRound >= 0 {
			break
		}
	}

	require.GreaterOrEqual(t, coldServedAtRound, 0,
		"cold lane was never served in %d rounds (%s of continuous hot-key arrivals); SAW-7548 starvation reproduced",
		totalRounds, time.Duration(totalRounds)*100*time.Millisecond)
}

func BenchmarkCentralQueueDrainInterleavedBacklog(b *testing.B) {
	const (
		laneCount             = 64
		itemsPerLane          = 512
		itemCompressedBytes   = 4 * 1024
		itemUncompressedBytes = 16 * 1024
		targetCompressedBytes = 256 * 1024
	)

	laneKeys := make([][]byte, laneCount)
	for lane := range laneCount {
		laneKeys[lane] = fmt.Appendf(nil, "lane-%02d", lane)
	}
	now := time.Unix(1000, 0)

	b.ReportAllocs()
	for b.Loop() {
		q := newCentralQueue(centralQueueSettings{
			maxCompressedBytes:           int64(laneCount * itemsPerLane * itemCompressedBytes),
			maxInflightUncompressedBytes: int64(laneCount * targetCompressedBytes * 16),
			maxUncompressedBatchBytes:    laneCount * targetCompressedBytes,
			targetCompressedBytes:        targetCompressedBytes,
			maxBatchDelay:                time.Second,
			maxReadyWindows:              laneCount,
		})

		b.StopTimer()
		for itemIndex := range itemsPerLane {
			for lane := range laneCount {
				require.NoError(b, q.enqueueAt(centralQueueItem{
					signal:            signalKindLogs,
					routingKey:        laneKeys[lane],
					compressedBytes:   itemCompressedBytes,
					uncompressedBytes: itemUncompressedBytes,
					count:             1,
				}, now.Add(time.Duration(itemIndex)*time.Nanosecond)))
			}
		}
		b.StartTimer()

		for q.len() > 0 {
			lease, err := q.tryLease(now.Add(time.Second))
			require.NoError(b, err)
			if lease == nil {
				b.Fatalf("queue stalled with %d items remaining", q.len())
			}
			lease.done()
		}
	}
}

func BenchmarkCentralQueueCollectInterleavedUnderfilledBacklog(b *testing.B) {
	const (
		laneCount             = 64
		itemsPerLane          = 512
		itemCompressedBytes   = 1
		itemUncompressedBytes = 16 * 1024
	)

	laneKeys := make([][]byte, laneCount)
	for lane := range laneCount {
		laneKeys[lane] = fmt.Appendf(nil, "lane-%02d", lane)
	}
	now := time.Unix(1000, 0)

	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           int64(laneCount * itemsPerLane * itemCompressedBytes),
		maxInflightUncompressedBytes: int64(laneCount * itemsPerLane * itemUncompressedBytes),
		maxUncompressedBatchBytes:    laneCount * itemsPerLane * itemUncompressedBytes,
		targetCompressedBytes:        itemsPerLane + 1,
		maxBatchDelay:                time.Hour,
		maxReadyWindows:              laneCount,
	})
	for itemIndex := range itemsPerLane {
		for lane := range laneCount {
			require.NoError(b, q.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        laneKeys[lane],
				compressedBytes:   itemCompressedBytes,
				uncompressedBytes: itemUncompressedBytes,
				count:             1,
			}, now.Add(time.Duration(itemIndex)*time.Nanosecond)))
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		q.mu.Lock()
		targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now)
		q.mu.Unlock()
		require.True(b, hasReady)
		require.Empty(b, targetCandidates)
		require.Empty(b, fallbackCandidates)
	}
}

func requireCentralQueueFirstRetryDelay(t *testing.T, q *centralQueue) {
	t.Helper()

	var item centralQueueItem
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		items := q.queuedItemsLocked()
		if len(items) != 1 || items[0].attempt != 1 || items[0].nextAttemptUnixNano == 0 {
			return false
		}
		item = items[0]
		return true
	}, time.Second, time.Millisecond)

	retryAfterEnqueue := time.Unix(0, item.nextAttemptUnixNano).Sub(time.Unix(0, item.enqueuedAtUnixNano))
	require.GreaterOrEqual(t, retryAfterEnqueue, centralQueueRetryDelay(0))
	require.LessOrEqual(t, retryAfterEnqueue, centralQueueRetryDelayUpperBound(0)+50*time.Millisecond)
}

func centralQueuePayloadStrings(items []centralQueueItem) []string {
	payloads := make([]string, 0, len(items))
	for i := range items {
		payloads = append(payloads, string(items[i].payload))
	}
	return payloads
}

func materializeCentralQueueCandidatesLocked(candidates []centralQueueWindowCandidate) {
	for i := range candidates {
		materializeWindowCandidateItemsLocked(&candidates[i])
	}
}

type centralQueueLeaseResult struct {
	lease *centralQueueLease
	err   error
}

func startCentralQueueLeaseWithoutPolling(ctx context.Context, q *centralQueue) <-chan centralQueueLeaseResult {
	result := make(chan centralQueueLeaseResult, 1)
	go func() {
		lease, err := q.leaseWithPollInterval(ctx, 0)
		result <- centralQueueLeaseResult{lease: lease, err: err}
	}()
	return result
}

func requireNoCentralQueueLeaseResult(t *testing.T, result <-chan centralQueueLeaseResult) {
	t.Helper()
	select {
	case leaseResult := <-result:
		require.NoError(t, leaseResult.err)
		t.Fatalf("unexpected central queue lease: %+v", leaseResult.lease)
	case <-time.After(25 * time.Millisecond):
	}
}

func requireCentralQueueLeaseResult(t *testing.T, result <-chan centralQueueLeaseResult) *centralQueueLease {
	t.Helper()
	select {
	case leaseResult := <-result:
		require.NoError(t, leaseResult.err)
		require.NotNil(t, leaseResult.lease)
		return leaseResult.lease
	case <-time.After(time.Second):
		t.Fatal("expected central queue lease")
		return nil
	}
}

func requireCentralQueueBucketCounts(t *testing.T, q *centralQueue, buckets, bucketsByKey, readyBuckets int) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	require.Len(t, q.buckets, buckets)
	require.Len(t, q.bucketsByKey, bucketsByKey)
	require.Len(t, q.readyBuckets, readyBuckets)
}

func requireNextAttemptWithinRetryDelay(t *testing.T, now time.Time, attempt int, nextAttemptUnixNano int64) {
	t.Helper()
	nextAttempt := time.Unix(0, nextAttemptUnixNano)
	require.GreaterOrEqual(t, nextAttempt.Sub(now), centralQueueRetryDelay(attempt))
	require.LessOrEqual(t, nextAttempt.Sub(now), centralQueueRetryDelayUpperBound(attempt))
}

func centralQueueRetryDelayUpperBound(attempt int) time.Duration {
	delay := centralQueueRetryDelay(attempt)
	return delay + centralQueueRetryJitterLimit(delay)
}

func requireCentralQueueReadyWindows(t *testing.T, q *centralQueue, expected int) {
	t.Helper()

	q.mu.Lock()
	defer q.mu.Unlock()
	require.Len(t, q.ready, expected)
}
