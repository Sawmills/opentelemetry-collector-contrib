// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"sync"
	"time"
)

const centralQueueLeasePollInterval = 10 * time.Millisecond
const (
	centralQueueInitialRetryDelay = 100 * time.Millisecond
	centralQueueMaxRetryDelay     = 5 * time.Second
	centralQueueMaxRetryJitter    = 100 * time.Millisecond
)

var errCentralQueueConsumersFull = errors.New("central queue effective consumers full")

// centralQueueDefaultForceScheduleAgeMultiplier bounds oldest_item_age under
// continuous hot-key arrivals: once a fallback candidate has been waiting longer
// than maxBatchDelay × this multiplier, the scheduler must serve it before any
// fresh target_reached candidate from a different lane. This prevents
// indefinite cold-lane starvation when target candidates continuously fill the
// maxReadyWindows budget (SAW-7548).
const centralQueueDefaultForceScheduleAgeMultiplier = 5

func centralQueueReadyWindowLimit(consumers int) int {
	if consumers <= 0 {
		return defaultCentralQueueNumConsumers
	}
	return consumers
}

type centralQueueSettings struct {
	maxCompressedBytes           int64
	maxInflightUncompressedBytes int64
	maxUncompressedBatchBytes    int
	targetCompressedBytes        int64
	maxBatchDelay                time.Duration
	maxReadyWindows              int
	// forceScheduleAge bounds how long a fallback candidate may wait before it
	// is promoted ahead of fresh target candidates. Zero means "derive from
	// maxBatchDelay × centralQueueDefaultForceScheduleAgeMultiplier". A negative
	// value disables anti-starvation (legacy strict target-first scheduling).
	forceScheduleAge time.Duration
	telemetry        *centralQueueTelemetry
}

type centralQueue struct {
	settings centralQueueSettings

	mu            sync.Mutex
	buckets       []*centralQueueBucket
	bucketsByKey  map[string]*centralQueueBucket
	readyBuckets  centralQueueReadyHeap
	readySequence int64
	itemCount     int
	stopped       bool
	ready         []centralQueueWindow
	notify        chan struct{}

	currentCompressedBytes int64
	currentInflightBytes   int64
	enqueuedAtCounts       map[int64]int
	enqueuedAtHeapEntries  map[int64]struct{}
	oldestEnqueuedAt       centralQueueEnqueuedAtHeap
}

type centralQueueLease struct {
	queue               *centralQueue
	window              centralQueueWindow
	item                centralQueueItem
	once                sync.Once
	consumerRelease     func()
	consumerReleaseOnce sync.Once
}

type centralQueueWindow struct {
	routingKey        []byte
	items             []centralQueueItem
	compressedBytes   int
	uncompressedBytes int
	count             int
	oldestEnqueuedAt  int64
	maxAttempt        int
	flushReason       centralQueueFlushReason
}

type centralQueueFlushReason string

const (
	centralQueueFlushReasonTargetReached      centralQueueFlushReason = "target_reached"
	centralQueueFlushReasonHardCap            centralQueueFlushReason = "hard_cap"
	centralQueueFlushReasonMaxDelayLowTraffic centralQueueFlushReason = "max_delay_low_traffic"
	centralQueueFlushReasonShutdown           centralQueueFlushReason = "shutdown"
)

type centralQueueWindowCandidate struct {
	bucket  *centralQueueBucket
	window  centralQueueWindow
	indexes []int
}

func newCentralQueue(settings centralQueueSettings) *centralQueue {
	if settings.targetCompressedBytes <= 0 {
		settings.targetCompressedBytes = 1
	}
	if settings.maxReadyWindows <= 0 {
		settings.maxReadyWindows = 1
	}
	if settings.forceScheduleAge == 0 && settings.maxBatchDelay > 0 {
		settings.forceScheduleAge = settings.maxBatchDelay * centralQueueDefaultForceScheduleAgeMultiplier
	}
	q := &centralQueue{
		settings:     settings,
		bucketsByKey: make(map[string]*centralQueueBucket),
		notify:       make(chan struct{}, 1),
	}
	q.settings.telemetry.observeOldestItemAge(q.oldestItemAgeMillis)
	q.settings.telemetry.observeSchedulerState(q.schedulerSnapshot)
	return q
}

func (q *centralQueue) enqueue(item centralQueueItem) error {
	return q.enqueueAt(item, time.Now())
}

func (q *centralQueue) enqueueAt(item centralQueueItem, now time.Time) error {
	if q.settings.maxUncompressedBatchBytes > 0 && item.uncompressedBytes > q.settings.maxUncompressedBatchBytes {
		q.settings.telemetry.recordRejected(context.Background(), int64(item.compressedBytes))
		return errCentralQueueItemTooLarge
	}
	if q.settings.maxInflightUncompressedBytes > 0 && int64(item.uncompressedBytes) > q.settings.maxInflightUncompressedBytes {
		q.settings.telemetry.recordRejected(context.Background(), int64(item.compressedBytes))
		return errCentralQueueItemTooLarge
	}

	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return errCentralQueueStopped
	}
	if q.currentCompressedBytes+int64(item.compressedBytes) > q.settings.maxCompressedBytes {
		q.mu.Unlock()
		q.settings.telemetry.recordRejected(context.Background(), int64(item.compressedBytes))
		return errCentralQueueFull
	}
	if item.enqueuedAtUnixNano == 0 {
		item.enqueuedAtUnixNano = now.UnixNano()
	}
	if item.routingKeyID == "" && len(item.routingKey) > 0 {
		item.routingKeyID = string(item.routingKey)
	}
	bucket := q.bucketForItemLocked(item)
	bucket.append(item)
	q.itemCount++
	q.updateReadyBucketLocked(bucket, now.UnixNano())
	q.currentCompressedBytes += int64(item.compressedBytes)
	q.trackOldestEnqueuedAtLocked(item)
	snapshot := q.snapshotLockedAt(now)
	q.mu.Unlock()
	q.notifyLeaseWaiters()
	q.settings.telemetry.record(context.Background(), snapshot)
	return nil
}

func (q *centralQueue) lease(ctx context.Context) (*centralQueueLease, error) {
	return q.leaseWithPollInterval(ctx, centralQueueLeasePollInterval)
}

func (q *centralQueue) leaseWithPollInterval(ctx context.Context, pollInterval time.Duration) (*centralQueueLease, error) {
	return q.leaseWithPollIntervalAndAcquire(ctx, pollInterval, nil)
}

type centralQueueLeaseAcquireFunc func(queueCompressedBytes int64, window centralQueueWindow) (func(), bool)

func (q *centralQueue) leaseWithAcquire(ctx context.Context, acquire centralQueueLeaseAcquireFunc) (*centralQueueLease, error) {
	return q.leaseWithPollIntervalAndAcquire(ctx, centralQueueLeasePollInterval, acquire)
}

func (q *centralQueue) leaseWithPollIntervalAndAcquire(ctx context.Context, pollInterval time.Duration, acquire centralQueueLeaseAcquireFunc) (*centralQueueLease, error) {
	var ticker *time.Ticker
	var poll <-chan time.Time
	if pollInterval > 0 {
		ticker = time.NewTicker(pollInterval)
		poll = ticker.C
		defer ticker.Stop()
	}
	for {
		lease, err := q.tryLeaseWithAcquire(time.Now(), acquire)
		if lease != nil {
			return lease, nil
		}
		if err != nil && !errors.Is(err, errCentralQueueInflightFull) && !errors.Is(err, errCentralQueueConsumersFull) {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.notify:
		case <-poll:
		}
	}
}

func (q *centralQueue) tryLease(now time.Time) (*centralQueueLease, error) {
	return q.tryLeaseWithAcquire(now, nil)
}

func (q *centralQueue) tryLeaseWithAcquire(now time.Time, acquire centralQueueLeaseAcquireFunc) (*centralQueueLease, error) {
	for {
		q.mu.Lock()
		if len(q.ready) == 0 && q.itemCount == 0 {
			stopped := q.stopped
			q.mu.Unlock()
			if stopped {
				return nil, errCentralQueueStopped
			}
			return nil, nil
		}

		state := centralQueueSchedulerStateReady
		if len(q.ready) == 0 {
			readyWindowLimit := q.settings.maxReadyWindows
			if acquire != nil {
				readyWindowLimit = 1
			}
			state = q.prepareReadyWindowsLocked(now, readyWindowLimit)
		}
		if len(q.ready) == 0 {
			q.mu.Unlock()
			if state == centralQueueSchedulerStateInflightBytes {
				return nil, errCentralQueueInflightFull
			}
			return nil, nil
		}
		if acquire == nil {
			lease := q.leaseReadyWindowLocked()
			q.mu.Unlock()
			return lease, nil
		}
		queueCompressedBytes := q.currentCompressedBytes
		selectedWindow := q.ready[0]
		selectedWindow.routingKey = append([]byte(nil), selectedWindow.routingKey...)
		q.mu.Unlock()

		release, acquired := acquire(queueCompressedBytes, selectedWindow)
		if !acquired {
			return nil, errCentralQueueConsumersFull
		}

		q.mu.Lock()
		if len(q.ready) == 0 || !bytes.Equal(q.ready[0].routingKey, selectedWindow.routingKey) {
			q.mu.Unlock()
			if release != nil {
				release()
			}
			continue
		}
		lease := q.leaseReadyWindowLocked()
		q.mu.Unlock()
		if lease == nil {
			if release != nil {
				release()
			}
			continue
		}
		lease.consumerRelease = release
		return lease, nil
	}
}

func (q *centralQueue) prepareReadyWindowsLocked(now time.Time, readyWindowLimit int) centralQueueSchedulerState {
	if readyWindowLimit <= 0 {
		readyWindowLimit = q.settings.maxReadyWindows
	}
	state := centralQueueSchedulerStateWaiting
	for len(q.ready) < readyWindowLimit {
		targetCandidates, fallbackCandidates := q.collectReadyWindowCandidatesLocked(now)
		if len(targetCandidates) == 0 && len(fallbackCandidates) == 0 {
			return state
		}

		// SAW-7548: stale fallbacks (those whose oldest item has been
		// waiting longer than forceScheduleAge) jump ahead of fresh target
		// candidates to bound oldest_item_age. If a stale fallback is
		// blocked by the inflight cap we fall through to the original
		// target/fresh-fallback flow rather than bailing early; smaller
		// windows in later groups may still fit, and the original code's
		// blocked-target to InflightBytes signaling remains intact.
		staleFallbacks, freshFallbacks := q.splitFallbackByStaleness(fallbackCandidates, now)
		scheduledStale, _ := q.scheduleReadyWindowCandidatesLocked(staleFallbacks, now, readyWindowLimit)
		if scheduledStale {
			state = centralQueueSchedulerStateReady
			// Items were removed from buckets, so the previously-collected
			// targetCandidates / freshFallbacks indexes are stale and could
			// remove wrong items if reused. Recollect against current state.
			targetCandidates, fallbackCandidates = q.collectReadyWindowCandidatesLocked(now)
			_, freshFallbacks = q.splitFallbackByStaleness(fallbackCandidates, now)
		}
		if len(q.ready) >= readyWindowLimit {
			state = centralQueueSchedulerStateReady
			continue
		}

		scheduledTarget, blocked := q.scheduleReadyWindowCandidatesLocked(targetCandidates, now, readyWindowLimit)
		if !scheduledTarget && !scheduledStale && blocked {
			return centralQueueSchedulerStateInflightBytes
		}
		if scheduledTarget {
			state = centralQueueSchedulerStateReady
		}
		if len(q.ready) >= readyWindowLimit {
			state = centralQueueSchedulerStateReady
			continue
		}
		if scheduledTarget {
			_, fallbackCandidates = q.collectReadyWindowCandidatesLocked(now)
			_, freshFallbacks = q.splitFallbackByStaleness(fallbackCandidates, now)
		}

		scheduledFallback, blocked := q.scheduleReadyWindowCandidatesLocked(freshFallbacks, now, readyWindowLimit)
		if blocked {
			return centralQueueSchedulerStateInflightBytes
		}
		if !scheduledStale && !scheduledTarget && !scheduledFallback {
			return state
		}
		state = centralQueueSchedulerStateReady
	}
	return centralQueueSchedulerStateReadyWindowLimit
}

// splitFallbackByStaleness partitions fallback candidates into stale (older
// than forceScheduleAge) and fresh. Stale candidates jump ahead of fresh
// target candidates so that under continuous hot-key arrivals no lane can be
// indefinitely starved. With forceScheduleAge <= 0 or no maxBatchDelay set,
// all candidates remain "fresh" (legacy strict target-first scheduling).
func (q *centralQueue) splitFallbackByStaleness(candidates []centralQueueWindowCandidate, now time.Time) (stale, fresh []centralQueueWindowCandidate) {
	if q.settings.forceScheduleAge <= 0 || len(candidates) == 0 {
		return nil, candidates
	}
	cutoffUnixNano := now.Add(-q.settings.forceScheduleAge).UnixNano()
	for i := range candidates {
		c := &candidates[i]
		if c.window.oldestEnqueuedAt > 0 && c.window.oldestEnqueuedAt <= cutoffUnixNano {
			stale = append(stale, *c)
		} else {
			fresh = append(fresh, *c)
		}
	}
	return stale, fresh
}

func (q *centralQueue) bucketForItemLocked(item centralQueueItem) *centralQueueBucket {
	key := item.routingKeyID
	if key == "" && len(item.routingKey) > 0 {
		key = string(item.routingKey)
	}
	bucket := q.bucketsByKey[key]
	if bucket != nil {
		return bucket
	}
	bucket = newCentralQueueBucket(key, item.routingKey)
	q.bucketsByKey[key] = bucket
	q.buckets = append(q.buckets, bucket)
	return bucket
}

func (q *centralQueue) updateReadyBucketLocked(bucket *centralQueueBucket, nowUnixNano int64) {
	readyAtUnixNano := q.bucketReadyAtUnixNanoLocked(bucket, nowUnixNano)
	if readyAtUnixNano == 0 {
		q.removeReadyBucketLocked(bucket)
		return
	}
	bucket.readyAtUnixNano = readyAtUnixNano
	q.readySequence++
	bucket.readySequence = q.readySequence
	if bucket.readyHeapIndex >= 0 {
		heap.Fix(&q.readyBuckets, bucket.readyHeapIndex)
		return
	}
	heap.Push(&q.readyBuckets, bucket)
}

func (q *centralQueue) removeReadyBucketLocked(bucket *centralQueueBucket) {
	if bucket.readyHeapIndex < 0 {
		return
	}
	heap.Remove(&q.readyBuckets, bucket.readyHeapIndex)
}

func (q *centralQueue) bucketReadyAtUnixNanoLocked(bucket *centralQueueBucket, nowUnixNano int64) int64 {
	if bucket == nil || len(bucket.items) == 0 {
		return 0
	}
	if q.stopped {
		return nowUnixNano
	}

	var candidate centralQueueWindow
	selectedItems := 0
	candidateReadyAt := int64(1)
	var futureReadyAt int64
	for i := range bucket.items {
		item := &bucket.items[i]
		if item.nextAttemptUnixNano > nowUnixNano {
			if futureReadyAt == 0 || item.nextAttemptUnixNano < futureReadyAt {
				futureReadyAt = item.nextAttemptUnixNano
			}
			continue
		}
		if item.nextAttemptUnixNano > candidateReadyAt {
			candidateReadyAt = item.nextAttemptUnixNano
		}
		if selectedItems > 0 && q.windowWouldExceedLimit(candidate, item) {
			return candidateReadyAt
		}
		appendCentralQueueWindowItemStats(&candidate, item)
		selectedItems++
		if int64(candidate.compressedBytes) >= q.settings.targetCompressedBytes {
			return candidateReadyAt
		}
	}
	if selectedItems == 0 {
		return futureReadyAt
	}
	var readyAt int64
	if q.settings.maxBatchDelay <= 0 {
		readyAt = candidateReadyAt
	} else {
		readyAt = max(candidateReadyAt, candidate.oldestEnqueuedAt+q.settings.maxBatchDelay.Nanoseconds())
	}
	if futureReadyAt > 0 && futureReadyAt < readyAt {
		return futureReadyAt
	}
	return readyAt
}

func (q *centralQueue) collectReadyWindowCandidatesLocked(now time.Time) ([]centralQueueWindowCandidate, []centralQueueWindowCandidate) {
	nowUnixNano := now.UnixNano()
	targetCandidates := make([]centralQueueWindowCandidate, 0)
	fallbackCandidates := make([]centralQueueWindowCandidate, 0)
	for _, bucket := range q.readyBuckets {
		if bucket.readyAtUnixNano > nowUnixNano {
			continue
		}
		candidate, _ := q.buildWindowCandidateFromBucketLocked(bucket, now)
		if len(candidate.indexes) == 0 {
			continue
		}
		if candidate.window.flushReason != centralQueueFlushReasonTargetReached {
			fallbackCandidates = append(fallbackCandidates, candidate)
			continue
		}
		targetCandidates = append(targetCandidates, candidate)
	}
	sortCentralQueueWindowCandidates(targetCandidates)
	sortCentralQueueWindowCandidates(fallbackCandidates)
	return targetCandidates, fallbackCandidates
}

// collectWindowCandidatesLocked evaluates one candidate per routing-key bucket
// so a hot lane cannot fill the bounded ready backlog before other ready lanes
// get a turn.
func (q *centralQueue) collectWindowCandidatesLocked(now time.Time) ([]centralQueueWindowCandidate, []centralQueueWindowCandidate, bool) {
	targetCandidates := make([]centralQueueWindowCandidate, 0)
	fallbackCandidates := make([]centralQueueWindowCandidate, 0)
	hasReady := false
	for _, bucket := range q.buckets {
		candidate, bucketHasReady := q.buildWindowCandidateFromBucketLocked(bucket, now)
		hasReady = hasReady || bucketHasReady
		if len(candidate.indexes) == 0 {
			continue
		}
		if candidate.window.flushReason != centralQueueFlushReasonTargetReached {
			fallbackCandidates = append(fallbackCandidates, candidate)
			continue
		}
		targetCandidates = append(targetCandidates, candidate)
	}
	return targetCandidates, fallbackCandidates, hasReady
}

func (q *centralQueue) buildWindowCandidateFromBucketLocked(bucket *centralQueueBucket, now time.Time) (centralQueueWindowCandidate, bool) {
	nowUnixNano := now.UnixNano()
	bucket.candidateIndexes = bucket.candidateIndexes[:0]
	candidate := centralQueueWindowCandidate{
		bucket: bucket,
		window: centralQueueWindow{
			routingKey: bucket.routingKey,
		},
		indexes: bucket.candidateIndexes,
	}
	hasReady := false
	for i := range bucket.items {
		item := &bucket.items[i]
		if !q.stopped && item.nextAttemptUnixNano > nowUnixNano {
			continue
		}
		hasReady = true
		if len(candidate.indexes) > 0 && q.windowWouldExceedLimit(candidate.window, item) {
			candidate.window.flushReason = centralQueueFlushReasonHardCap
			break
		}
		appendCentralQueueWindowCandidateIndex(&candidate, item, i)
		if int64(candidate.window.compressedBytes) >= q.settings.targetCompressedBytes {
			candidate.window.flushReason = centralQueueFlushReasonTargetReached
			break
		}
	}
	if len(candidate.indexes) == 0 {
		bucket.candidateIndexes = candidate.indexes
		return centralQueueWindowCandidate{}, hasReady
	}
	if q.stopped {
		candidate.window.flushReason = centralQueueFlushReasonShutdown
		bucket.candidateIndexes = candidate.indexes
		return candidate, hasReady
	}
	switch candidate.window.flushReason {
	case centralQueueFlushReasonTargetReached:
	case "":
		switch {
		case q.settings.maxBatchDelay <= 0:
			candidate.window.flushReason = centralQueueFlushReasonMaxDelayLowTraffic
		default:
			oldest := time.Unix(0, candidate.window.oldestEnqueuedAt)
			if oldest.IsZero() || now.Sub(oldest) < q.settings.maxBatchDelay {
				bucket.candidateIndexes = candidate.indexes
				return centralQueueWindowCandidate{}, hasReady
			}
			candidate.window.flushReason = centralQueueFlushReasonMaxDelayLowTraffic
		}
	}
	bucket.candidateIndexes = candidate.indexes
	return candidate, hasReady
}

func sortCentralQueueWindowCandidates(candidates []centralQueueWindowCandidate) {
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].bucket
		right := candidates[j].bucket
		if left == nil || right == nil {
			return right != nil
		}
		if left.readyAtUnixNano != right.readyAtUnixNano {
			return left.readyAtUnixNano < right.readyAtUnixNano
		}
		return left.readySequence < right.readySequence
	})
}

func (q *centralQueue) scheduleReadyWindowCandidatesLocked(candidates []centralQueueWindowCandidate, now time.Time, readyWindowLimit int) (bool, bool) {
	if len(candidates) == 0 {
		return false, false
	}
	if readyWindowLimit <= 0 {
		readyWindowLimit = q.settings.maxReadyWindows
	}

	selected := make([]centralQueueWindowCandidate, 0, len(candidates))
	pendingInflightBytes := q.currentInflightBytes
	blocked := false
	for i := range candidates {
		candidate := &candidates[i]
		if len(q.ready)+len(selected) >= readyWindowLimit {
			break
		}
		if q.windowInflightBlockedWithBase(candidate.window, pendingInflightBytes) {
			blocked = true
			continue
		}
		selectedCandidate := *candidate
		selectedCandidate.indexes = append([]int(nil), candidate.indexes...)
		selectedCandidate.window.routingKey = append([]byte(nil), candidate.window.routingKey...)
		selected = append(selected, selectedCandidate)
		pendingInflightBytes += int64(candidate.window.uncompressedBytes)
	}

	if len(selected) == 0 {
		return false, blocked
	}

	for i := range selected {
		candidate := &selected[i]
		materializeWindowCandidateItemsLocked(candidate)
		q.ready = append(q.ready, candidate.window)
		q.currentInflightBytes += int64(candidate.window.uncompressedBytes)
	}

	for i := range selected {
		candidate := &selected[i]
		if !q.removeWindowFromBucketLocked(candidate.bucket, candidate.indexes, false) {
			q.updateReadyBucketLocked(candidate.bucket, now.UnixNano())
		}
	}

	snapshot := q.snapshotLocked()
	q.settings.telemetry.record(context.Background(), snapshot)
	return true, blocked
}

func appendCentralQueueWindowCandidateIndex(candidate *centralQueueWindowCandidate, item *centralQueueItem, index int) {
	candidate.indexes = append(candidate.indexes, index)
	appendCentralQueueWindowItemStats(&candidate.window, item)
}

func appendCentralQueueWindowItemStats(window *centralQueueWindow, item *centralQueueItem) {
	window.compressedBytes += item.compressedBytes
	window.uncompressedBytes += item.uncompressedBytes
	window.count += item.count
	if item.attempt > window.maxAttempt {
		window.maxAttempt = item.attempt
	}
	if window.oldestEnqueuedAt == 0 || item.enqueuedAtUnixNano < window.oldestEnqueuedAt {
		window.oldestEnqueuedAt = item.enqueuedAtUnixNano
	}
}

func materializeWindowCandidateItemsLocked(candidate *centralQueueWindowCandidate) {
	candidate.window.items = make([]centralQueueItem, 0, len(candidate.indexes))
	for _, index := range candidate.indexes {
		candidate.window.items = append(candidate.window.items, candidate.bucket.items[index])
	}
}

func (q *centralQueue) windowWouldExceedLimit(window centralQueueWindow, item *centralQueueItem) bool {
	return q.settings.maxUncompressedBatchBytes > 0 &&
		window.uncompressedBytes+item.uncompressedBytes > q.settings.maxUncompressedBatchBytes
}

func (q *centralQueue) windowInflightBlockedLocked(window centralQueueWindow) bool {
	return q.windowInflightBlockedWithBase(window, q.currentInflightBytes)
}

func (q *centralQueue) windowInflightBlockedWithBase(window centralQueueWindow, inflightBytes int64) bool {
	return q.settings.maxInflightUncompressedBytes > 0 &&
		inflightBytes+int64(window.uncompressedBytes) > q.settings.maxInflightUncompressedBytes
}

func (q *centralQueue) leaseReadyWindowLocked() *centralQueueLease {
	if len(q.ready) == 0 {
		return nil
	}

	window := q.ready[0]
	copy(q.ready, q.ready[1:])
	q.ready[len(q.ready)-1] = centralQueueWindow{}
	q.ready = q.ready[:len(q.ready)-1]
	for i := range window.items {
		q.untrackOldestEnqueuedAtLocked(window.items[i])
	}
	snapshot := q.snapshotLocked()
	q.settings.telemetry.record(context.Background(), snapshot)
	q.settings.telemetry.recordWindow(context.Background(), window, q.settings.targetCompressedBytes)
	lease := &centralQueueLease{
		queue:  q,
		window: window,
	}
	if len(window.items) > 0 {
		lease.item = window.items[0]
	}
	return lease
}

func (q *centralQueue) removeWindowFromBucketLocked(bucket *centralQueueBucket, indexes []int, untrack bool) bool {
	if bucket == nil || len(indexes) == 0 {
		return false
	}
	var onRemoved func(centralQueueItem)
	if untrack {
		onRemoved = func(item centralQueueItem) {
			q.untrackOldestEnqueuedAtLocked(item)
		}
	}
	removed := bucket.removeIndexes(indexes, onRemoved)
	q.itemCount -= removed
	if len(bucket.items) > 0 {
		return false
	}
	q.pruneBucketLocked(bucket)
	return true
}

func (q *centralQueue) pruneBucketLocked(bucket *centralQueueBucket) {
	q.removeReadyBucketLocked(bucket)
	delete(q.bucketsByKey, bucket.routingKeyID)
	for i := range q.buckets {
		if q.buckets[i] != bucket {
			continue
		}
		copy(q.buckets[i:], q.buckets[i+1:])
		q.buckets[len(q.buckets)-1] = nil
		q.buckets = q.buckets[:len(q.buckets)-1]
		break
	}
	bucket.candidateIndexes = bucket.candidateIndexes[:0]
}

func (l *centralQueueLease) done() {
	l.once.Do(func() {
		l.queue.mu.Lock()
		l.queue.currentInflightBytes -= int64(l.window.uncompressedBytes)
		l.queue.currentCompressedBytes -= int64(l.window.compressedBytes)
		snapshot := l.queue.snapshotLocked()
		l.queue.mu.Unlock()
		l.queue.notifyLeaseWaiters()
		l.queue.settings.telemetry.record(context.Background(), snapshot)
	})
}

func (l *centralQueueLease) releaseConsumer() {
	if l == nil || l.consumerRelease == nil {
		return
	}
	l.consumerReleaseOnce.Do(l.consumerRelease)
}

func (l *centralQueueLease) requeue(now time.Time) error {
	var err error
	l.once.Do(func() {
		l.queue.settings.telemetry.recordRetry(context.Background())

		l.queue.mu.Lock()
		l.queue.currentInflightBytes -= int64(l.window.uncompressedBytes)
		if l.queue.stopped {
			l.queue.currentCompressedBytes -= int64(l.window.compressedBytes)
			err = errCentralQueueStopped
		} else {
			for i := range l.window.items {
				item := l.window.items[i]
				nextAttempt := now.Add(centralQueueRetryDelayWithJitter(item))
				item.attempt++
				item.nextAttemptUnixNano = nextAttempt.UnixNano()
				bucket := l.queue.bucketForItemLocked(item)
				bucket.append(item)
				l.queue.itemCount++
				l.queue.updateReadyBucketLocked(bucket, now.UnixNano())
				l.queue.trackOldestEnqueuedAtLocked(item)
			}
		}
		snapshot := l.queue.snapshotLocked()
		l.queue.mu.Unlock()
		l.queue.notifyLeaseWaiters()
		l.queue.settings.telemetry.record(context.Background(), snapshot)
	})
	return err
}

func (l *centralQueueLease) deferReady(now time.Time) error {
	var err error
	l.once.Do(func() {
		l.queue.mu.Lock()
		l.queue.currentInflightBytes -= int64(l.window.uncompressedBytes)
		if l.queue.stopped {
			l.queue.currentCompressedBytes -= int64(l.window.compressedBytes)
			err = errCentralQueueStopped
		} else {
			nextAttemptUnixNano := now.Add(centralQueueLeasePollInterval).UnixNano()
			for i := range l.window.items {
				item := l.window.items[i]
				item.nextAttemptUnixNano = nextAttemptUnixNano
				bucket := l.queue.bucketForItemLocked(item)
				bucket.append(item)
				l.queue.itemCount++
				l.queue.updateReadyBucketLocked(bucket, now.UnixNano())
				l.queue.trackOldestEnqueuedAtLocked(item)
			}
		}
		snapshot := l.queue.snapshotLocked()
		l.queue.mu.Unlock()
		l.queue.notifyLeaseWaiters()
		l.queue.settings.telemetry.record(context.Background(), snapshot)
	})
	return err
}

func (q *centralQueue) stop() {
	q.mu.Lock()
	q.stopped = true
	nowUnixNano := time.Now().UnixNano()
	for _, bucket := range q.buckets {
		q.updateReadyBucketLocked(bucket, nowUnixNano)
	}
	q.mu.Unlock()
	q.notifyLeaseWaiters()
	q.settings.telemetry.stopObservingOldestItemAge()
	q.settings.telemetry.stopObservingSchedulerState()
	q.settings.telemetry.stopObservingConsumerDecision()
}

func (q *centralQueue) notifyLeaseWaiters() {
	if q == nil || q.notify == nil {
		return
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *centralQueue) compressedBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.currentCompressedBytes
}

func (q *centralQueue) inflightUncompressedBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.currentInflightBytes
}

func (q *centralQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.itemCount + q.readyItemCountLocked()
}

func (q *centralQueue) snapshotLocked() centralQueueSnapshot {
	return q.snapshotLockedAt(time.Now())
}

func (q *centralQueue) snapshotLockedAt(now time.Time) centralQueueSnapshot {
	return centralQueueSnapshot{
		compressedBytes:              q.currentCompressedBytes,
		compressedCapacity:           q.settings.maxCompressedBytes,
		items:                        int64(q.itemCount + q.readyItemCountLocked()),
		inflightUncompressed:         q.currentInflightBytes,
		inflightUncompressedCapacity: q.settings.maxInflightUncompressedBytes,
		oldestItemAgeMillis:          q.oldestItemAgeMillisLocked(now),
	}
}

func (q *centralQueue) readyItemCountLocked() int {
	count := 0
	for _, window := range q.ready {
		count += len(window.items)
	}
	return count
}

func (q *centralQueue) queuedItemsLocked() []centralQueueItem {
	items := make([]centralQueueItem, 0, q.itemCount)
	for _, bucket := range q.buckets {
		items = append(items, bucket.items...)
	}
	return items
}

func (q *centralQueue) queuedItems() []centralQueueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedItemsLocked()
}

func (q *centralQueue) schedulerSnapshot() centralQueueSchedulerSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.schedulerSnapshotLocked(time.Now())
}

func (q *centralQueue) schedulerSnapshotLocked(now time.Time) centralQueueSchedulerSnapshot {
	snapshot := centralQueueSchedulerSnapshot{
		readyWindows:     int64(len(q.ready)),
		readyWindowLimit: int64(q.settings.maxReadyWindows),
		state:            centralQueueSchedulerStateWaiting,
	}
	for _, window := range q.ready {
		snapshot.readyUncompressed += int64(window.uncompressedBytes)
	}
	switch {
	case len(q.ready) >= q.settings.maxReadyWindows:
		snapshot.state = centralQueueSchedulerStateReadyWindowLimit
	case len(q.ready) > 0:
		snapshot.state = centralQueueSchedulerStateReady
	case q.itemCount == 0 && q.stopped:
		snapshot.state = centralQueueSchedulerStateStopped
	case q.itemCount == 0:
		snapshot.state = centralQueueSchedulerStateQueueEmpty
	default:
		targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now)
		snapshot.readyLanes = int64(len(targetCandidates) + len(fallbackCandidates))
		if !hasReady || len(targetCandidates)+len(fallbackCandidates) == 0 {
			snapshot.state = centralQueueSchedulerStateWaiting
			return snapshot
		}
		for i := range targetCandidates {
			candidate := &targetCandidates[i]
			if !q.windowInflightBlockedLocked(candidate.window) {
				snapshot.state = centralQueueSchedulerStateReady
				return snapshot
			}
		}
		if len(targetCandidates) > 0 {
			snapshot.state = centralQueueSchedulerStateInflightBytes
			return snapshot
		}
		for i := range fallbackCandidates {
			candidate := &fallbackCandidates[i]
			if !q.windowInflightBlockedLocked(candidate.window) {
				snapshot.state = centralQueueSchedulerStateReady
				return snapshot
			}
		}
		snapshot.state = centralQueueSchedulerStateInflightBytes
	}
	return snapshot
}

func (q *centralQueue) oldestItemAgeMillis() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.oldestItemAgeMillisLocked(time.Now())
}

func (q *centralQueue) oldestItemAgeMillisLocked(now time.Time) int64 {
	q.pruneOldestEnqueuedAtLocked()
	if len(q.oldestEnqueuedAt) == 0 {
		return 0
	}
	age := now.Sub(time.Unix(0, q.oldestEnqueuedAt[0]))
	if age <= 0 {
		return 0
	}
	return age.Milliseconds()
}

func (q *centralQueue) trackOldestEnqueuedAtLocked(item centralQueueItem) {
	if item.enqueuedAtUnixNano == 0 {
		return
	}
	if q.enqueuedAtCounts == nil {
		q.enqueuedAtCounts = map[int64]int{}
	}
	if q.enqueuedAtHeapEntries == nil {
		q.enqueuedAtHeapEntries = map[int64]struct{}{}
	}
	if _, ok := q.enqueuedAtHeapEntries[item.enqueuedAtUnixNano]; !ok {
		heap.Push(&q.oldestEnqueuedAt, item.enqueuedAtUnixNano)
		q.enqueuedAtHeapEntries[item.enqueuedAtUnixNano] = struct{}{}
	}
	q.enqueuedAtCounts[item.enqueuedAtUnixNano]++
}

func (q *centralQueue) untrackOldestEnqueuedAtLocked(item centralQueueItem) {
	if item.enqueuedAtUnixNano == 0 || q.enqueuedAtCounts == nil {
		return
	}
	count := q.enqueuedAtCounts[item.enqueuedAtUnixNano]
	if count <= 1 {
		delete(q.enqueuedAtCounts, item.enqueuedAtUnixNano)
	} else {
		q.enqueuedAtCounts[item.enqueuedAtUnixNano] = count - 1
	}
	q.pruneOldestEnqueuedAtLocked()
}

func (q *centralQueue) pruneOldestEnqueuedAtLocked() {
	for len(q.oldestEnqueuedAt) > 0 && q.enqueuedAtCounts[q.oldestEnqueuedAt[0]] == 0 {
		enqueuedAt := heap.Pop(&q.oldestEnqueuedAt).(int64)
		delete(q.enqueuedAtHeapEntries, enqueuedAt)
	}
}

type centralQueueEnqueuedAtHeap []int64

func (h centralQueueEnqueuedAtHeap) Len() int {
	return len(h)
}

func (h centralQueueEnqueuedAtHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h centralQueueEnqueuedAtHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *centralQueueEnqueuedAtHeap) Push(x any) {
	*h = append(*h, x.(int64))
}

func (h *centralQueueEnqueuedAtHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func centralQueueRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	shift := min(attempt, 6)
	delay := centralQueueInitialRetryDelay * time.Duration(1<<shift)
	return min(delay, centralQueueMaxRetryDelay)
}

func centralQueueRetryDelayWithJitter(item centralQueueItem) time.Duration {
	delay := centralQueueRetryDelay(item.attempt)
	jitterLimit := centralQueueRetryJitterLimit(delay)
	if jitterLimit <= 0 {
		return delay
	}
	hashInput := make([]byte, len(item.routingKey)+16)
	copy(hashInput, item.routingKey)
	binary.BigEndian.PutUint64(hashInput[len(item.routingKey):], uint64(item.enqueuedAtUnixNano))
	binary.BigEndian.PutUint64(hashInput[len(item.routingKey)+8:], uint64(item.attempt))
	jitter := time.Duration(crc32.ChecksumIEEE(hashInput) % uint32(jitterLimit+1))
	return delay + jitter
}

func centralQueueRetryJitterLimit(delay time.Duration) time.Duration {
	return min(delay/10, centralQueueMaxRetryJitter)
}

func centralQueueLaneRoutingKey(signal signalKind, routingKey []byte, laneCount int) []byte {
	if laneCount <= 0 {
		return append([]byte(nil), routingKey...)
	}
	lane := centralQueueLaneIndex(signal, routingKey, laneCount)
	return centralQueueLaneKey(signal, lane, 0)
}

func centralQueueBalancedLaneRoutingKeyForLoadBalancerLane(lb *loadBalancer, signal signalKind, lane uint32) []byte {
	if lb == nil {
		return centralQueueLaneKey(signal, lane, 0)
	}
	lb.updateLock.RLock()
	defer lb.updateLock.RUnlock()
	return centralQueueBalancedLaneRoutingKeyForRing(lb.ring, signal, lane)
}

func centralQueueBalancedLaneRoutingKeyForRing(ring *hashRing, signal signalKind, lane uint32) []byte {
	if ring == nil {
		return centralQueueLaneKey(signal, lane, 0)
	}
	cacheKey := centralQueueBalancedLaneCacheKey{signal: signal, lane: lane}
	if routingKey, ok := ring.balancedLaneRoutingKeys.Load(cacheKey); ok {
		return routingKey.([]byte)
	}
	routingKey := centralQueueBalancedLaneRoutingKeyForRingUncached(ring, signal, lane)
	actual, _ := ring.balancedLaneRoutingKeys.LoadOrStore(cacheKey, routingKey)
	return actual.([]byte)
}

type centralQueueBalancedLaneCacheKey struct {
	signal signalKind
	lane   uint32
}

func centralQueueBalancedLaneRoutingKeyForRingUncached(ring *hashRing, signal signalKind, lane uint32) []byte {
	base := centralQueueLaneKey(signal, lane, 0)
	var endpoints []string
	if ring != nil {
		endpoints = ring.endpoints
	}
	if len(endpoints) <= 1 {
		return base
	}
	target := endpoints[int(lane)%len(endpoints)]
	if endpointWithPort(ring.endpointFor(base)) == endpointWithPort(target) {
		return base
	}
	for salt := uint32(1); salt <= 1024; salt++ {
		candidate := centralQueueLaneKey(signal, lane, salt)
		if endpointWithPort(ring.endpointFor(candidate)) == endpointWithPort(target) {
			return candidate
		}
	}
	return base
}

func centralQueueLaneIndex(signal signalKind, routingKey []byte, laneCount int) uint32 {
	hashInput := make([]byte, len(routingKey)+len(signal)+1)
	copy(hashInput, string(signal))
	hashInput[len(signal)] = 0
	copy(hashInput[len(signal)+1:], routingKey)
	return crc32.ChecksumIEEE(hashInput) % uint32(laneCount)
}

func centralQueueLaneKey(signal signalKind, lane, salt uint32) []byte {
	laneRoutingKey := make([]byte, len(signal)+1+4)
	if salt > 0 {
		laneRoutingKey = make([]byte, len(signal)+1+4+4)
	}
	copy(laneRoutingKey, string(signal))
	laneRoutingKey[len(signal)] = 0
	binary.BigEndian.PutUint32(laneRoutingKey[len(signal)+1:], lane)
	if salt > 0 {
		binary.BigEndian.PutUint32(laneRoutingKey[len(signal)+1+4:], salt)
	}
	return laneRoutingKey
}
