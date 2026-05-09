// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"hash/crc32"
	"sync"
	"time"
)

var (
	errCentralQueueFull         = errors.New("central queue is full")
	errCentralQueueStopped      = errors.New("central queue is stopped")
	errCentralQueueItemTooLarge = errors.New("central queue item exceeds hard limits")
)

type centralQueueSignal string

const (
	centralQueueSignalLogs    centralQueueSignal = "logs"
	centralQueueSignalMetrics centralQueueSignal = "metrics"
)

const defaultCentralQueueBackendID = "default"

func centralQueueLaneID(identifier []byte, laneCount int) uint32 {
	if laneCount <= 0 {
		return 0
	}
	return crc32.ChecksumIEEE(identifier) % uint32(laneCount)
}

type centralQueueKey struct {
	signal    centralQueueSignal
	backendID string
	laneID    uint32
}

type centralQueueFlushReason string

const (
	centralQueueFlushReasonTargetReached      centralQueueFlushReason = "target_reached"
	centralQueueFlushReasonHardCap            centralQueueFlushReason = "hard_cap"
	centralQueueFlushReasonMaxDelayLowTraffic centralQueueFlushReason = "max_delay_low_traffic"
	centralQueueFlushReasonShutdown           centralQueueFlushReason = "shutdown"
)

type centralQueueItem struct {
	key               centralQueueKey
	payload           []byte
	compressedBytes   int
	uncompressedBytes int
	itemCount         int
	enqueuedAt        time.Time
}

type centralQueueWindow struct {
	key               centralQueueKey
	items             []centralQueueItem
	compressedBytes   int
	uncompressedBytes int
	itemCount         int
	oldestEnqueuedAt  time.Time
	flushReason       centralQueueFlushReason
}

type centralQueueSettings struct {
	capacityBytes int64
	batching      CentralQueueBatchingConfig
	now           func() time.Time
}

type centralQueue struct {
	settings centralQueueSettings

	mu          sync.Mutex
	notEmpty    *sync.Cond
	queues      map[centralQueueKey][]centralQueueItem
	activeKeys  []centralQueueKey
	activeKey   map[centralQueueKey]bool
	queuedBytes int64
	stopped     bool
}

func centralQueueSettingsFromConfig(cfg CentralQueueConfig) centralQueueSettings {
	cfg.ApplyDefaults()
	return centralQueueSettings{
		capacityBytes: cfg.CapacityBytes,
		batching:      cfg.RequestBatching,
		now:           time.Now,
	}
}

func newCentralQueue(settings centralQueueSettings) *centralQueue {
	if settings.now == nil {
		settings.now = time.Now
	}
	q := &centralQueue{
		settings:  settings,
		queues:    make(map[centralQueueKey][]centralQueueItem),
		activeKey: make(map[centralQueueKey]bool),
	}
	q.notEmpty = sync.NewCond(&q.mu)
	return q
}

func (q *centralQueue) enqueue(ctx context.Context, item centralQueueItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if item.enqueuedAt.IsZero() {
		item.enqueuedAt = q.settings.now()
	}
	if err := q.validateItem(item); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return errCentralQueueStopped
	}
	if q.queuedBytes+int64(item.compressedBytes) > q.settings.capacityBytes {
		return errCentralQueueFull
	}
	q.pushBackLocked(item)
	q.queuedBytes += int64(item.compressedBytes)
	q.notEmpty.Signal()
	return nil
}

func (q *centralQueue) validateItem(item centralQueueItem) error {
	if item.compressedBytes <= 0 || item.uncompressedBytes <= 0 || item.itemCount <= 0 {
		return errCentralQueueItemTooLarge
	}
	batching := q.settings.batching
	if item.compressedBytes > int(batching.MaxCompressedBytes) ||
		item.uncompressedBytes > int(batching.MaxUncompressedBytes) ||
		item.itemCount > batching.MaxMergedItems {
		return errCentralQueueItemTooLarge
	}
	return nil
}

func (q *centralQueue) nextWindow(ctx context.Context) (centralQueueWindow, bool) {
	done := q.broadcastOnContextDone(ctx)
	defer close(done)

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		for len(q.activeKeys) == 0 && !q.stopped && ctx.Err() == nil {
			q.notEmpty.Wait()
		}
		if ctx.Err() != nil || len(q.activeKeys) == 0 {
			return centralQueueWindow{}, false
		}

		now := q.settings.now()
		if readyIndex := q.readyKeyIndexLocked(now); readyIndex >= 0 {
			window := q.popWindowLocked(readyIndex)
			q.queuedBytes -= int64(window.compressedBytes)
			if len(q.activeKeys) > 0 {
				q.notEmpty.Signal()
			}
			return window, true
		}

		wait := q.nextReadyDelayLocked(now)
		timer := time.AfterFunc(wait, func() {
			q.mu.Lock()
			q.notEmpty.Broadcast()
			q.mu.Unlock()
		})
		q.notEmpty.Wait()
		timer.Stop()
	}
}

func (q *centralQueue) buildWindowLocked(key centralQueueKey, items []centralQueueItem) (centralQueueWindow, []centralQueueItem) {
	batching := q.settings.batching
	window := centralQueueWindow{key: key, items: make([]centralQueueItem, 0, len(items))}

	for len(items) > 0 {
		item := items[0]
		if len(window.items) > 0 && exceedsWindowLimits(window, item, batching) {
			window.flushReason = centralQueueFlushReasonHardCap
			break
		}
		items = items[1:]
		window.items = append(window.items, item)
		window.compressedBytes += item.compressedBytes
		window.uncompressedBytes += item.uncompressedBytes
		window.itemCount += item.itemCount
		if window.oldestEnqueuedAt.IsZero() || item.enqueuedAt.Before(window.oldestEnqueuedAt) {
			window.oldestEnqueuedAt = item.enqueuedAt
		}
		if window.compressedBytes >= int(batching.TargetCompressedBytes) {
			window.flushReason = centralQueueFlushReasonTargetReached
			break
		}
	}
	if window.flushReason == "" {
		window.flushReason = centralQueueFlushReasonMaxDelayLowTraffic
	}

	return window, items
}

func (q *centralQueue) readyKeyIndexLocked(now time.Time) int {
	if q.stopped {
		return 0
	}
	for i, key := range q.activeKeys {
		if q.laneReadyLocked(key, now) {
			return i
		}
	}
	return -1
}

func (q *centralQueue) laneReadyLocked(key centralQueueKey, now time.Time) bool {
	items := q.queues[key]
	if len(items) == 0 {
		return false
	}
	window, remaining := q.buildWindowLocked(key, items)
	if window.compressedBytes >= int(q.settings.batching.TargetCompressedBytes) {
		return true
	}
	if len(remaining) > 0 {
		return true
	}
	return now.Sub(window.oldestEnqueuedAt) >= q.settings.batching.MaxDelay
}

func (q *centralQueue) nextReadyDelayLocked(now time.Time) time.Duration {
	delay := q.settings.batching.MaxDelay
	for _, key := range q.activeKeys {
		items := q.queues[key]
		if len(items) == 0 {
			continue
		}
		untilReady := q.settings.batching.MaxDelay - now.Sub(items[0].enqueuedAt)
		if untilReady <= 0 {
			return 0
		}
		if untilReady < delay {
			delay = untilReady
		}
	}
	return delay
}

func (q *centralQueue) popWindowLocked(index int) centralQueueWindow {
	key := q.activeKeys[index]
	items := q.queues[key]
	window, remaining := q.buildWindowLocked(key, items)
	if q.stopped && window.compressedBytes < int(q.settings.batching.TargetCompressedBytes) {
		window.flushReason = centralQueueFlushReasonShutdown
	}
	q.queues[key] = remaining
	if len(remaining) == 0 {
		delete(q.queues, key)
		delete(q.activeKey, key)
		q.activeKeys = append(q.activeKeys[:index], q.activeKeys[index+1:]...)
	} else {
		q.activeKeys = append(q.activeKeys[:index], q.activeKeys[index+1:]...)
		q.activeKeys = append(q.activeKeys, key)
	}
	return window
}

func exceedsWindowLimits(window centralQueueWindow, item centralQueueItem, batching CentralQueueBatchingConfig) bool {
	return window.compressedBytes+item.compressedBytes > int(batching.MaxCompressedBytes) ||
		window.uncompressedBytes+item.uncompressedBytes > int(batching.MaxUncompressedBytes) ||
		window.itemCount+item.itemCount > batching.MaxMergedItems
}

func (q *centralQueue) requeueFront(window centralQueueWindow) {
	if len(window.items) == 0 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	existing := q.queues[window.key]
	requeued := make([]centralQueueItem, 0, len(window.items)+len(existing))
	requeued = append(requeued, window.items...)
	requeued = append(requeued, existing...)
	q.queues[window.key] = requeued
	if !q.activeKey[window.key] {
		q.activeKey[window.key] = true
		q.activeKeys = append([]centralQueueKey{window.key}, q.activeKeys...)
	}
	q.queuedBytes += int64(window.compressedBytes)
	q.notEmpty.Signal()
}

func (q *centralQueue) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stopped = true
	q.notEmpty.Broadcast()
}

func (q *centralQueue) currentBytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedBytes
}

func (q *centralQueue) activeLaneCount(signal centralQueueSignal) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	count := 0
	for key := range q.queues {
		if key.signal == signal {
			count++
		}
	}
	return count
}

func (q *centralQueue) oldestItemAge(signal centralQueueSignal, now time.Time) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()

	var oldest time.Time
	for key, items := range q.queues {
		if key.signal != signal || len(items) == 0 {
			continue
		}
		enqueuedAt := items[0].enqueuedAt
		if oldest.IsZero() || enqueuedAt.Before(oldest) {
			oldest = enqueuedAt
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return now.Sub(oldest)
}

func (q *centralQueue) pushBackLocked(item centralQueueItem) {
	q.queues[item.key] = append(q.queues[item.key], item)
	if q.activeKey[item.key] {
		return
	}
	q.activeKey[item.key] = true
	q.activeKeys = append(q.activeKeys, item.key)
}

func (q *centralQueue) broadcastOnContextDone(ctx context.Context) chan struct{} {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.notEmpty.Broadcast()
			q.mu.Unlock()
		case <-done:
		}
	}()
	return done
}
