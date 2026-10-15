// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"time"

	"go.uber.org/zap"
)

func (q *centralQueue) shutdownTimeout() time.Duration {
	if q.settings.shutdownTimeout > 0 {
		return q.settings.shutdownTimeout
	}
	return defaultCentralQueueShutdownTimeout
}

// drain seals admission and flushes partially filled batches while consumers
// continue delivery and transient-error retries. In-flight bytes remain charged
// until delivery completes, so an empty pending list alone is not sufficient.
func (q *centralQueue) drain(ctx context.Context) error {
	q.mu.Lock()
	q.draining = true
	now := time.Now().UnixNano()
	for _, bucket := range q.buckets {
		q.updateReadyBucketLocked(bucket, now)
	}
	q.mu.Unlock()
	q.notifyLeaseWaiters()

	ticker := time.NewTicker(centralQueueLeasePollInterval)
	defer ticker.Stop()
	for {
		q.mu.Lock()
		empty := q.itemCount == 0 && len(q.ready) == 0 && q.currentCompressedBytes == 0 && q.currentInflightBytes == 0
		q.mu.Unlock()
		if empty {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			// Recheck completion under the queue lock before reporting a timeout.
		case <-ticker.C:
		}
	}
}

// stopAndDiscard seals the queue and discards only unleased work. Consumers
// retain their leases: success is still delivery, while cancellation or a
// stopped requeue counts that lease exactly once through its terminal call.
func (q *centralQueue) stopAndDiscard() int64 {
	q.mu.Lock()
	q.stopped = true
	q.discardOnStop = true
	var records int64
	for _, bucket := range q.buckets {
		for _, item := range bucket.items {
			records += int64(item.count)
			q.currentCompressedBytes -= int64(item.compressedBytes)
		}
	}
	for _, window := range q.ready {
		records += int64(window.count)
		q.currentCompressedBytes -= int64(window.compressedBytes)
		q.currentInflightBytes -= int64(window.uncompressedBytes)
	}
	q.buckets = nil
	clear(q.bucketsByKey)
	q.readyBuckets = nil
	q.ready = nil
	q.itemCount = 0
	q.oldestEnqueuedAt = nil
	clear(q.enqueuedAtCounts)
	clear(q.enqueuedAtHeapEntries)
	snapshot := q.snapshotLocked()
	q.mu.Unlock()
	q.settings.telemetry.record(context.Background(), snapshot)
	q.stop()
	return records
}

func (q *centralQueue) recordShutdownDiscard(records int64) {
	if records <= 0 {
		return
	}
	q.settings.telemetry.recordShutdownDropped(context.Background(), records)
	if q.settings.logger != nil {
		q.settings.logger.Warn("central queue records discarded at shutdown deadline",
			zap.Int64("dropped_items", records), zap.String("reason", "forced"))
	}
}
