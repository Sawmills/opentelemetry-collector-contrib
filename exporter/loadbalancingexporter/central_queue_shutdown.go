// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"time"
)

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
