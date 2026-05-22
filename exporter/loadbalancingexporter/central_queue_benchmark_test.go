// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCentralQueueCollectManyLanesManyItems(b *testing.B) {
	const laneCount = 64
	const itemsPerLane = 1024
	now := time.Unix(1_700_000_000, 0)

	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1 << 30,
		maxInflightUncompressedBytes: 1 << 30,
		maxUncompressedBatchBytes:    1 << 20,
		targetCompressedBytes:        256 << 10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              centralQueueReadyWindowLimit(defaultCentralQueueNumConsumers),
	})

	for lane := range laneCount {
		routingKey := fmt.Appendf(nil, "lane-%02d", lane)
		for range itemsPerLane {
			err := q.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        routingKey,
				compressedBytes:   512,
				uncompressedBytes: 4096,
				count:             1,
			}, now)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.mu.Lock()
		targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now.Add(2 * time.Second))
		q.mu.Unlock()
		if !hasReady || len(targetCandidates)+len(fallbackCandidates) == 0 {
			b.Fatal("expected ready candidates")
		}
	}
}
