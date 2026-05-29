// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueBalancedLaneRoutingKeyColdMissUsesCachedEndpointSnapshot(t *testing.T) {
	endpoints := centralQueueBenchmarkBigIDEndpoints()
	ring := newHashRing(endpoints)
	lane := centralQueueBenchmarkBaseHitLane(t, ring)

	allocs := testing.AllocsPerRun(100, func() {
		coldRing := &hashRing{
			items:     ring.items,
			endpoints: ring.endpoints,
		}
		_ = centralQueueBalancedLaneRoutingKeyForRing(coldRing, signalKindLogs, lane)
	})

	require.LessOrEqual(t, allocs, 16.0)
}

func TestCentralQueueBalancedLaneRoutingKeyCacheHitAvoidsAllocations(t *testing.T) {
	endpoints := centralQueueBenchmarkBigIDEndpoints()
	ring := newHashRing(endpoints)
	lane := centralQueueBenchmarkBaseHitLane(t, ring)

	_ = centralQueueBalancedLaneRoutingKeyForRing(ring, signalKindLogs, lane)
	allocs := testing.AllocsPerRun(100, func() {
		_ = centralQueueBalancedLaneRoutingKeyForRing(ring, signalKindLogs, lane)
	})

	require.LessOrEqual(t, allocs, 1.0)
}

func TestCentralQueueBalancedLaneRoutingKeyCacheMatchesUncached(t *testing.T) {
	endpoints := centralQueueBenchmarkBigIDEndpoints()
	ring := newHashRing(endpoints)

	for _, signal := range []signalKind{signalKindLogs, signalKindMetrics} {
		for lane := range uint32(512) {
			expected := append([]byte(nil), centralQueueBalancedLaneRoutingKeyForRingUncached(ring, signal, lane)...)
			require.Equal(t, expected, centralQueueBalancedLaneRoutingKeyForRing(ring, signal, lane))
			require.Equal(t, expected, centralQueueBalancedLaneRoutingKeyForRing(ring, signal, lane))
		}
	}
}

func TestCentralQueueLaneIndexAvoidsAllocations(t *testing.T) {
	routingKey := []byte("0123456789abcdef")

	for _, signal := range []signalKind{signalKindLogs, signalKindMetrics} {
		allocs := testing.AllocsPerRun(100, func() {
			centralQueueLaneIndexBenchmarkSink = centralQueueLaneIndex(signal, routingKey, 256)
		})

		require.Zero(t, allocs)
	}
}

func TestCentralQueueLaneIndexMatchesConcatenatedHash(t *testing.T) {
	routingKey := []byte("0123456789abcdef")

	for _, signal := range []signalKind{signalKindLogs, signalKindMetrics} {
		hashInput := make([]byte, len(routingKey)+len(signal)+1)
		copy(hashInput, string(signal))
		hashInput[len(signal)] = 0
		copy(hashInput[len(signal)+1:], routingKey)

		require.Equal(t, crc32.ChecksumIEEE(hashInput)%256, centralQueueLaneIndex(signal, routingKey, 256))
	}
}

func BenchmarkCentralQueueBalancedLaneRoutingKeyBigIDTopology(b *testing.B) {
	endpoints := centralQueueBenchmarkBigIDEndpoints()
	ring := newHashRing(endpoints)
	const laneCount = 256

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_ = centralQueueBalancedLaneRoutingKeyForRing(ring, signalKindLogs, uint32(i%laneCount))
	}
}

var centralQueueLaneIndexBenchmarkSink uint32

func BenchmarkCentralQueueLaneIndex(b *testing.B) {
	routingKeys := [][]byte{
		[]byte("0123456789abcdef"),
		[]byte("fedcba9876543210"),
		[]byte("bigid-production-mt3-a"),
		[]byte("bigid-production-mt3-b"),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		centralQueueLaneIndexBenchmarkSink = centralQueueLaneIndex(signalKindLogs, routingKeys[i&3], 256)
	}
}

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

func centralQueueBenchmarkBigIDEndpoints() []string {
	const endpointCount = 240
	endpoints := make([]string, endpointCount)
	for i := range endpointCount {
		endpoints[i] = fmt.Sprintf("10.%d.%d.%d:4317", 141+(i/65_536)%256, (i/256)%256, i%256)
	}
	return endpoints
}

func centralQueueBenchmarkBaseHitLane(tb testing.TB, ring *hashRing) uint32 {
	tb.Helper()

	for lane := range uint32(100_000) {
		base := centralQueueLaneKey(signalKindLogs, lane, 0)
		target := ring.endpoints[int(lane)%len(ring.endpoints)]
		if endpointWithPort(ring.endpointFor(base)) == endpointWithPort(target) {
			return lane
		}
	}

	require.FailNow(tb, "failed to find base-hit lane")
	return 0
}
