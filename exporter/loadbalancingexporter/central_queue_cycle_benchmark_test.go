// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"
)

// BenchmarkCentralQueueFullCycle includes enqueue, scheduling, leasing, simulated
// delivery, return on failure, and completion. Each iteration starts with an
// empty queue so the baseline and candidate process the same records.
func BenchmarkCentralQueueFullCycle(b *testing.B) {
	const records = 512
	const compressedBytes = 512
	const uncompressedBytes = 4096

	for _, scenario := range []struct {
		name              string
		lanes             int
		hotPercent        int
		batchItems        int
		deferredWindows   int
		requeuedWindows   int
		shutdownBeforeRun bool
	}{
		{name: "healthy_balanced_batch_8", lanes: 64, batchItems: 8},
		{name: "healthy_hot_batch_64", lanes: 64, hotPercent: 80, batchItems: 64},
		{name: "healthy_hot_batch_256", lanes: 64, hotPercent: 80, batchItems: 256},
		{name: "unavailable_hot_batch_64", lanes: 64, hotPercent: 80, batchItems: 64, deferredWindows: 4, requeuedWindows: 4},
		{name: "unavailable_hot_batch_256", lanes: 64, hotPercent: 80, batchItems: 256, deferredWindows: 2, requeuedWindows: 2},
		{name: "shutdown_hot_batch_64", lanes: 64, hotPercent: 80, batchItems: 64, shutdownBeforeRun: true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			laneKeys := make([][]byte, scenario.lanes)
			// Keep the last 1024 drain samples without growing a slice in the timed loop.
			var drainDurations [1024]time.Duration
			sampledCycles := 0
			deferredRecords, requeuedRecords := 0, 0
			for lane := range laneKeys {
				laneKeys[lane] = fmt.Appendf(nil, "lane-%02d", lane)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				now := time.Unix(1_700_000_000, 0)
				q := newCentralQueue(centralQueueSettings{
					maxCompressedBytes:           records * compressedBytes,
					maxInflightUncompressedBytes: records * uncompressedBytes,
					maxUncompressedBatchBytes:    scenario.batchItems * uncompressedBytes,
					targetCompressedBytes:        int64(scenario.batchItems * compressedBytes),
					maxBatchDelay:                time.Millisecond,
					maxReadyWindows:              scenario.lanes,
				})
				for record := range records {
					lane := record % scenario.lanes
					if scenario.hotPercent > 0 {
						if record%100 < scenario.hotPercent {
							lane = 0
						} else {
							lane = 1 + record%(scenario.lanes-1)
						}
					}
					if err := q.enqueueAt(centralQueueItem{
						signal:            signalKindLogs,
						routingKey:        laneKeys[lane],
						compressedBytes:   compressedBytes,
						uncompressedBytes: uncompressedBytes,
						count:             1,
					}, now); err != nil {
						b.Fatal(err)
					}
				}
				if scenario.shutdownBeforeRun {
					q.stop()
					now = time.Now()
				}

				drainStart := time.Now()
				completed, deferred, requeued := 0, 0, 0
				for attempts := 0; completed < records; attempts++ {
					if attempts > records*4 {
						b.Fatalf("queue stalled after completing %d of %d records", completed, records)
					}
					lease, err := q.tryLease(now.Add(time.Second))
					if err != nil {
						b.Fatal(err)
					}
					if lease == nil {
						now = now.Add(time.Second)
						continue
					}
					if bytes.Equal(lease.window.routingKey, laneKeys[0]) && deferred < scenario.deferredWindows {
						deferredRecords += len(lease.window.items)
						if err := lease.deferReady(now); err != nil {
							b.Fatal(err)
						}
						deferred++
						now = now.Add(time.Second)
						continue
					}
					if bytes.Equal(lease.window.routingKey, laneKeys[0]) && requeued < scenario.requeuedWindows {
						requeuedRecords += len(lease.window.items)
						if err := lease.requeue(now); err != nil {
							b.Fatal(err)
						}
						requeued++
						now = now.Add(time.Second)
						continue
					}
					completed += len(lease.window.items)
					lease.done()
				}
				if q.len() != 0 || q.compressedBytes() != 0 || q.inflightUncompressedBytes() != 0 {
					b.Fatal("queue was not empty after completing all records")
				}
				drainDurations[sampledCycles%len(drainDurations)] = time.Since(drainStart)
				sampledCycles++
			}
			b.StopTimer()
			samples := drainDurations[:min(sampledCycles, len(drainDurations))]
			slices.Sort(samples)
			b.ReportMetric(float64(samples[(len(samples)*99-1)/100].Nanoseconds()), "p99-drain-ns")
			b.ReportMetric(float64(deferredRecords)/float64(b.N), "deferred-records/op")
			b.ReportMetric(float64(requeuedRecords)/float64(b.N), "requeued-records/op")
			b.ReportMetric(float64(b.N*records)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
