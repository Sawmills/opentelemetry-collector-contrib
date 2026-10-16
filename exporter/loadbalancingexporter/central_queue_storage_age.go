// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import "container/heap"

// Every retained payload owns one entry. Indexed removal keeps this heap bounded
// by pending payload count even when acknowledgements arrive out of order.
type centralStoreEnqueuedAt struct {
	unixNano int64
	index    int
}
type centralStoreAgeHeap []*centralStoreEnqueuedAt

func (h centralStoreAgeHeap) Len() int           { return len(h) }
func (h centralStoreAgeHeap) Less(i, j int) bool { return h[i].unixNano < h[j].unixNano }
func (h centralStoreAgeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *centralStoreAgeHeap) Push(value any) {
	entry := value.(*centralStoreEnqueuedAt)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *centralStoreAgeHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	entry.index = -1
	return entry
}

func (h *centralStoreAgeHeap) track(entry *centralStoreEnqueuedAt) {
	if entry.index < 0 {
		heap.Push(h, entry)
	}
}
func (h *centralStoreAgeHeap) remove(entry *centralStoreEnqueuedAt) { heap.Remove(h, entry.index) }
func (h centralStoreAgeHeap) oldestUnixSeconds() int64 {
	if len(h) == 0 {
		return 0
	}
	return h[0].unixNano / 1e9
}
