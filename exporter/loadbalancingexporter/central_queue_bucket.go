// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

type centralQueueBucket struct {
	routingKey       []byte
	items            []centralQueueItem
	candidateIndexes []int
	readyAtUnixNano  int64
	readyHeapIndex   int
}

func newCentralQueueBucket(routingKey []byte) *centralQueueBucket {
	return &centralQueueBucket{
		routingKey:     append([]byte(nil), routingKey...),
		readyHeapIndex: -1,
	}
}

func (b *centralQueueBucket) append(item centralQueueItem) {
	b.items = append(b.items, item)
}

func (b *centralQueueBucket) removeIndexes(indexes []int) []centralQueueItem {
	if len(indexes) == 0 {
		return nil
	}

	removed := make([]centralQueueItem, 0, len(indexes))
	writeIndex := indexes[0]
	removeIndex := 0
	for readIndex := indexes[0]; readIndex < len(b.items); readIndex++ {
		if removeIndex < len(indexes) && indexes[removeIndex] == readIndex {
			removed = append(removed, b.items[readIndex])
			for removeIndex < len(indexes) && indexes[removeIndex] == readIndex {
				removeIndex++
			}
			continue
		}
		b.items[writeIndex] = b.items[readIndex]
		writeIndex++
	}
	clear(b.items[writeIndex:])
	b.items = b.items[:writeIndex]
	return removed
}

type centralQueueReadyHeap []*centralQueueBucket

func (h centralQueueReadyHeap) Len() int {
	return len(h)
}

func (h centralQueueReadyHeap) Less(i, j int) bool {
	return h[i].readyAtUnixNano < h[j].readyAtUnixNano
}

func (h centralQueueReadyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].readyHeapIndex = i
	h[j].readyHeapIndex = j
}

func (h *centralQueueReadyHeap) Push(x any) {
	bucket := x.(*centralQueueBucket)
	bucket.readyHeapIndex = len(*h)
	*h = append(*h, bucket)
}

func (h *centralQueueReadyHeap) Pop() any {
	old := *h
	n := len(old)
	bucket := old[n-1]
	bucket.readyHeapIndex = -1
	old[n-1] = nil
	*h = old[:n-1]
	return bucket
}
