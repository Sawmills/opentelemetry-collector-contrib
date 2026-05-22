// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueBucketRemoveIndexesDoesNotAllocateWithoutCallback(t *testing.T) {
	bucket := &centralQueueBucket{}
	items := []centralQueueItem{
		{routingKey: []byte("lane-a"), compressedBytes: 1},
		{routingKey: []byte("lane-a"), compressedBytes: 2},
		{routingKey: []byte("lane-a"), compressedBytes: 3},
	}
	indexes := []int{0, 2}

	var removedLen int
	allocs := testing.AllocsPerRun(100, func() {
		bucket.items = items
		removedLen = bucket.removeIndexes(indexes, nil)
	})

	require.Equal(t, 2, removedLen)
	require.Zero(t, allocs)
}
