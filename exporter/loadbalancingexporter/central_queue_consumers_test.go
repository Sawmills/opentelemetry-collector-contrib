// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTryAcquireCentralQueueConsumerBlocksWhenEffectiveConsumersAreFull(t *testing.T) {
	var active atomic.Int64
	controller := newCentralQueueConsumerController(120, 256<<10, 1)
	lb := centralQueueConsumerTestLoadBalancerWithRoutableBackendCount(2)
	active.Store(2)

	acquired := tryAcquireCentralQueueConsumer(t.Context(), &active, controller, nil, lb, 1<<30)

	require.False(t, acquired)
	require.EqualValues(t, 2, active.Load())
}

func TestTryAcquireCentralQueueConsumerDoesNotRecoverOnFailedAcquire(t *testing.T) {
	var active atomic.Int64
	active.Store(5)
	controller := &centralQueueConsumerController{
		policy: centralQueueConsumerPolicy{
			maxConsumers:               120,
			minConsumers:               1,
			targetCompressedBytes:      256 << 10,
			maxInflightSendsPerBackend: 1,
			pressureRecoveryStep:       1,
		},
		last: centralQueueConsumerResult{
			effectiveConsumers: 4,
			limitReason:        centralQueueConsumerLimitReasonBackendPressure,
			pressureState:      centralQueueConsumerPressureReducing,
		},
	}
	lb := centralQueueConsumerTestLoadBalancerWithRoutableBackendCount(16)

	first := tryAcquireCentralQueueConsumer(t.Context(), &active, controller, nil, lb, 1<<30)
	second := tryAcquireCentralQueueConsumer(t.Context(), &active, controller, nil, lb, 1<<30)

	require.False(t, first)
	require.False(t, second)
	require.EqualValues(t, 5, active.Load())
	require.Equal(t, 4, controller.last.effectiveConsumers)
	require.Equal(t, centralQueueConsumerPressureReducing, controller.last.pressureState)
}

func TestTryAcquireCentralQueueConsumerCommitsPressureReductionOnFailedAcquire(t *testing.T) {
	var active atomic.Int64
	active.Store(8)
	controller := &centralQueueConsumerController{
		policy: centralQueueConsumerPolicy{
			maxConsumers:               120,
			minConsumers:               1,
			targetCompressedBytes:      256 << 10,
			maxInflightSendsPerBackend: 1,
		},
		last: centralQueueConsumerResult{
			effectiveConsumers: 8,
			limitReason:        centralQueueConsumerLimitReasonBackendCapacity,
			pressureState:      centralQueueConsumerPressureStable,
		},
	}

	result, acquired, changed := controller.tryAcquire(&active, 1<<30, 16, true)

	require.False(t, acquired)
	require.True(t, changed)
	require.EqualValues(t, 8, active.Load())
	require.Equal(t, 4, result.effectiveConsumers)
	require.Equal(t, 4, controller.last.effectiveConsumers)
	require.Equal(t, centralQueueConsumerPressureReducing, controller.last.pressureState)
}

func TestTryAcquireCentralQueueConsumerReleasesAndNotifies(t *testing.T) {
	var active atomic.Int64
	controller := newCentralQueueConsumerController(120, 256<<10, 1)
	lb := centralQueueConsumerTestLoadBalancerWithRoutableBackendCount(2)
	queue := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           100,
		maxInflightUncompressedBytes: 100,
		maxUncompressedBatchBytes:    100,
	})

	acquired := tryAcquireCentralQueueConsumer(t.Context(), &active, controller, queue, lb, 1<<30)
	require.True(t, acquired)
	require.EqualValues(t, 1, active.Load())

	releaseCentralQueueConsumer(t.Context(), &active, queue)
	require.Zero(t, active.Load())
	select {
	case <-queue.notify:
	default:
		t.Fatal("expected release to notify central queue lease waiters")
	}
}

func centralQueueConsumerTestLoadBalancerWithRoutableBackendCount(count int) *loadBalancer {
	lb := &loadBalancer{}
	lb.cachedRoutableBackendCount.Store(int64(count))
	lb.hasCachedRoutableBackendCount.Store(true)
	return lb
}
