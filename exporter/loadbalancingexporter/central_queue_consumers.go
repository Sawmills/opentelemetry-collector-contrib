// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"sync/atomic"
)

func tryAcquireCentralQueueConsumer(
	ctx context.Context,
	active *atomic.Int64,
	controller *centralQueueConsumerController,
	queue *centralQueue,
	lb *loadBalancer,
	queueCompressedBytes int64,
) bool {
	if active == nil {
		return false
	}
	result, decisionChanged := controller.computeWithChange(queueCompressedBytes, centralQueueRoutableBackendCount(lb), centralQueueBackendPressure(lb))
	if queue != nil && decisionChanged {
		queue.settings.telemetry.recordConsumerDecision(ctx, result)
	}
	if result.effectiveConsumers <= 0 {
		return false
	}

	for {
		current := active.Load()
		if current >= int64(result.effectiveConsumers) {
			return false
		}
		if active.CompareAndSwap(current, current+1) {
			if queue != nil {
				queue.settings.telemetry.recordActiveConsumers(ctx, current+1)
			}
			return true
		}
	}
}

func releaseCentralQueueConsumer(ctx context.Context, active *atomic.Int64, queue *centralQueue) {
	if active == nil {
		return
	}
	current := active.Add(-1)
	if queue != nil {
		queue.settings.telemetry.recordActiveConsumers(ctx, current)
		queue.notifyLeaseWaiters()
	}
}

func centralQueueBackendPressure(lb *loadBalancer) bool {
	return lb != nil && lb.endpointHealth != nil && lb.endpointHealth.underPressure()
}
