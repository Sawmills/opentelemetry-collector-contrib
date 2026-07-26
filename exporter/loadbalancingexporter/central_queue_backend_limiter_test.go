// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueBackendLimiterReleaseWakesWaiter(t *testing.T) {
	limiter := newCentralQueueBackendLimiter()
	limiter.limit = 1
	limiter.fallbackInitialDelay = time.Hour

	first, err := limiter.acquire(t.Context(), "endpoint-1:4317")
	require.NoError(t, err)

	acquired := make(chan *centralQueueBackendLease, 1)
	go func() {
		lease, acquireErr := limiter.acquire(t.Context(), "endpoint-1:4317")
		if acquireErr == nil {
			acquired <- lease
		}
	}()

	select {
	case <-acquired:
		t.Fatal("backend slot acquired before release")
	case <-time.After(20 * time.Millisecond):
	}

	start := time.Now()
	first.release()
	select {
	case lease := <-acquired:
		require.Less(t, time.Since(start), time.Second)
		lease.release()
	case <-time.After(time.Second):
		t.Fatal("backend slot release did not wake waiter")
	}
}

func TestCentralQueueBackendLimiterReleaseWakesOnlyMatchingEndpoint(t *testing.T) {
	limiter := newCentralQueueBackendLimiter()
	limiter.limit = 1
	limiter.fallbackInitialDelay = time.Hour

	firstA, err := limiter.acquire(t.Context(), "endpoint-a:4317")
	require.NoError(t, err)
	firstB, err := limiter.acquire(t.Context(), "endpoint-b:4317")
	require.NoError(t, err)

	acquiredA := make(chan *centralQueueBackendLease, 1)
	acquiredB := make(chan *centralQueueBackendLease, 1)
	go acquireCentralQueueBackendForTest(t.Context(), limiter, "endpoint-a:4317", acquiredA)
	go acquireCentralQueueBackendForTest(t.Context(), limiter, "endpoint-b:4317", acquiredB)
	require.Eventually(t, func() bool {
		limiter.mu.Lock()
		defer limiter.mu.Unlock()
		return len(limiter.waiters) == 2
	}, time.Second, time.Millisecond)

	firstA.release()
	select {
	case lease := <-acquiredA:
		lease.release()
	case <-time.After(time.Second):
		t.Fatal("endpoint A release did not wake endpoint A waiter")
	}
	select {
	case <-acquiredB:
		t.Fatal("endpoint A release woke endpoint B waiter")
	case <-time.After(20 * time.Millisecond):
	}

	firstB.release()
	select {
	case lease := <-acquiredB:
		lease.release()
	case <-time.After(time.Second):
		t.Fatal("endpoint B release did not wake endpoint B waiter")
	}
}

func acquireCentralQueueBackendForTest(
	ctx context.Context,
	limiter *centralQueueBackendLimiter,
	endpoint string,
	acquired chan<- *centralQueueBackendLease,
) {
	lease, err := limiter.acquire(ctx, endpoint)
	if err == nil {
		acquired <- lease
	}
}
