// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"sync"
	"time"
)

const centralQueueBackendCompletionZeroObservations = 2

type centralQueueBackendLimiter struct {
	mu                   sync.Mutex
	active               map[string]int
	inflightEnqueuedAt   map[string]map[int64]int
	completionZeros      map[string]int
	limit                int
	waiters              map[string]chan struct{}
	fallbackInitialDelay time.Duration
}

type centralQueueBackendLease struct {
	limiter          *centralQueueBackendLimiter
	endpoint         string
	oldestEnqueuedAt int64
	once             sync.Once
}

type centralQueueAcquiredBackend struct {
	exporter *wrappedExporter
	endpoint string
	lease    *centralQueueBackendLease
}

func tryAcquireCentralQueueBackendForWindow(lb *loadBalancer, limiter *centralQueueBackendLimiter, window centralQueueWindow) (*centralQueueAcquiredBackend, bool) {
	if lb == nil {
		return nil, true
	}
	exp, endpoint, err := lb.exporterAndEndpoint(window.routingKey)
	if err != nil {
		return nil, true
	}
	if limiter == nil || endpoint == "" {
		return &centralQueueAcquiredBackend{
			exporter: exp,
			endpoint: endpoint,
			lease:    &centralQueueBackendLease{},
		}, true
	}
	if !limiter.tryAcquire(endpoint, window.oldestEnqueuedAt) {
		return nil, false
	}
	return &centralQueueAcquiredBackend{
		exporter: exp,
		endpoint: endpoint,
		lease: &centralQueueBackendLease{
			limiter:          limiter,
			endpoint:         endpoint,
			oldestEnqueuedAt: window.oldestEnqueuedAt,
		},
	}, true
}

func newCentralQueueBackendLimiter() *centralQueueBackendLimiter {
	return &centralQueueBackendLimiter{
		active:               make(map[string]int),
		inflightEnqueuedAt:   make(map[string]map[int64]int),
		completionZeros:      make(map[string]int),
		limit:                defaultCentralQueueMaxInflightSendsPerBackend,
		waiters:              make(map[string]chan struct{}),
		fallbackInitialDelay: centralQueueLeaseFallbackInitialDelay,
	}
}

func (l *centralQueueBackendLimiter) acquire(ctx context.Context, endpoint string, oldestEnqueuedAt int64) (*centralQueueBackendLease, error) {
	if l == nil || endpoint == "" {
		return &centralQueueBackendLease{}, nil
	}
	acquired, notify := l.tryAcquireOrWait(endpoint, oldestEnqueuedAt)
	if acquired {
		return &centralQueueBackendLease{limiter: l, endpoint: endpoint, oldestEnqueuedAt: oldestEnqueuedAt}, nil
	}
	delay := l.fallbackInitialDelay
	if delay <= 0 {
		delay = centralQueueLeaseFallbackInitialDelay
	}
	maxDelay := max(delay, centralQueueLeaseFallbackMaxDelay)
	timer := time.NewTimer(delay)
	defer stopCentralQueueTimer(timer)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
			delay = l.fallbackInitialDelay
			if delay <= 0 {
				delay = centralQueueLeaseFallbackInitialDelay
			}
		case <-timer.C:
			delay = min(delay*2, maxDelay)
		}
		acquired, notify = l.tryAcquireOrWait(endpoint, oldestEnqueuedAt)
		if acquired {
			return &centralQueueBackendLease{limiter: l, endpoint: endpoint, oldestEnqueuedAt: oldestEnqueuedAt}, nil
		}
		resetCentralQueueTimer(timer, delay)
	}
}

func (l *centralQueueBackendLimiter) tryAcquireOrWait(endpoint string, oldestEnqueuedAt int64) (bool, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[endpoint] < l.limit {
		l.trackAcquireLocked(endpoint, oldestEnqueuedAt)
		return true, nil
	}
	if l.waiters == nil {
		l.waiters = make(map[string]chan struct{})
	}
	notify := l.waiters[endpoint]
	if notify == nil {
		notify = make(chan struct{})
		l.waiters[endpoint] = notify
	}
	return false, notify
}

func (l *centralQueueBackendLimiter) tryAcquire(endpoint string, oldestEnqueuedAt int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[endpoint] >= l.limit {
		return false
	}
	l.trackAcquireLocked(endpoint, oldestEnqueuedAt)
	return true
}

func (l *centralQueueBackendLimiter) release(endpoint string, oldestEnqueuedAt int64) {
	l.mu.Lock()
	active := l.active[endpoint]
	if active <= 1 {
		delete(l.active, endpoint)
	} else {
		l.active[endpoint] = active - 1
	}
	l.trackReleaseLocked(endpoint, oldestEnqueuedAt)
	notify := l.waiters[endpoint]
	delete(l.waiters, endpoint)
	l.mu.Unlock()
	if notify != nil {
		close(notify)
	}
}

func (l *centralQueueBackendLimiter) trackAcquireLocked(endpoint string, oldestEnqueuedAt int64) {
	l.active[endpoint]++
	delete(l.completionZeros, endpoint)
	if oldestEnqueuedAt <= 0 {
		return
	}
	if l.inflightEnqueuedAt == nil {
		l.inflightEnqueuedAt = make(map[string]map[int64]int)
	}
	timestamps := l.inflightEnqueuedAt[endpoint]
	if timestamps == nil {
		timestamps = make(map[int64]int)
		l.inflightEnqueuedAt[endpoint] = timestamps
	}
	timestamps[oldestEnqueuedAt]++
}

func (l *centralQueueBackendLimiter) trackReleaseLocked(endpoint string, oldestEnqueuedAt int64) {
	if oldestEnqueuedAt <= 0 {
		return
	}
	timestamps := l.inflightEnqueuedAt[endpoint]
	if timestamps == nil {
		return
	}
	if timestamps[oldestEnqueuedAt] <= 1 {
		delete(timestamps, oldestEnqueuedAt)
	} else {
		timestamps[oldestEnqueuedAt]--
	}
	if len(timestamps) == 0 {
		delete(l.inflightEnqueuedAt, endpoint)
	}
	if l.active[endpoint] == 0 {
		if l.completionZeros == nil {
			l.completionZeros = make(map[string]int)
		}
		l.completionZeros[endpoint] = centralQueueBackendCompletionZeroObservations
	}
}

func (l *centralQueueBackendLimiter) oldestInflightAges(now time.Time) map[string]int64 {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ages := make(map[string]int64, len(l.active)+len(l.completionZeros))
	for endpoint := range l.active {
		ages[endpoint] = 0
	}
	for endpoint, timestamps := range l.inflightEnqueuedAt {
		var oldest int64
		for enqueuedAt := range timestamps {
			if oldest == 0 || enqueuedAt < oldest {
				oldest = enqueuedAt
			}
		}
		if oldest == 0 {
			continue
		}
		age := now.Sub(time.Unix(0, oldest)).Milliseconds()
		if age > 0 {
			ages[endpoint] = age
		}
	}
	for endpoint, remaining := range l.completionZeros {
		ages[endpoint] = 0
		if remaining <= 1 {
			delete(l.completionZeros, endpoint)
		} else {
			l.completionZeros[endpoint] = remaining - 1
		}
	}
	return ages
}

func (l *centralQueueBackendLease) release() {
	if l == nil || l.limiter == nil || l.endpoint == "" {
		return
	}
	l.once.Do(func() {
		l.limiter.release(l.endpoint, l.oldestEnqueuedAt)
	})
}
