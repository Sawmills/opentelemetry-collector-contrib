// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"sync"
	"time"
)

type centralQueueBackendLimiter struct {
	mu     sync.Mutex
	active map[string]int
	limit  int
}

type centralQueueBackendLease struct {
	limiter  *centralQueueBackendLimiter
	endpoint string
	once     sync.Once
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
	if !limiter.tryAcquire(endpoint) {
		return nil, false
	}
	return &centralQueueAcquiredBackend{
		exporter: exp,
		endpoint: endpoint,
		lease: &centralQueueBackendLease{
			limiter:  limiter,
			endpoint: endpoint,
		},
	}, true
}

func (b *centralQueueAcquiredBackend) release() {
	if b == nil || b.lease == nil {
		return
	}
	b.lease.release()
}

func newCentralQueueBackendLimiter(limit int) *centralQueueBackendLimiter {
	if limit <= 0 {
		limit = defaultCentralQueueMaxInflightSendsPerBackend
	}
	return &centralQueueBackendLimiter{
		active: make(map[string]int),
		limit:  limit,
	}
}

func (l *centralQueueBackendLimiter) acquire(ctx context.Context, endpoint string) (*centralQueueBackendLease, error) {
	if l == nil || endpoint == "" {
		return &centralQueueBackendLease{}, nil
	}
	if l.tryAcquire(endpoint) {
		return &centralQueueBackendLease{limiter: l, endpoint: endpoint}, nil
	}
	ticker := time.NewTicker(centralQueueLeasePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if l.tryAcquire(endpoint) {
				return &centralQueueBackendLease{limiter: l, endpoint: endpoint}, nil
			}
		}
	}
}

func (l *centralQueueBackendLimiter) tryAcquire(endpoint string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active[endpoint] >= l.limit {
		return false
	}
	l.active[endpoint]++
	return true
}

func (l *centralQueueBackendLimiter) release(endpoint string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	active := l.active[endpoint]
	if active <= 1 {
		delete(l.active, endpoint)
		return
	}
	l.active[endpoint] = active - 1
}

func (l *centralQueueBackendLease) release() {
	if l == nil || l.limiter == nil || l.endpoint == "" {
		return
	}
	l.once.Do(func() {
		l.limiter.release(l.endpoint)
	})
}
