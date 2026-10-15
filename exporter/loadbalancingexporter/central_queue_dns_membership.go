// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"net/netip"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type centralQueueDNSMembership struct {
	config   DNSMembershipConfig
	resolver netResolver
	now      func() time.Time
}

func (m *centralQueueMembership) runDNS(ctx context.Context) {
	m.refreshDNS(ctx)
	ticker := time.NewTicker(m.dns.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refreshDNS(ctx)
		}
	}
}

func (m *centralQueueMembership) refreshDNS(ctx context.Context) {
	lookupCtx, cancel := context.WithTimeout(ctx, m.dns.config.Timeout)
	defer cancel()
	addresses, err := m.dns.resolver.LookupIPAddr(lookupCtx, m.dns.config.Hostname)
	self, _ := netip.ParseAddr(m.dns.config.SelfIP)
	self = self.Unmap()
	peers := make(map[netip.Addr]struct{})
	for _, address := range addresses {
		ip, ok := netip.AddrFromSlice(address.IP)
		if !ok {
			continue
		}
		ip = ip.Unmap()
		if ip.Is4() == self.Is4() {
			peers[ip] = struct{}{}
		}
	}
	_, includesSelf := peers[self]
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || ctx.Err() != nil {
		return
	}
	m.initialized = err == nil && lookupCtx.Err() == nil && includesSelf
	m.count = 0
	m.freshUntil = time.Time{}
	if m.initialized {
		m.count = len(peers)
		m.freshUntil = m.dns.now().Add(m.dns.config.MaxAge)
	}
	reason := ""
	switch {
	case err != nil || lookupCtx.Err() != nil:
		reason = "lookup_failed"
	case len(peers) == 0:
		reason = "empty"
	case !includesSelf:
		reason = "self_missing"
	}
	m.recordDNSResolution(m.initialized, reason)
	if !m.initialized {
		m.logger.Debug("load balancer DNS membership unavailable; using configured behavior", zap.String("reason", reason), zap.Error(err))
	}
}

func (m *centralQueueMembership) recordDNSResolution(success bool, reason string) {
	if m.telemetry == nil {
		return
	}
	attrs := []attribute.KeyValue{dnsResolverAttr, attribute.String("scope", "load_balancer_membership"), attribute.Bool("success", success)}
	if reason != "" {
		attrs = append(attrs, attribute.String("reason", reason))
	}
	m.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}
