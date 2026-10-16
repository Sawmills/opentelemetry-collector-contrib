// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestCentralQueueDNSMembershipConfiguration(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	require.NoError(t, cfg.Validate())
}

func TestCentralQueueDNSMembershipFrameworkValidationRespectsDisabledQueue(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	cfg.CentralQueue.Enabled = false
	cfg.CentralQueue.LoadBalancerDNS.SelfIP = ""
	require.NoError(t, xconfmap.Validate(cfg), "inactive DNS settings must not prevent startup")
	cfg.CentralQueue.Enabled = true
	require.ErrorContains(t, xconfmap.Validate(cfg), "self_ip")
}

func TestCentralQueueDNSMembershipNormalStartup(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	for _, signal := range []string{"logs", "metrics"} {
		t.Run(signal, func(t *testing.T) {
			cfg := dnsMembershipTestConfig(t)
			var m *centralQueueMembership
			var controller *centralQueueConsumerController
			var start func(context.Context) error
			var stop func(context.Context) error
			if signal == "logs" {
				e, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, err)
				m, controller = e.centralQueueMembership, e.centralQueueConsumers
				start = func(ctx context.Context) error { return e.Start(ctx, componenttest.NewNopHost()) }
				stop = e.Shutdown
			} else {
				e, err := newMetricsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, err)
				m, controller = e.centralQueueMembership, e.centralQueueConsumers
				start = func(ctx context.Context) error { return e.Start(ctx, componenttest.NewNopHost()) }
				stop = e.Shutdown
			}
			m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(ctx context.Context, hostname string) ([]net.IPAddr, error) {
				assert.Equal(t, "collector-lb-peers.customer.svc", hostname)
				assert.NotNil(t, ctx.Done(), "lookup must have a timeout")
				return dnsMembershipAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6", "10.0.0.1", "2001:db8::1"), nil
			}}
			require.NoError(t, start(t.Context()))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
				defer cancel()
				require.NoError(t, stop(ctx))
			})
			requireMembership(t, m, 6, true)
			var active atomic.Int64
			decision, acquired, _ := controller.tryAcquire(&active, 1<<30, 18, false)
			require.True(t, acquired)
			require.Equal(t, 3, decision.backendSafeConsumersPerLB)
			m.shutdown()
			active.Store(0)
			decision, acquired, _ = controller.tryAcquire(&active, 1<<30, 18, false)
			require.True(t, acquired, "unknown DNS must keep sends moving for both signals")
			require.Equal(t, 18, decision.backendSafeConsumersPerLB)
		})
	}
}

func dnsMembershipAddresses(addresses ...string) []net.IPAddr {
	result := make([]net.IPAddr, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, net.IPAddr{IP: net.ParseIP(address)})
	}
	return result
}

func TestCentralQueueDNSMembershipCountsFailedAttemptOnce(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	m, err := centralQueueMembershipFromConfig(cfg.CentralQueue, exportertest.NewNopSettings(metadata.Type).Logger)
	require.NoError(t, err)
	m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("DNS unavailable")
	}}
	settings, tb, reader := getTelemetryAssetsWithReader(t)
	queueTelemetry, err := newCentralQueueTelemetry(settings.TelemetrySettings, signalKindLogs)
	require.NoError(t, err)
	controller := newCentralQueueConsumerController(30, 256<<10, 1)
	controller.legacyOnUnknownMembership = true
	m.bind(t.Context(), controller, queueTelemetry, tb)
	t.Cleanup(m.shutdown)
	require.Eventually(t, func() bool {
		value, readErr := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
		if readErr != nil {
			return false
		}
		for _, point := range value.Data.(metricdata.Sum[int64]).DataPoints {
			if reason, ok := point.Attributes.Value("reason"); ok && reason.AsString() == "lookup_failed" {
				return point.Value == 1
			}
		}
		return false
	}, time.Second, time.Millisecond)
	for range 100 {
		count, known := m.snapshot()
		require.Zero(t, count)
		require.False(t, known)
	}
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_num_resolutions", "{resolutions}", attribute.NewSet(dnsResolverAttr, attribute.String("scope", "load_balancer_membership"), attribute.Bool("success", false), attribute.String("reason", "lookup_failed")), 1)
	var active atomic.Int64
	_, acquired, _ := controller.tryAcquire(&active, 1<<30, 18, false)
	require.True(t, acquired)
	requireCentralQueueIntGauge(t, reader, "otelcol_loadbalancer_central_queue_active_load_balancer_replicas", "{replicas}", attribute.NewSet(attribute.String("signal", "logs")), 0)
}

func TestCentralQueueDNSMembershipScaleFailureRecovery(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	m, err := centralQueueMembershipFromConfig(cfg.CentralQueue, exportertest.NewNopSettings(metadata.Type).Logger)
	require.NoError(t, err)
	m.dns.config.Interval = time.Millisecond
	var state atomic.Int64
	m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
		switch state.Load() {
		case 0:
			return dnsMembershipAddresses("10.0.0.1"), nil
		case 1:
			return dnsMembershipAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6"), nil
		case 2:
			return dnsMembershipAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3"), nil
		case 3:
			return nil, errors.New("DNS unavailable")
		case 4:
			return nil, nil
		default:
			return dnsMembershipAddresses("10.0.0.2", "10.0.0.3"), nil
		}
	}}
	m.start(t.Context())
	t.Cleanup(m.shutdown)
	requireMembership(t, m, 1, true)
	state.Store(1)
	requireMembership(t, m, 6, true)
	state.Store(2)
	requireMembership(t, m, 3, true)
	for _, failure := range []int64{3, 4, 5} {
		state.Store(failure)
		requireMembership(t, m, 0, false)
		state.Store(2)
		requireMembership(t, m, 3, true)
	}
	m.shutdown()
	requireMembership(t, m, 0, false)
}

func TestCentralQueueDNSMembershipSixControllersShareBackend(t *testing.T) {
	fleet := newSharedBudgetFleet(t, 1, 18)
	for _, peer := range fleet.peers {
		cfg := dnsMembershipTestConfig(t)
		cfg.CentralQueue.TargetCompressedBytes = 1
		e, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)
		m := e.centralQueueMembership
		m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return dnsMembershipAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6"), nil
		}}
		peer.controller = e.centralQueueConsumers
		m.bind(t.Context(), peer.controller, nil, e.telemetry)
		t.Cleanup(m.shutdown)
		requireMembership(t, m, 6, true)
	}
	healthy := fleet.drainWave(t, false)
	require.Equal(t, 18, healthy.attempted)
	require.Zero(t, healthy.refused)
	require.Equal(t, 18, healthy.completed)
	fleet.requireRetained(t)
	pressured := fleet.drainWave(t, true)
	require.LessOrEqual(t, pressured.attempted, 18)
	require.Zero(t, pressured.refused)
	recovered := fleet.drainWave(t, false)
	require.LessOrEqual(t, recovered.attempted, 18)
	require.Zero(t, recovered.refused)
	require.Greater(t, recovered.completed, pressured.completed)
	fleet.requireRetained(t)
}

func TestCentralQueueDNSMembershipIPv6AndLookupTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "IPv6 deduplication", true: "late success is unknown"}[timeout], func(t *testing.T) {
			cfg := dnsMembershipTestConfig(t)
			cfg.CentralQueue.LoadBalancerDNS.SelfIP = "2001:db8::1"
			m, err := centralQueueMembershipFromConfig(cfg.CentralQueue, exportertest.NewNopSettings(metadata.Type).Logger)
			require.NoError(t, err)
			if timeout {
				m.dns.config.Timeout = time.Millisecond
			}
			m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
				if timeout {
					<-ctx.Done()
				}
				return dnsMembershipAddresses("2001:db8::1", "2001:db8::2", "2001:db8::2", "10.0.0.1"), nil
			}}
			_, tb, reader := getTelemetryAssetsWithReader(t)
			m.bind(t.Context(), newCentralQueueConsumerController(30, 256<<10, 1), nil, tb)
			t.Cleanup(m.shutdown)
			require.Eventually(t, func() bool {
				value, readErr := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
				return readErr == nil && len(value.Data.(metricdata.Sum[int64]).DataPoints) > 0
			}, time.Second, time.Millisecond)
			if timeout {
				requireMembership(t, m, 0, false)
			} else {
				requireMembership(t, m, 2, true)
			}
		})
	}
}

func TestCentralQueueDNSMembershipRenewalAndExpiry(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	m, err := centralQueueMembershipFromConfig(cfg.CentralQueue, exportertest.NewNopSettings(metadata.Type).Logger)
	require.NoError(t, err)
	m.dns.config.Interval = time.Millisecond
	var seconds atomic.Int64
	seconds.Store(1000)
	m.dns.now = func() time.Time { return time.Unix(seconds.Load(), 0) }
	answers := make(chan []net.IPAddr)
	m.dns.resolver = &mockDNSResolver{onLookupIPAddr: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
		select {
		case answer := <-answers:
			return answer, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	_, tb, reader := getTelemetryAssetsWithReader(t)
	m.bind(t.Context(), newCentralQueueConsumerController(30, 256<<10, 1), nil, tb)
	t.Cleanup(m.shutdown)
	send := func() {
		t.Helper()
		select {
		case answers <- dnsMembershipAddresses("10.0.0.1", "10.0.0.2", "10.0.0.3"):
		case <-time.After(time.Second):
			t.Fatal("DNS lookup did not start")
		}
	}
	successes := func() int64 {
		value, readErr := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
		if readErr != nil {
			return 0
		}
		for _, point := range value.Data.(metricdata.Sum[int64]).DataPoints {
			if success, ok := point.Attributes.Value("success"); ok && success.AsBool() {
				return point.Value
			}
		}
		return 0
	}
	send()
	requireMembership(t, m, 3, true)
	seconds.Store(1010)
	send()
	require.Eventually(t, func() bool { return successes() == 2 }, time.Second, time.Millisecond)
	seconds.Store(1016)
	requireMembership(t, m, 3, true)
	seconds.Store(1025)
	for range 100 {
		count, known := m.snapshot()
		require.Zero(t, count)
		require.False(t, known, "membership expires at the age boundary")
	}
	value, readErr := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
	require.NoError(t, readErr)
	var stale int64
	for _, point := range value.Data.(metricdata.Sum[int64]).DataPoints {
		if reason, ok := point.Attributes.Value("reason"); ok && reason.AsString() == "stale" {
			stale += point.Value
		}
	}
	require.EqualValues(t, 1, stale, "expiry is counted once despite repeated snapshots")
	send()
	requireMembership(t, m, 3, true)
	stopped := make(chan struct{})
	go func() { m.shutdown(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("DNS shutdown did not cancel pending work")
	}
	requireMembership(t, m, 0, false)
}

func TestCentralQueueDNSMembershipFactoriesNeedNoKubernetesCredentials(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	cfg := dnsMembershipTestConfig(t)
	logs, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NotNil(t, logs.centralQueueMembership, "DNS mode must create a live membership source")
	metrics, err := newMetricsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NotNil(t, metrics.centralQueueMembership, "DNS mode must create a live membership source")
}

func TestCentralQueueDNSMembershipRejectsAPIMode(t *testing.T) {
	cfg := dnsMembershipTestConfig(t)
	cfg.CentralQueue.LoadBalancerService = "collector.customer"

	require.ErrorContains(t, cfg.Validate(), "load_balancer_dns and load_balancer_service are mutually exclusive")
}

func TestCentralQueueDNSMembershipRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*DNSMembershipConfig)
		want string
	}{
		{"empty hostname", func(c *DNSMembershipConfig) { c.Hostname = "" }, "hostname"},
		{"padded hostname", func(c *DNSMembershipConfig) { c.Hostname = " collector-lb-peers.customer.svc " }, "hostname"},
		{"invalid self address", func(c *DNSMembershipConfig) { c.SelfIP = "unknown" }, "self_ip"},
		{"negative interval", func(c *DNSMembershipConfig) { c.Interval = -time.Second }, "interval"},
		{"negative timeout", func(c *DNSMembershipConfig) { c.Timeout = -time.Second }, "timeout"},
		{"negative age", func(c *DNSMembershipConfig) { c.MaxAge = -time.Second }, "max_age"},
		{"age shorter than lookup cycle", func(c *DNSMembershipConfig) { c.MaxAge = time.Second }, "max_age"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := dnsMembershipTestConfig(t)
			tc.edit(cfg.CentralQueue.LoadBalancerDNS)
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}
}

func TestCentralQueueDNSMembershipUnknownKeepsLegacyBudget(t *testing.T) {
	baseline := newSharedBudgetFleet(t, 1, 18).drainWave(t, false)
	fleet := newSharedBudgetFleet(t, 1, 18)
	for _, peer := range fleet.peers {
		cfg := dnsMembershipTestConfig(t)
		cfg.CentralQueue.TargetCompressedBytes = 1
		exporter, err := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)
		peer.controller = exporter.centralQueueConsumers
		peer.controller.setMembershipSource(func() (int, bool) { return 0, false })
	}

	unknown := fleet.drainWave(t, false)

	require.Positive(t, unknown.attempted, "unknown DNS membership must not hold queued sends")
	require.Equal(t, baseline.attempted, unknown.attempted)
	require.Equal(t, baseline.refused, unknown.refused)
	fleet.requireRetained(t)
}

func dnsMembershipTestConfig(t *testing.T) *Config {
	t.Helper()
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"resolver": map[string]any{
			"static": map[string]any{"hostnames": []string{"worker:4317"}},
		},
		"central_queue": map[string]any{
			"enabled":              true,
			"max_compressed_bytes": 1 << 20,
			"load_balancer_dns": map[string]any{
				"hostname": "collector-lb-peers.customer.svc",
				"self_ip":  "10.0.0.1",
				"interval": "5s",
				"timeout":  "1s",
				"max_age":  "15s",
			},
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))
	return cfg
}
