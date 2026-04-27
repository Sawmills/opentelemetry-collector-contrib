// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestNewLoadBalancerNoResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoResolver, err)
}

func TestNewLoadBalancerInvalidStaticResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{}}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoEndpoints, err)
}

func TestNewLoadBalancerInvalidDNSResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			DNS: configoptional.Some(DNSResolver{
				Hostname: "",
			}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoHostname, err)
}

func TestNewLoadBalancerInvalidK8sResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			K8sSvc: configoptional.Some(K8sSvcResolver{
				Service: "",
			}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	assert.Nil(t, p)
	assert.True(t, clientcmd.IsConfigurationInvalid(err) || errors.Is(err, errNoSvc))
}

func TestLoadBalancerStart(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()

	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)
	require.NotNil(t, p)
	require.NoError(t, err)
	p.res = &mockResolver{}

	// test
	res := p.Start(t.Context(), componenttest.NewNopHost())
	defer func() {
		require.NoError(t, p.Shutdown(t.Context()))
	}()
	// verify
	assert.NoError(t, res)
}

func TestWithDNSResolver(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			DNS: configoptional.Some(DNSResolver{
				Hostname: "service-1",
			}),
		},
	}

	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	res, ok := p.res.(*dnsResolver)

	// verify
	assert.NotNil(t, res)
	assert.True(t, ok)
}

func TestWithDNSResolverNoEndpoints(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			DNS: configoptional.Some(DNSResolver{
				Hostname: "service-1",
			}),
		},
	}

	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)
	require.NotNil(t, p)
	require.NoError(t, err)
	res, ok := p.res.(*dnsResolver)
	require.True(t, ok)
	res.resolver = &mockDNSResolver{
		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return nil, nil
		},
	}

	err = p.Start(t.Context(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() { assert.NoError(t, p.Shutdown(t.Context())) }()

	// test
	_, e, _ := p.exporterAndEndpoint([]byte{128, 128, 0, 0})

	// verify
	assert.Empty(t, e)
}

func TestMultipleResolvers(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{
				Hostnames: []string{"endpoint-1", "endpoint-2"},
			}),
			DNS: configoptional.Some(DNSResolver{
				Hostname: "service-1",
			}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	assert.Nil(t, p)
	assert.Equal(t, errMultipleResolversProvided, err)
}

func TestStartFailureStaticResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()

	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	expectedErr := errors.New("some expected err")
	p.res = &mockResolver{
		onStart: func(context.Context) error {
			return expectedErr
		},
	}

	// test
	res := p.Start(t.Context(), componenttest.NewNopHost())

	// verify
	assert.Equal(t, expectedErr, res)
}

func TestLoadBalancerShutdown(t *testing.T) {
	// prepare
	cfg := simpleConfig()
	p, err := newTracesExporter(exportertest.NewNopSettings(metadata.Type), cfg)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	res := p.Shutdown(t.Context())

	// verify
	assert.NoError(t, res)
}

func TestOnBackendChanges(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return newNopMockExporter(), nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	p.onBackendChanges([]string{"endpoint-1"})
	require.Len(t, p.ring.items, defaultWeight)

	// this should resolve to two endpoints
	endpoints := []string{"endpoint-1", "endpoint-2"}
	p.onBackendChanges(endpoints)

	// verify
	assert.Len(t, p.ring.items, 2*defaultWeight)
}

func TestRemoveExtraExporters(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return newNopMockExporter(), nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.addMissingExporters(t.Context(), []string{"endpoint-1", "endpoint-2"})
	resolved := []string{"endpoint-1"}

	// test
	removed := p.removeExtraExportersLocked(resolved)
	p.drainRemovedExporters(t.Context(), removed)

	// verify
	assert.Len(t, p.exporters, 1)
	assert.NotContains(t, p.exporters, endpointWithPort("endpoint-2"))
}

func TestAddMissingExporters(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	exporterFactory := exporter.NewFactory(component.MustNewType("otlp"), func() component.Config {
		return &otlpexporter.Config{}
	}, exporter.WithTraces(func(
		_ context.Context,
		_ exporter.Settings,
		_ component.Config,
	) (exporter.Traces, error) {
		return newNopMockTracesExporter(), nil
	}, component.StabilityLevelDevelopment))
	fn := func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := cfg.Protocol.OTLP
		oCfg.ClientConfig.Endpoint = endpoint
		return exporterFactory.CreateTraces(ctx, exportertest.NewNopSettings(exporterFactory.Type()), &oCfg)
	}

	p, err := newLoadBalancer(ts.Logger, cfg, fn, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.exporters["endpoint-1:4317"] = newNopMockExporter()
	resolved := []string{"endpoint-1", "endpoint-2"}

	// test
	p.addMissingExporters(t.Context(), resolved)

	// verify
	assert.Len(t, p.exporters, 2)
	assert.Contains(t, p.exporters, "endpoint-2:4317")
}

func TestFailedToAddMissingExporters(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	expectedErr := errors.New("some expected error")
	exporterFactory := exporter.NewFactory(component.MustNewType("otlp"), func() component.Config {
		return &otlpexporter.Config{}
	}, exporter.WithTraces(func(
		_ context.Context,
		_ exporter.Settings,
		_ component.Config,
	) (exporter.Traces, error) {
		return nil, expectedErr
	}, component.StabilityLevelDevelopment))
	fn := func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := cfg.Protocol.OTLP
		oCfg.ClientConfig.Endpoint = endpoint
		return exporterFactory.CreateTraces(ctx, exportertest.NewNopSettings(metadata.Type), &oCfg)
	}

	p, err := newLoadBalancer(ts.Logger, cfg, fn, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.exporters["endpoint-1:4317"] = newNopMockExporter()
	resolved := []string{"endpoint-1", "endpoint-2"}

	// test
	p.addMissingExporters(t.Context(), resolved)

	// verify
	assert.Len(t, p.exporters, 1)
	assert.Contains(t, p.exporters, "endpoint-1:4317")
}

func TestLoadBalancerEndpointHealthExcludesQuarantinedEndpoint(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return mockComponent{}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
	require.Contains(t, p.exporters, "endpoint-1:4317")
	require.Contains(t, p.exporters, "endpoint-2:4317")

	decision := p.handleBackendFailure(t.Context(), "endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.endpointLocal)
	require.True(t, decision.quarantined)
	require.NotContains(t, p.exporters, "endpoint-1:4317")
	require.Contains(t, p.exporters, "endpoint-2:4317")

	for i := range 100 {
		_, endpoint, err := p.exporterAndEndpoint([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		require.NoError(t, err)
		assert.NotEqual(t, "endpoint-1:4317", endpoint)
	}
}

func TestLoadBalancerEndpointHealthRemovesDNSStaleEndpoint(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)
	var shutdowns sync.Map
	componentFactory := func(_ context.Context, endpoint string) (component.Component, error) {
		return &countingComponent{endpoint: endpoint, shutdowns: &shutdowns}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
	p.onBackendChanges([]string{"endpoint-2"})

	require.NotContains(t, p.exporters, "endpoint-1:4317")
	require.Contains(t, p.exporters, "endpoint-2:4317")
	require.Eventually(t, func() bool {
		count, ok := shutdowns.Load("endpoint-1:4317")
		return ok && count.(*atomic.Int64).Load() > 0
	}, time.Second, 10*time.Millisecond)
}

func TestLoadBalancerEndpointHealthFailOpenRefreshesFailedExporter(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return mockComponent{}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
	endpoint1Original := p.exporters["endpoint-1:4317"]
	endpoint2Original := p.exporters["endpoint-2:4317"]

	decision := p.handleBackendFailure(t.Context(), "endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	require.False(t, decision.failOpen)

	decision = p.handleBackendFailure(t.Context(), "endpoint-2:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	require.True(t, decision.failOpen)
	require.Contains(t, decision.eligible, "endpoint-1:4317")
	require.Contains(t, decision.eligible, "endpoint-2:4317")
	require.NotSame(t, endpoint1Original, p.exporters["endpoint-1:4317"])
	require.NotSame(t, endpoint2Original, p.exporters["endpoint-2:4317"])
}

func TestLoadBalancerEndpointHealthSuccessRecoversFailOpenRing(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return mockComponent{}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
	decision := p.handleBackendFailure(t.Context(), "endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	decision = p.handleBackendFailure(t.Context(), "endpoint-2:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.failOpen)

	p.handleBackendSuccess("endpoint-1:4317")

	require.False(t, p.endpointHealth.failOpen())
	for i := range 100 {
		_, endpoint, err := p.exporterAndEndpoint([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		require.NoError(t, err)
		assert.Equal(t, "endpoint-1:4317", endpoint)
	}
}

func TestLoadBalancerEndpointHealthCreatesExportersOutsideUpdateLock(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)

	var p *loadBalancer
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		if p != nil {
			acquired := make(chan struct{})
			go func() {
				p.updateLock.RLock()
				p.updateLock.RUnlock()
				close(acquired)
			}()

			select {
			case <-acquired:
			case <-time.After(100 * time.Millisecond):
				return nil, errors.New("componentFactory called while updateLock is held")
			}
		}
		return mockComponent{}, nil
	}

	var err error
	p, err = newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
	decision := p.handleBackendFailure(t.Context(), "endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	decision = p.handleBackendFailure(t.Context(), "endpoint-2:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.failOpen)

	p.updateLock.Lock()
	delete(p.exporters, "endpoint-1:4317")
	p.updateLock.Unlock()

	p.handleBackendSuccess("endpoint-1:4317")
	require.Contains(t, p.exporters, "endpoint-1:4317")
}

func TestLoadBalancerEndpointHealthResolverRecomputesEligibilityBeforeRingInstall(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)

	var shutdowns sync.Map
	var blockedEndpoint1 atomic.Bool
	endpoint1CreateBlocked := make(chan struct{})
	releaseEndpoint1Create := make(chan struct{})
	componentFactory := func(_ context.Context, endpoint string) (component.Component, error) {
		if endpoint == "endpoint-1:4317" && blockedEndpoint1.CompareAndSwap(false, true) {
			close(endpoint1CreateBlocked)
			select {
			case <-releaseEndpoint1Create:
			case <-time.After(5 * time.Second):
				return nil, errors.New("timed out waiting to release endpoint-1 creation")
			}
		}
		return &countingComponent{endpoint: endpoint, shutdowns: &shutdowns}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	resolverDone := make(chan struct{})
	go func() {
		p.onBackendChanges([]string{"endpoint-1", "endpoint-2"})
		close(resolverDone)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-endpoint1CreateBlocked:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	decision := p.handleBackendFailure(t.Context(), "endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	require.Equal(t, []string{"endpoint-2:4317"}, decision.eligible)

	close(releaseEndpoint1Create)
	require.Eventually(t, func() bool {
		select {
		case <-resolverDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	p.updateLock.RLock()
	for _, item := range p.ring.items {
		require.NotEqual(t, "endpoint-1:4317", item.endpoint)
	}
	p.updateLock.RUnlock()

	require.NotContains(t, p.exporters, "endpoint-1:4317")
	require.Contains(t, p.exporters, "endpoint-2:4317")
	require.Eventually(t, func() bool {
		count, ok := shutdowns.Load("endpoint-1:4317")
		return ok && count.(*atomic.Int64).Load() > 0
	}, time.Second, 10*time.Millisecond)

	for i := range 100 {
		_, endpoint, err := p.exporterAndEndpoint([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		require.NoError(t, err)
		assert.Equal(t, "endpoint-2:4317", endpoint)
	}
}

func TestLoadBalancerEndpointHealthResolverCommitUsesLockedEligibility(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	cfg := simpleConfig()
	enableEndpointHealth(cfg)
	var shutdowns sync.Map
	componentFactory := func(_ context.Context, endpoint string) (component.Component, error) {
		return &countingComponent{endpoint: endpoint, shutdowns: &shutdowns}, nil
	}

	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)

	resolved := normalizeEndpoints([]string{"endpoint-1", "endpoint-2"})
	reconcile := p.endpointHealth.reconcile(resolved)
	require.Equal(t, []string{"endpoint-1:4317", "endpoint-2:4317"}, reconcile.eligible)

	staleEndpoint1 := newWrappedExporter(&countingComponent{endpoint: "endpoint-1:4317", shutdowns: &shutdowns}, "endpoint-1:4317")
	endpoint2 := newWrappedExporter(&countingComponent{endpoint: "endpoint-2:4317", shutdowns: &shutdowns}, "endpoint-2:4317")
	created := []createdExporter{
		{endpoint: "endpoint-1:4317", exporter: staleEndpoint1},
		{endpoint: "endpoint-2:4317", exporter: endpoint2},
	}

	decision := p.endpointHealth.markFailure("endpoint-1:4317", status.Error(codes.Unavailable, "unavailable"))
	require.True(t, decision.quarantined)
	require.Equal(t, []string{"endpoint-2:4317"}, decision.eligible)

	p.updateLock.Lock()
	duplicates, removed := p.commitEndpointHealthResolverUpdateLocked(resolved, created)
	p.updateLock.Unlock()
	p.shutdownCreatedExporters(t.Context(), duplicates)
	p.drainRemovedExporters(t.Context(), removed)

	require.Contains(t, p.exporters, "endpoint-2:4317")
	require.Same(t, endpoint2, p.exporters["endpoint-2:4317"])
	require.NotContains(t, p.exporters, "endpoint-1:4317")
	require.Eventually(t, func() bool {
		count, ok := shutdowns.Load("endpoint-1:4317")
		return ok && count.(*atomic.Int64).Load() > 0
	}, time.Second, 10*time.Millisecond)

	for i := range 100 {
		_, endpoint, err := p.exporterAndEndpoint([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		require.NoError(t, err)
		assert.Equal(t, "endpoint-2:4317", endpoint)
	}
}

func TestEndpointWithPort(t *testing.T) {
	for _, tt := range []struct {
		input, expected string
	}{
		{
			"endpoint-1",
			"endpoint-1:4317",
		},
		{
			"endpoint-1:55690",
			"endpoint-1:55690",
		},
	} {
		assert.Equal(t, tt.expected, endpointWithPort(tt.input))
	}
}

func TestFailedExporterInRing(t *testing.T) {
	// this test is based on the discussion in the original PR for this exporter:
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/1542#discussion_r521268180
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2"}}),
		},
	}
	componentFactory := func(_ context.Context, _ string) (component.Component, error) {
		return newNopMockExporter(), nil
	}
	p, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NotNil(t, p)
	require.NoError(t, err)

	err = p.Start(t.Context(), componenttest.NewNopHost())
	require.NoError(t, err)

	// simulate the case where one of the exporters failed to be created and do not exist in the internal map
	// this is a case that we are not even sure that might happen, so, this test case is here to document
	// this behavior. As the solution would require more locks/syncs/checks, we should probably wait to see
	// if this is really a problem in the real world
	resEndpoint := "endpoint-2"
	delete(p.exporters, endpointWithPort(resEndpoint))

	// sanity check
	require.Contains(t, p.res.(*staticResolver).endpoints, resEndpoint)

	// test
	// this trace ID will reach the endpoint-2 -- see the consistent hashing tests for more info
	_, _, err = p.exporterAndEndpoint([]byte{128, 128, 1, 0})

	// verify
	assert.Error(t, err)

	// test
	// this service name will reach the endpoint-2 -- see the consistent hashing tests for more info
	_, _, err = p.exporterAndEndpoint([]byte("get-recommendations-2"))

	// verify
	assert.Error(t, err)
}

func TestNewLoadBalancerInvalidNamespaceAwsResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			AWSCloudMap: configoptional.Some(AWSCloudMapResolver{
				NamespaceName: "",
			}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	assert.Nil(t, p)
	assert.True(t, clientcmd.IsConfigurationInvalid(err) || errors.Is(err, errNoNamespace))
}

func TestNewLoadBalancerInvalidServiceAwsResolver(t *testing.T) {
	// prepare
	ts, tb := getTelemetryAssets(t)
	cfg := &Config{
		Resolver: ResolverSettings{
			AWSCloudMap: configoptional.Some(AWSCloudMapResolver{
				NamespaceName: "cloudmap",
				ServiceName:   "",
			}),
		},
	}

	// test
	p, err := newLoadBalancer(ts.Logger, cfg, nil, tb)

	// verify
	assert.Nil(t, p)
	assert.True(t, clientcmd.IsConfigurationInvalid(err) || errors.Is(err, errNoServiceName))
}

func newNopMockExporter() *wrappedExporter {
	return newWrappedExporter(mockComponent{}, "mock")
}

func enableEndpointHealth(cfg *Config) {
	cfg.EndpointHealth.Enabled = true
	cfg.EndpointHealth.QuarantineDuration = time.Minute
	cfg.EndpointHealth.RerouteOnFailure = true
	cfg.EndpointHealth.MaxRerouteAttempts = 1
}

type countingComponent struct {
	endpoint  string
	shutdowns *sync.Map
}

func (*countingComponent) Start(context.Context, component.Host) error {
	return nil
}

func (c *countingComponent) Shutdown(context.Context) error {
	count, _ := c.shutdowns.LoadOrStore(c.endpoint, &atomic.Int64{})
	count.(*atomic.Int64).Add(1)
	return nil
}
