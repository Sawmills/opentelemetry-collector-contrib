// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	defaultPort                   = "4317"
	defaultBackendFailureCooldown = 10 * time.Second
)

var (
	errNoResolver                = errors.New("no resolvers specified for the exporter")
	errMultipleResolversProvided = errors.New("only one resolver should be specified")
)

type componentFactory func(ctx context.Context, endpoint string) (component.Component, error)

type loadBalancer struct {
	logger *zap.Logger
	host   component.Host

	res  resolver
	ring *hashRing

	componentFactory componentFactory
	exporters        map[string]*wrappedExporter
	onExporterRemove func(context.Context, string, *wrappedExporter) error
	quarantineUntil  map[string]time.Time
	quarantineAfter  time.Duration

	stopped    bool
	updateLock sync.RWMutex
}

// Create new load balancer
func newLoadBalancer(logger *zap.Logger, cfg component.Config, factory componentFactory, telemetry *metadata.TelemetryBuilder) (*loadBalancer, error) {
	oCfg := cfg.(*Config)

	count := 0
	if oCfg.Resolver.DNS.HasValue() {
		count++
	}
	if oCfg.Resolver.Static.HasValue() {
		count++
	}
	if oCfg.Resolver.AWSCloudMap.HasValue() {
		count++
	}
	if oCfg.Resolver.K8sSvc.HasValue() {
		count++
	}
	if count > 1 {
		return nil, errMultipleResolversProvided
	}

	var res resolver
	if oCfg.Resolver.Static.HasValue() {
		var err error
		res, err = newStaticResolver(
			oCfg.Resolver.Static.Get().Hostnames,
			telemetry,
		)
		if err != nil {
			return nil, err
		}
	}
	if oCfg.Resolver.DNS.HasValue() {
		dnsLogger := logger.With(zap.String("resolver", "dns"))

		var err error
		dnsResolver := oCfg.Resolver.DNS.Get()
		res, err = newDNSResolver(
			dnsLogger,
			dnsResolver.Hostname,
			dnsResolver.Port,
			dnsResolver.Interval,
			dnsResolver.Timeout,
			telemetry,
		)
		if err != nil {
			return nil, err
		}
	}
	if oCfg.Resolver.K8sSvc.HasValue() {
		k8sLogger := logger.With(zap.String("resolver", "k8s service"))

		clt, err := newInClusterClient()
		if err != nil {
			return nil, err
		}
		k8sSvcResolver := oCfg.Resolver.K8sSvc.Get()
		res, err = newK8sResolver(
			clt,
			k8sLogger,
			k8sSvcResolver.Service,
			k8sSvcResolver.Ports,
			k8sSvcResolver.Timeout,
			k8sSvcResolver.ReturnHostnames,
			telemetry,
		)
		if err != nil {
			return nil, err
		}
	}

	if oCfg.Resolver.AWSCloudMap.HasValue() {
		awsCloudMapLogger := logger.With(zap.String("resolver", "aws_cloud_map"))
		awsCloudMapResolver := oCfg.Resolver.AWSCloudMap.Get()
		var err error
		res, err = newCloudMapResolver(
			awsCloudMapLogger,
			&awsCloudMapResolver.NamespaceName,
			&awsCloudMapResolver.ServiceName,
			awsCloudMapResolver.Port,
			&awsCloudMapResolver.HealthStatus,
			awsCloudMapResolver.Interval,
			awsCloudMapResolver.Timeout,
			telemetry,
		)
		if err != nil {
			return nil, err
		}
	}

	if res == nil {
		return nil, errNoResolver
	}

	return &loadBalancer{
		logger:           logger,
		res:              res,
		componentFactory: factory,
		exporters:        map[string]*wrappedExporter{},
		quarantineUntil:  map[string]time.Time{},
		quarantineAfter:  oCfg.backendFailureCooldown(),
	}, nil
}

func (lb *loadBalancer) Start(ctx context.Context, host component.Host) error {
	lb.res.onChange(lb.onBackendChanges)
	lb.host = host
	return lb.res.start(ctx)
}

func (lb *loadBalancer) onBackendChanges(resolved []string) {
	newRing := newHashRing(resolved)

	if newRing.equal(lb.ring) {
		return
	}

	// TODO: set a timeout?
	ctx := context.Background()

	lb.updateLock.Lock()
	lb.ring = newRing

	// add the missing exporters first
	lb.addMissingExporters(ctx, resolved)
	removed := lb.removeExtraExportersLocked(resolved)
	lb.updateLock.Unlock()

	lb.drainRemovedExporters(ctx, removed)
}

type removedExporter struct {
	endpoint string
	exporter *wrappedExporter
}

func (lb *loadBalancer) drainRemovedExporters(ctx context.Context, removed []removedExporter) {
	for _, removedExporter := range removed {
		if lb.onExporterRemove != nil {
			if err := lb.onExporterRemove(ctx, removedExporter.endpoint, removedExporter.exporter); err != nil {
				lb.logger.Error("failed to drain exporter before removal", zap.String("endpoint", removedExporter.endpoint), zap.Error(err))
			}
		}
		// Shutdown the exporter asynchronously to avoid blocking the resolver.
		go func(exp *wrappedExporter) {
			_ = exp.Shutdown(ctx)
		}(removedExporter.exporter)
	}
}

func (lb *loadBalancer) addMissingExporters(ctx context.Context, endpoints []string) {
	for _, endpoint := range endpoints {
		endpoint = endpointWithPort(endpoint)

		if _, exists := lb.exporters[endpoint]; !exists {
			exp, err := lb.componentFactory(ctx, endpoint)
			if err != nil {
				lb.logger.Error("failed to create new exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}
			we := newWrappedExporter(exp, endpoint)
			if err = we.Start(ctx, lb.host); err != nil {
				lb.logger.Error("failed to start new exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}
			lb.exporters[endpoint] = we
		}
	}
}

func endpointWithPort(endpoint string) string {
	if !strings.Contains(endpoint, ":") {
		endpoint = fmt.Sprintf("%s:%s", endpoint, defaultPort)
	}
	return endpoint
}

func (lb *loadBalancer) removeExtraExportersLocked(endpoints []string) []removedExporter {
	endpointsWithPort := make([]string, len(endpoints))
	for i, e := range endpoints {
		endpointsWithPort[i] = endpointWithPort(e)
	}

	var removed []removedExporter
	for existing := range lb.exporters {
		if slices.Contains(endpointsWithPort, existing) {
			continue
		}

		exp := lb.exporters[existing]
		exp.markStopping()
		delete(lb.exporters, existing)
		delete(lb.quarantineUntil, existing)
		removed = append(removed, removedExporter{endpoint: existing, exporter: exp})
	}

	return removed
}

func (lb *loadBalancer) Shutdown(ctx context.Context) error {
	err := lb.res.shutdown(ctx)
	lb.stopped = true

	for _, e := range lb.exporters {
		err = errors.Join(err, e.Shutdown(ctx))
	}
	return err
}

func (lb *loadBalancer) withExporterAndEndpoint(identifier []byte, fn func(*wrappedExporter, string) error) error {
	lb.updateLock.RLock()
	defer lb.updateLock.RUnlock()

	candidates := lb.ring.candidateEndpointsFor(identifier, len(lb.exporters))
	if len(candidates) == 0 {
		return fmt.Errorf("couldn't find the exporter for the endpoint %q", "")
	}

	var fallbackExp *wrappedExporter
	var fallbackEndpoint string
	now := time.Now()
	for _, endpoint := range candidates {
		exp, found := lb.exporters[endpointWithPort(endpoint)]
		if !found {
			continue
		}
		if fallbackExp == nil {
			fallbackExp = exp
			fallbackEndpoint = endpoint
		}
		until, quarantined := lb.quarantineUntil[endpointWithPort(endpoint)]
		if quarantined && now.Before(until) {
			continue
		}
		return fn(exp, endpoint)
	}

	if fallbackExp != nil {
		return fn(fallbackExp, fallbackEndpoint)
	}

	return errors.New("couldn't find the exporter for any candidate endpoint")
}

// exporterAndEndpoint returns the exporter and the endpoint for the given identifier.
func (lb *loadBalancer) exporterAndEndpoint(identifier []byte) (*wrappedExporter, string, error) {
	var matchedExporter *wrappedExporter
	var matchedEndpoint string
	err := lb.withExporterAndEndpoint(identifier, func(exp *wrappedExporter, endpoint string) error {
		matchedExporter = exp
		matchedEndpoint = endpoint
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	return matchedExporter, matchedEndpoint, nil
}

func (lb *loadBalancer) quarantineEndpoint(endpoint string) {
	if endpoint == "" || lb.quarantineAfter <= 0 {
		return
	}

	lb.updateLock.Lock()
	defer lb.updateLock.Unlock()
	lb.quarantineUntil[endpointWithPort(endpoint)] = time.Now().Add(lb.quarantineAfter)
}
