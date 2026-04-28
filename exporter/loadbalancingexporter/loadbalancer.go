// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	defaultPort = "4317"
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

	componentFactory  componentFactory
	exporters         map[string]*wrappedExporter
	onExporterRemove  func(context.Context, string, *wrappedExporter) error
	telemetry         *metadata.TelemetryBuilder
	endpointHealth    *endpointHealthManager
	resolvedEndpoints []string

	stopped   bool
	cleanupMu sync.Mutex
	cleanupWG sync.WaitGroup

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
		telemetry:        telemetry,
		endpointHealth:   newEndpointHealthManager(endpointHealthSettingsFromConfig(oCfg.EndpointHealth)),
	}, nil
}

func (lb *loadBalancer) Start(ctx context.Context, host component.Host) error {
	lb.res.onChange(lb.onBackendChanges)
	lb.host = host
	return lb.res.start(ctx)
}

func (lb *loadBalancer) onBackendChanges(resolved []string) {
	if lb.endpointHealth.enabled() {
		lb.onBackendChangesWithEndpointHealth(resolved)
		return
	}

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

	if len(removed) > 0 {
		lb.runCleanup(func() {
			lb.drainRemovedExporters(ctx, removed)
		})
	}
}

func (lb *loadBalancer) onBackendChangesWithEndpointHealth(resolved []string) {
	resolved = normalizeEndpoints(resolved)
	reconcile := lb.endpointHealth.reconcile(resolved)

	// TODO: set a timeout?
	ctx := context.Background()
	lb.recordEndpointHealthReconcile(ctx, reconcile)
	created := lb.createEndpointHealthMissingExporters(ctx, reconcile.eligible)
	eligible := lb.endpointHealth.eligibleEndpoints()

	lb.updateLock.Lock()
	duplicates, removed := lb.commitEndpointHealthResolverUpdateLocked(resolved, eligible, created)
	lb.updateLock.Unlock()

	lb.shutdownCreatedExporters(ctx, duplicates)
	if len(removed) > 0 {
		lb.runCleanup(func() {
			lb.drainRemovedExporters(ctx, removed)
		})
	}
}

func (lb *loadBalancer) commitEndpointHealthResolverUpdateLocked(resolved []string, eligible []string, created []createdExporter) ([]createdExporter, []removedExporter) {
	lb.ring = newHashRing(eligible)
	lb.resolvedEndpoints = resolved

	duplicates := lb.installCreatedExportersLocked(created, eligible)
	removed := lb.removeExtraExportersLocked(resolved)
	return duplicates, removed
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
		exp := removedExporter.exporter
		// Shutdown the exporter asynchronously to avoid blocking the resolver.
		lb.runCleanupAsync(func() {
			_ = exp.Shutdown(ctx)
		})
	}
}

func (lb *loadBalancer) beginCleanup() (func(), bool) {
	lb.cleanupMu.Lock()
	if lb.stopped {
		lb.cleanupMu.Unlock()
		return func() {}, false
	}
	lb.cleanupWG.Add(1)
	lb.cleanupMu.Unlock()

	return lb.cleanupWG.Done, true
}

func (lb *loadBalancer) runCleanup(fn func()) {
	done, _ := lb.beginCleanup()
	defer done()
	fn()
}

func (lb *loadBalancer) runCleanupAsync(fn func()) {
	done, async := lb.beginCleanup()
	if !async {
		fn()
		return
	}

	go func() {
		defer done()
		fn()
	}()
}

func (lb *loadBalancer) waitForAsyncCleanup(ctx context.Context) error {
	return waitForInflight(ctx, &lb.cleanupWG)
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

type createdExporter struct {
	endpoint string
	exporter *wrappedExporter
}

func (lb *loadBalancer) createMissingExporters(ctx context.Context, endpoints []string, forceCreate map[string]struct{}) []createdExporter {
	return lb.createMissingExportersWithGuard(ctx, endpoints, forceCreate, nil)
}

func (lb *loadBalancer) createEndpointHealthMissingExporters(ctx context.Context, endpoints []string) []createdExporter {
	return lb.createMissingExportersWithGuard(ctx, endpoints, nil, lb.endpointHealthCurrentlyEligible)
}

func (lb *loadBalancer) createMissingExportersWithGuard(ctx context.Context, endpoints []string, forceCreate map[string]struct{}, shouldCreate func(string) bool) []createdExporter {
	var created []createdExporter
	for _, endpoint := range endpoints {
		endpoint = endpointWithPort(endpoint)
		if shouldCreate != nil && !shouldCreate(endpoint) {
			continue
		}

		if _, force := forceCreate[endpoint]; !force {
			lb.updateLock.RLock()
			_, exists := lb.exporters[endpoint]
			lb.updateLock.RUnlock()
			if exists {
				continue
			}
		}

		exp, err := lb.componentFactory(ctx, endpoint)
		if err != nil {
			lb.logger.Error("failed to create new exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
			continue
		}
		if shouldCreate != nil && !shouldCreate(endpoint) {
			continue
		}
		we := newWrappedExporter(exp, endpoint)
		if err = we.Start(ctx, lb.host); err != nil {
			lb.logger.Error("failed to start new exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
			continue
		}
		created = append(created, createdExporter{endpoint: endpoint, exporter: we})
	}
	return created
}

func (lb *loadBalancer) endpointHealthCurrentlyEligible(endpoint string) bool {
	return slices.Contains(lb.endpointHealth.eligibleEndpoints(), endpointWithPort(endpoint))
}

func (lb *loadBalancer) installCreatedExportersLocked(created []createdExporter, endpoints []string) []createdExporter {
	var duplicates []createdExporter
	eligible := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		eligible[endpointWithPort(endpoint)] = struct{}{}
	}
	for _, createdExporter := range created {
		if _, ok := eligible[createdExporter.endpoint]; !ok {
			createdExporter.exporter.markStopping()
			duplicates = append(duplicates, createdExporter)
			continue
		}
		if _, exists := lb.exporters[createdExporter.endpoint]; exists {
			createdExporter.exporter.markStopping()
			duplicates = append(duplicates, createdExporter)
			continue
		}
		lb.exporters[createdExporter.endpoint] = createdExporter.exporter
	}
	return duplicates
}

func (lb *loadBalancer) shutdownCreatedExporters(ctx context.Context, created []createdExporter) {
	for _, createdExporter := range created {
		exp := createdExporter.exporter
		lb.runCleanupAsync(func() {
			_ = exp.Shutdown(ctx)
		})
	}
}

func endpointWithPort(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	if !strings.Contains(endpoint, ":") {
		endpoint = fmt.Sprintf("%s:%s", endpoint, defaultPort)
	}
	return endpoint
}

func normalizeEndpoints(endpoints []string) []string {
	normalized := make([]string, 0, len(endpoints))
	seen := make(map[string]struct{}, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint = endpointWithPort(endpoint)
		if endpoint == "" {
			continue
		}
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		normalized = append(normalized, endpoint)
	}
	sort.Strings(normalized)
	return normalized
}

func (lb *loadBalancer) removeExtraExportersLocked(endpoints []string) []removedExporter {
	endpointsWithPort := make([]string, len(endpoints))
	for i, e := range endpoints {
		endpointsWithPort[i] = endpointWithPort(e)
	}

	var removed []removedExporter
	for existing := range lb.exporters {
		if !slices.Contains(endpointsWithPort, existing) {
			exp := lb.exporters[existing]
			exp.markStopping()
			delete(lb.exporters, existing)
			removed = append(removed, removedExporter{endpoint: existing, exporter: exp})
		}
	}

	return removed
}

func (lb *loadBalancer) handleBackendFailure(ctx context.Context, endpoint string, err error) endpointHealthFailureDecision {
	return lb.handleBackendFailureWithDrain(ctx, endpoint, err, true)
}

func (lb *loadBalancer) handleBackendFailureWithoutDrain(ctx context.Context, endpoint string, err error) endpointHealthFailureDecision {
	return lb.handleBackendFailureWithDrain(ctx, endpoint, err, false)
}

func (lb *loadBalancer) handleBackendFailureHealthOnly(ctx context.Context, endpoint string, err error) endpointHealthFailureDecision {
	endpoint = endpointWithPort(endpoint)
	decision := lb.endpointHealth.markFailure(endpoint, err)
	lb.recordEndpointFailureDecision(ctx, endpoint, decision)
	if !shouldCommitEndpointHealthFailure(endpoint, decision) {
		return decision
	}

	forceCreate := map[string]struct{}{}
	if slices.Contains(decision.eligible, endpoint) {
		forceCreate[endpoint] = struct{}{}
	}
	created := lb.createMissingExporters(ctx, decision.eligible, forceCreate)

	lb.updateLock.Lock()
	lb.ring = newHashRing(decision.eligible)
	var removed []removedExporter
	if _, refreshRequested := forceCreate[endpoint]; refreshRequested && slices.ContainsFunc(created, func(created createdExporter) bool {
		return created.endpoint == endpoint
	}) {
		if exp, ok := lb.exporters[endpoint]; ok {
			exp.markStopping()
			delete(lb.exporters, endpoint)
			removed = append(removed, removedExporter{endpoint: endpoint, exporter: exp})
		}
	}
	duplicates := lb.installCreatedExportersLocked(created, decision.eligible)
	lb.updateLock.Unlock()

	lb.shutdownCreatedExporters(ctx, duplicates)
	if len(removed) > 0 {
		lb.runCleanupAsync(func() {
			lb.drainRemovedExporters(context.WithoutCancel(ctx), removed)
		})
	}
	return decision
}

func (lb *loadBalancer) cleanupBackendWithoutDrain(ctx context.Context, endpoint string) {
	endpoint = endpointWithPort(endpoint)
	lb.updateLock.Lock()
	var removed []removedExporter
	if exp, ok := lb.exporters[endpoint]; ok {
		exp.markStopping()
		delete(lb.exporters, endpoint)
		removed = append(removed, removedExporter{endpoint: endpoint, exporter: exp})
	}
	lb.updateLock.Unlock()
	if len(removed) > 0 {
		lb.runCleanupAsync(func() {
			lb.drainRemovedExporters(context.WithoutCancel(ctx), removed)
		})
	}
}

func (lb *loadBalancer) handleBackendFailureWithDrain(ctx context.Context, endpoint string, err error, drainRemoved bool) endpointHealthFailureDecision {
	endpoint = endpointWithPort(endpoint)
	decision := lb.endpointHealth.markFailure(endpoint, err)
	lb.recordEndpointFailureDecision(ctx, endpoint, decision)
	if !shouldCommitEndpointHealthFailure(endpoint, decision) {
		return decision
	}

	forceCreate := map[string]struct{}{}
	if slices.Contains(decision.eligible, endpoint) {
		forceCreate[endpoint] = struct{}{}
	}
	created := lb.createMissingExporters(ctx, decision.eligible, forceCreate)

	lb.updateLock.Lock()
	lb.ring = newHashRing(decision.eligible)

	var removed []removedExporter
	if exp, ok := lb.exporters[endpoint]; ok {
		exp.markStopping()
		delete(lb.exporters, endpoint)
		removed = append(removed, removedExporter{endpoint: endpoint, exporter: exp})
	}
	duplicates := lb.installCreatedExportersLocked(created, decision.eligible)
	lb.updateLock.Unlock()

	lb.shutdownCreatedExporters(ctx, duplicates)
	if drainRemoved {
		if len(removed) > 0 {
			lb.runCleanup(func() {
				lb.drainRemovedExporters(ctx, removed)
			})
		}
	} else if len(removed) > 0 {
		lb.runCleanupAsync(func() {
			lb.drainRemovedExporters(context.WithoutCancel(ctx), removed)
		})
	}
	return decision
}

func shouldCommitEndpointHealthFailure(endpoint string, decision endpointHealthFailureDecision) bool {
	if decision.quarantined {
		return true
	}
	return decision.endpointLocal && !decision.failOpen && !slices.Contains(decision.eligible, endpointWithPort(endpoint))
}

func (lb *loadBalancer) handleBackendSuccess(endpoint string) {
	if !lb.endpointHealth.enabled() {
		return
	}

	endpoint = endpointWithPort(endpoint)
	decision := lb.endpointHealth.markSuccessDecision(endpoint)
	if !decision.recovered {
		return
	}
	ctx := context.Background()
	lb.recordEndpointUnquarantine(ctx, endpoint, decision.reason)
	eligible := decision.eligible
	created := lb.createMissingExporters(ctx, eligible, nil)

	lb.updateLock.Lock()
	lb.ring = newHashRing(eligible)
	duplicates := lb.installCreatedExportersLocked(created, eligible)
	lb.updateLock.Unlock()

	lb.shutdownCreatedExporters(ctx, duplicates)
}

func (lb *loadBalancer) refreshExpiredEndpointHealth(ctx context.Context) {
	if !lb.endpointHealth.quarantineRefreshDue() {
		return
	}

	refresh := lb.endpointHealth.refreshExpiredQuarantines()
	if len(refresh.recovered) == 0 && !refresh.failOpenStarted {
		return
	}
	for _, recovered := range refresh.recovered {
		lb.recordEndpointUnquarantine(ctx, recovered.endpoint, recovered.reason)
	}
	if refresh.failOpenStarted {
		lb.recordEndpointFailOpen(ctx)
	}

	created := lb.createMissingExporters(ctx, refresh.eligible, nil)
	lb.updateLock.Lock()
	lb.ring = newHashRing(refresh.eligible)
	duplicates := lb.installCreatedExportersLocked(created, refresh.eligible)
	lb.updateLock.Unlock()

	lb.shutdownCreatedExporters(ctx, duplicates)
}

func (lb *loadBalancer) recordEndpointHealthReconcile(ctx context.Context, reconcile endpointHealthReconcileResult) {
	for _, endpoint := range reconcile.removed {
		lb.recordEndpointStale(ctx, endpoint)
	}
	if reconcile.failOpenStarted {
		lb.recordEndpointFailOpen(ctx)
	}
	for _, endpoint := range reconcile.eligible {
		lb.recordEndpointState(ctx, endpoint, "quarantined", 0)
		lb.recordEndpointState(ctx, endpoint, "stale", 0)
		lb.recordEndpointState(ctx, endpoint, "eligible", 1)
	}
}

func (lb *loadBalancer) recordEndpointFailureDecision(ctx context.Context, endpoint string, decision endpointHealthFailureDecision) {
	if decision.quarantined {
		lb.recordEndpointQuarantine(ctx, endpoint, decision.reason)
	}
	if decision.failOpenStarted {
		lb.recordEndpointFailOpen(ctx)
	}
}

func (lb *loadBalancer) recordEndpointQuarantine(ctx context.Context, endpoint string, reason endpointFailureReason) {
	if lb.telemetry == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("endpoint", endpoint), attribute.String("reason", string(reason)))
	lb.telemetry.LoadbalancerBackendQuarantineTotal.Add(ctx, 1, attrs)
	lb.recordEndpointState(ctx, endpoint, "eligible", 0)
	lb.recordEndpointState(ctx, endpoint, "quarantined", 1)
}

func (lb *loadBalancer) recordEndpointUnquarantine(ctx context.Context, endpoint string, reason endpointFailureReason) {
	if lb.telemetry == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("endpoint", endpoint), attribute.String("reason", string(reason)))
	lb.telemetry.LoadbalancerBackendUnquarantineTotal.Add(ctx, 1, attrs)
	lb.recordEndpointState(ctx, endpoint, "quarantined", 0)
	lb.recordEndpointState(ctx, endpoint, "eligible", 1)
}

func (lb *loadBalancer) recordEndpointStale(ctx context.Context, endpoint string) {
	if lb.telemetry == nil {
		return
	}
	lb.telemetry.LoadbalancerBackendStaleTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", endpoint)))
	lb.recordEndpointState(ctx, endpoint, "eligible", 0)
	lb.recordEndpointState(ctx, endpoint, "quarantined", 0)
	lb.recordEndpointState(ctx, endpoint, "stale", 1)
}

func (lb *loadBalancer) recordEndpointFailOpen(ctx context.Context) {
	if lb.telemetry == nil {
		return
	}
	lb.telemetry.LoadbalancerBackendFailOpenTotal.Add(ctx, 1)
}

func (lb *loadBalancer) recordEndpointState(ctx context.Context, endpoint, state string, value int64) {
	if lb.telemetry == nil {
		return
	}
	lb.telemetry.LoadbalancerBackendState.Record(ctx, value, metric.WithAttributes(attribute.String("endpoint", endpoint), attribute.String("state", state)))
}

func (lb *loadBalancer) recordBackendReroute(ctx context.Context, signal string, reason endpointFailureReason, err error) {
	if lb.telemetry == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "failure"
	}
	lb.telemetry.LoadbalancerBackendRerouteTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("signal", signal),
		attribute.String("result", result),
		attribute.String("reason", string(reason)),
	))
}

func (lb *loadBalancer) Shutdown(ctx context.Context) error {
	err := lb.res.shutdown(ctx)

	lb.cleanupMu.Lock()
	lb.stopped = true
	lb.cleanupMu.Unlock()

	err = errors.Join(err, lb.waitForAsyncCleanup(ctx))

	for _, e := range lb.exporters {
		err = errors.Join(err, e.Shutdown(ctx))
	}
	return err
}

// exporterAndEndpoint returns the exporter and the endpoint for the given identifier.
func (lb *loadBalancer) exporterAndEndpoint(identifier []byte) (*wrappedExporter, string, error) {
	lb.refreshExpiredEndpointHealth(context.Background())

	lb.updateLock.RLock()
	defer lb.updateLock.RUnlock()

	endpoint := lb.ring.endpointFor(identifier)
	exp, found := lb.exporters[endpointWithPort(endpoint)]
	if !found {
		// something is really wrong... how come we couldn't find the exporter??
		return nil, "", fmt.Errorf("couldn't find the exporter for the endpoint %q", endpoint)
	}

	return exp, endpointWithPort(endpoint), nil
}
