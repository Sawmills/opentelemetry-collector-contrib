// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/pager"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

type centralQueueMembership struct {
	dns                *centralQueueDNSMembership
	informer           cache.SharedInformer
	service, namespace string
	logger             *zap.Logger
	timeout            time.Duration
	mu                 sync.Mutex
	freshUntil         time.Time
	stopped            bool
	cancel             context.CancelFunc
	done               chan struct{}
	telemetry          *metadata.TelemetryBuilder
	slices             map[string]*discoveryv1.EndpointSlice
	count              int
	initialized        bool
	fallback           bool
	fallbackReplicas   int
	failureReported    bool
}

func centralQueueMembershipFromConfig(cfg CentralQueueConfig, logger *zap.Logger) (*centralQueueMembership, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.LoadBalancerDNS != nil {
		return &centralQueueMembership{dns: &centralQueueDNSMembership{config: cfg.LoadBalancerDNS.withDefaults(), resolver: net.DefaultResolver, now: time.Now}, logger: logger}, nil
	}
	if cfg.LoadBalancerService == "" {
		return nil, nil
	}
	cl, err := newInClusterClient()
	if err != nil {
		return nil, err
	}
	m, err := newCentralQueueMembership(cl, logger, cfg.LoadBalancerService, defaultListWatchTimeout)
	if err == nil {
		m.fallbackReplicas = configuredCentralQueueActiveLBReplicas(cfg.ActiveLoadBalancerReplicas)
	}
	return m, err
}

func (m *centralQueueMembership) bind(ctx context.Context, controller *centralQueueConsumerController, telemetry *centralQueueTelemetry, resolverTelemetry *metadata.TelemetryBuilder) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.telemetry = resolverTelemetry
	m.mu.Unlock()
	lastCount := -1
	controller.setMembershipSource(func() (int, bool) {
		count, known := m.snapshot()
		if count != lastCount {
			telemetry.recordActiveLoadBalancerReplicas(ctx, int64(count))
			lastCount = count
		}
		return count, known
	})
	m.start(context.Background())
}

func newCentralQueueMembership(cl kubernetes.Interface, logger *zap.Logger, service string, timeout time.Duration) (*centralQueueMembership, error) {
	if timeout <= 0 {
		timeout = defaultListWatchTimeout
	}
	if !strings.Contains(service, ".") {
		ns, err := getInClusterNamespace()
		if err != nil {
			return nil, fmt.Errorf("discover load balancer membership namespace: %w", err)
		}
		service += "." + strings.TrimSpace(ns)
	}
	name, ns, lw, err := newK8sServiceListWatcher(cl, logger, service, timeout)
	if err != nil {
		return nil, err
	}
	m := &centralQueueMembership{service: name, namespace: ns, logger: logger, timeout: timeout, slices: make(map[string]*discoveryv1.EndpointSlice), fallbackReplicas: 1}
	tracked := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			m.beginAttempt()
			ctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			obj, _, err := pager.New(lw.ListWithContext).ListWithAlloc(ctx, options)
			var objects []runtime.Object
			if err == nil {
				objects, err = meta.ExtractList(obj)
			}
			m.mu.Lock()
			if err == nil {
				m.slices = make(map[string]*discoveryv1.EndpointSlice)
				for _, object := range objects {
					slice := object.(*discoveryv1.EndpointSlice)
					m.slices[slice.Namespace+"/"+slice.Name] = slice
				}
				m.initialized = true
				m.recountLocked()
			}
			m.observeLocked(err, "list")
			m.mu.Unlock()
			return obj, err
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			m.beginAttempt()
			if options.SendInitialEvents != nil && *options.SendInitialEvents {
				m.mu.Lock()
				m.initialized = false
				m.slices = make(map[string]*discoveryv1.EndpointSlice)
				m.mu.Unlock()
			}
			w, err := lw.WatchWithContext(ctx, options)
			m.observe(err)
			if err != nil {
				return nil, err
			}
			return newMembershipWatch(ctx, w, func(event watch.Event) {
				m.mu.Lock()
				defer m.mu.Unlock()
				if event.Type == watch.Error {
					m.failLocked("watch", apierrors.FromObject(event.Object))
				}
				if slice, ok := event.Object.(*discoveryv1.EndpointSlice); ok {
					key := slice.Namespace + "/" + slice.Name
					switch event.Type {
					case watch.Added, watch.Modified:
						m.slices[key] = slice
					case watch.Deleted:
						delete(m.slices, key)
					}
					m.recountLocked()
				}
				if event.Type == watch.Bookmark {
					if obj, ok := event.Object.(metav1.Object); ok && obj.GetAnnotations()["k8s.io/initial-events-end"] == "true" {
						m.initialized = true
					}
				}
			}), nil
		},
	}
	m.informer = cache.NewSharedInformer(tracked, &discoveryv1.EndpointSlice{}, 0)
	if err := m.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.failLocked("watch", err)
	}); err != nil {
		return nil, err
	}
	return m, nil
}

type membershipWatch struct {
	source watch.Interface
	result chan watch.Event
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func newMembershipWatch(ctx context.Context, source watch.Interface, observe func(watch.Event)) watch.Interface {
	w := &membershipWatch{source: source, result: make(chan watch.Event), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		defer close(w.result)
		defer w.stopSource()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stop:
				return
			case event, ok := <-source.ResultChan():
				if !ok {
					return
				}
				observe(event)
				select {
				case w.result <- event:
				case <-ctx.Done():
					return
				case <-w.stop:
					return
				}
			}
		}
	}()
	return w
}

func (w *membershipWatch) ResultChan() <-chan watch.Event { return w.result }
func (w *membershipWatch) Stop()                          { w.stopSource(); <-w.done }
func (w *membershipWatch) stopSource()                    { w.once.Do(func() { close(w.stop); w.source.Stop() }) }

func (m *centralQueueMembership) observe(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observeLocked(err, "watch")
}

func (m *centralQueueMembership) beginAttempt() {
	m.mu.Lock()
	m.failureReported = false
	m.mu.Unlock()
}

func (m *centralQueueMembership) failLocked(operation string, err error) {
	m.fallback = true
	m.freshUntil = time.Time{}
	if m.failureReported {
		return
	}
	m.failureReported = true
	reason := operation + "_failed"
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		reason = operation + "_denied"
	}
	m.recordResolution(false, reason)
	m.logger.Warn("load balancer membership discovery failed; using configured replica behavior", zap.String("reason", reason), zap.String("service", m.service), zap.String("namespace", m.namespace), zap.Error(err))
}

func (m *centralQueueMembership) observeLocked(err error, operation string) {
	if err != nil {
		m.failLocked(operation, err)
		return
	}
	m.fallback = false
	// The API server bounds each watch by timeout. A new successful watch
	// renews freshness even when membership does not change.
	m.freshUntil = time.Now().Add(2 * m.timeout)
	m.recordResolution(true, "")
}

func (m *centralQueueMembership) recordResolution(success bool, reason string) {
	if m.telemetry == nil {
		return
	}
	attrs := []attribute.KeyValue{k8sResolverAttr, attribute.String("scope", "load_balancer_membership"), attribute.Bool("success", success)}
	if reason != "" {
		attrs = append(attrs, attribute.String("reason", reason))
	}
	m.telemetry.LoadbalancerNumResolutions.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (m *centralQueueMembership) start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.cancel != nil {
		return
	}
	ctx, m.cancel = context.WithCancel(ctx)
	m.freshUntil = time.Now().Add(2 * m.timeout)
	m.done = make(chan struct{})
	go func() {
		defer close(m.done)
		if m.dns != nil {
			m.runDNS(ctx)
		} else {
			m.informer.Run(ctx.Done())
		}
		m.mu.Lock()
		m.stopped = true
		m.mu.Unlock()
	}()
}

func (m *centralQueueMembership) snapshot() (int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.cancel == nil {
		return 0, false
	}
	if m.dns != nil {
		if m.initialized && !m.dns.now().Before(m.freshUntil) {
			m.initialized = false
			m.count = 0
			m.recordDNSResolution(false, "stale")
		}
		if !m.initialized {
			return 0, false
		}
		return m.count, m.count > 0
	}
	if !m.fallback && !time.Now().Before(m.freshUntil) {
		m.failLocked("stale", errors.New("membership freshness expired"))
	}
	if m.fallback {
		return m.fallbackReplicas, true
	}
	if !m.initialized {
		return 0, false
	}
	return m.count, m.count > 0
}

func (m *centralQueueMembership) recountLocked() {
	peers := make(map[string]struct{})
	for _, s := range m.slices {
		if s.Namespace != m.namespace || s.Labels[discoveryv1.LabelServiceName] != m.service {
			continue
		}
		for _, ep := range s.Endpoints {
			var key string
			if ref := ep.TargetRef; ref != nil && ref.Kind == "Pod" {
				if ref.UID != "" {
					key = "uid:" + string(ref.UID)
				} else {
					key = "pod:" + ref.Namespace + "/" + ref.Name
				}
			} else {
				addresses := append([]string(nil), ep.Addresses...)
				sort.Strings(addresses)
				if len(addresses) == 0 {
					m.count = 0
					return
				}
				key = "address:" + strings.Join(addresses, ",")
			}
			peers[key] = struct{}{}
		}
	}
	m.count = len(peers)
}

func (m *centralQueueMembership) shutdown() {
	m.mu.Lock()
	m.stopped = true
	if m.cancel != nil {
		m.cancel()
	}
	done := m.done
	m.mu.Unlock()
	if done != nil {
		<-done
	}
}
