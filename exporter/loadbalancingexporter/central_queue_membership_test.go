// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func membershipSlice(name, service, namespace string, peers ...string) *discoveryv1.EndpointSlice {
	s := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{discoveryv1.LabelServiceName: service}}, AddressType: discoveryv1.AddressTypeIPv4}
	for i, peer := range peers {
		s.Endpoints = append(s.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.0.0." + string(rune('1'+i))},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: namespace, Name: peer, UID: types.UID(peer)},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false), Terminating: ptr.To(true)},
		})
	}
	return s
}

func TestCentralQueueMembershipContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m, err := newCentralQueueMembership(fake.NewClientset(membershipSlice("a", "lb", "customer", "one")), zap.NewNop(), "lb.customer", time.Minute)
	require.NoError(t, err)
	m.start(ctx)
	t.Cleanup(m.shutdown)
	requireMembership(t, m, 1, true)
	cancel()
	requireMembership(t, m, 0, false)
}

func TestCentralQueueMembershipExpiry(t *testing.T) {
	m, err := newCentralQueueMembership(fake.NewClientset(membershipSlice("a", "lb", "customer", "one", "two", "three", "four", "five", "six")), zap.NewNop(), "lb.customer", 250*time.Millisecond)
	require.NoError(t, err)
	m.start(t.Context())
	t.Cleanup(m.shutdown)
	requireMembership(t, m, 6, true)
	// Expired discovery restores configured behavior instead of blocking sends.
	requireMembership(t, m, 1, true)
}

func TestCentralQueueMembershipListWatchFailureRecovery(t *testing.T) {
	cl := fake.NewClientset(membershipSlice("a", "lb", "customer", "one", "two", "three", "four", "five", "six"))
	var listFailed, watchFailed atomic.Bool
	listFailed.Store(true)
	cl.PrependReactor("list", "endpointslices", func(ktesting.Action) (bool, runtime.Object, error) {
		if listFailed.Load() {
			return true, nil, errors.New("list unavailable")
		}
		return false, nil, nil
	})
	var watcher atomic.Pointer[watch.RaceFreeFakeWatcher]
	watcher.Store(watch.NewRaceFreeFake())
	cl.PrependWatchReactor("endpointslices", func(ktesting.Action) (bool, watch.Interface, error) {
		if watchFailed.Load() {
			return true, nil, errors.New("watch unavailable")
		}
		return true, watcher.Load(), nil
	})
	m, err := newCentralQueueMembership(cl, zap.NewNop(), "lb.customer", time.Minute)
	require.NoError(t, err)
	_, tb, reader := getTelemetryAssetsWithReader(t)
	m.bind(t.Context(), newCentralQueueConsumerController(30, 256<<10, 1), nil, tb)
	t.Cleanup(m.shutdown)
	require.Eventually(t, func() bool {
		metric, readErr := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
		if readErr != nil {
			return false
		}
		data := metric.Data.(metricdata.Sum[int64])
		for _, point := range data.DataPoints {
			if success, ok := point.Attributes.Value("success"); ok && !success.AsBool() {
				return point.Value == 1
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	requireCentralQueueIntSum(t, reader, "otelcol_loadbalancer_num_resolutions", "{resolutions}", attribute.NewSet(k8sResolverAttr, attribute.String("scope", "load_balancer_membership"), attribute.Bool("success", false), attribute.String("reason", "list_failed")), 1)
	requireMembership(t, m, 1, true)
	listFailed.Store(false)
	requireMembership(t, m, 6, true)
	watchFailed.Store(true)
	watcher.Load().Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired})
	watcher.Load().Stop()
	requireMembership(t, m, 1, true)
	// A new list and watch restore a complete membership view.
	watcher.Store(watch.NewRaceFreeFake())
	watchFailed.Store(false)
	requireMembership(t, m, 6, true)
}

func requireMembership(t *testing.T, m *centralQueueMembership, count int, known bool) {
	t.Helper()
	require.Eventually(t, func() bool { n, ok := m.snapshot(); return n == count && ok == known }, 8*time.Second, 5*time.Millisecond)
}

func TestCentralQueueMembershipScaleAndIsolation(t *testing.T) {
	ctx := t.Context()
	a := membershipSlice("a", "collector-lb", "customer", "one", "two")
	b := membershipSlice("b", "collector-lb", "customer", "two", "three")
	b.AddressType = discoveryv1.AddressTypeIPv6
	for i := range b.Endpoints {
		b.Endpoints[i].Addresses = []string{"::" + string(rune('1'+i))}
	}
	cl := fake.NewClientset(a, b,
		membershipSlice("foreign-service", "other-lb", "customer", "foreign"),
		membershipSlice("foreign-namespace", "collector-lb", "other", "foreign"))
	m, err := newCentralQueueMembership(cl, zap.NewNop(), "collector-lb.customer.svc.cluster.local", time.Minute)
	require.NoError(t, err)
	requireMembership(t, m, 0, false)
	m.start(ctx)
	t.Cleanup(m.shutdown)
	requireMembership(t, m, 3, true)
	// Deleting a duplicate slice must retain peers still present in another slice.
	require.NoError(t, cl.DiscoveryV1().EndpointSlices("customer").Delete(ctx, "a", metav1.DeleteOptions{}))
	requireMembership(t, m, 2, true)
	b.Endpoints = append(b.Endpoints, membershipSlice("", "", "customer", "four").Endpoints...)
	_, err = cl.DiscoveryV1().EndpointSlices("customer").Update(ctx, b, metav1.UpdateOptions{})
	require.NoError(t, err)
	requireMembership(t, m, 3, true)
	require.NoError(t, cl.DiscoveryV1().EndpointSlices("customer").Delete(ctx, "b", metav1.DeleteOptions{}))
	requireMembership(t, m, 0, false)
	_, err = cl.DiscoveryV1().EndpointSlices("customer").Create(ctx, a, metav1.CreateOptions{})
	require.NoError(t, err)
	requireMembership(t, m, 2, true)
	m.shutdown()
	requireMembership(t, m, 0, false)
}

func TestCentralQueueMembershipDoesNotResumeFromOldInformerCache(t *testing.T) {
	s := membershipSlice("a", "lb", "customer", "one")
	cl := fake.NewClientset(s)
	m, err := newCentralQueueMembership(cl, zap.NewNop(), "lb.customer", time.Minute)
	require.NoError(t, err)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	require.NoError(t, m.informer.SetTransform(func(obj any) (any, error) {
		if slice, ok := obj.(*discoveryv1.EndpointSlice); ok && len(slice.Endpoints) == 6 {
			once.Do(func() { close(blocked) })
			<-release
		}
		return obj, nil
	}))
	m.start(t.Context())
	t.Cleanup(m.shutdown)
	t.Cleanup(func() { close(release) })
	requireMembership(t, m, 1, true)
	s.Endpoints = membershipSlice("", "", "customer", "one", "two", "three", "four", "five", "six").Endpoints
	_, err = cl.DiscoveryV1().EndpointSlices("customer").Update(t.Context(), s, metav1.UpdateOptions{})
	require.NoError(t, err)
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("informer replacement was not reached")
	}
	observed := make(chan bool, 1)
	go func() { n, known := m.snapshot(); observed <- !known || n == 6 }()
	select {
	case safe := <-observed:
		require.True(t, safe, "membership must use the fresh six-peer observation or hold, never the old one-peer cache")
	case <-time.After(time.Second):
		t.Fatal("membership snapshot blocked behind informer cache replacement")
	}
}

func TestCentralQueueMembershipPublishesOnlyCompleteList(t *testing.T) {
	cl := fake.NewClientset()
	secondPage, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	cl.PrependReactor("list", "endpointslices", func(ktesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) == 1 {
			return true, &discoveryv1.EndpointSliceList{ListMeta: metav1.ListMeta{Continue: "second-page", ResourceVersion: "7"}, Items: []discoveryv1.EndpointSlice{*membershipSlice("a", "lb", "customer", "one", "two", "three")}}, nil
		}
		close(secondPage)
		<-release
		return true, &discoveryv1.EndpointSliceList{ListMeta: metav1.ListMeta{ResourceVersion: "7"}, Items: []discoveryv1.EndpointSlice{*membershipSlice("b", "lb", "customer", "four", "five", "six")}}, nil
	})
	m, err := newCentralQueueMembership(cl, zap.NewNop(), "lb.customer", time.Minute)
	require.NoError(t, err)
	m.start(t.Context())
	t.Cleanup(m.shutdown)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	select {
	case <-secondPage:
	case <-time.After(3 * time.Second):
		t.Fatal("second page not requested")
	}
	_, known := m.snapshot()
	require.False(t, known, "partial membership must retain queued batches")
	releaseOnce.Do(func() { close(release) })
	requireMembership(t, m, 6, true)
}

func TestMembershipWatchStopWaitsForObservation(t *testing.T) {
	source := watch.NewRaceFreeFake()
	entered, release := make(chan struct{}), make(chan struct{})
	w := newMembershipWatch(t.Context(), source, func(watch.Event) { close(entered); <-release })
	source.Add(membershipSlice("a", "lb", "customer", "one"))
	<-entered
	stopped := make(chan struct{})
	go func() { w.Stop(); close(stopped) }()
	select {
	case <-stopped:
		close(release)
		t.Fatal("Stop returned while the old watch could still mutate membership")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish")
	}
}

type membershipReplicaGauge struct {
	metric.Int64Gauge
	values []int64
}

func (g *membershipReplicaGauge) Record(_ context.Context, value int64, _ ...metric.RecordOption) {
	g.values = append(g.values, value)
}

func TestCentralQueueMembershipRecordsReplicaChangesOnly(t *testing.T) {
	cl := fake.NewClientset(membershipSlice("a", "lb", "customer", "one", "two"))
	m, err := newCentralQueueMembership(cl, zap.NewNop(), "lb.customer", time.Minute)
	require.NoError(t, err)
	gauge := &membershipReplicaGauge{}
	controller := newCentralQueueConsumerController(30, 256<<10, 1)
	m.bind(t.Context(), controller, &centralQueueTelemetry{activeLBReplicas: gauge}, nil)
	t.Cleanup(m.shutdown)
	requireMembership(t, m, 2, true)
	var active atomic.Int64
	for range 10 {
		controller.tryAcquire(&active, 1<<30, 18, false)
	}
	require.Equal(t, []int64{2}, gauge.values)
	_, err = cl.DiscoveryV1().EndpointSlices("customer").Update(t.Context(), membershipSlice("a", "lb", "customer", "one", "two", "three"), metav1.UpdateOptions{})
	require.NoError(t, err)
	requireMembership(t, m, 3, true)
	controller.tryAcquire(&active, 1<<30, 18, false)
	require.Equal(t, []int64{2, 3}, gauge.values)
	m.shutdown()
	controller.tryAcquire(&active, 1<<30, 18, false)
	controller.tryAcquire(&active, 1<<30, 18, false)
	require.Equal(t, []int64{2, 3, 0}, gauge.values)
}

func TestCentralQueueMembershipDeniedRetainsConfiguredBehavior(t *testing.T) {
	for _, operation := range []string{"list", "watch"} {
		t.Run(operation, func(t *testing.T) {
			cl := fake.NewClientset()
			denied := apierrors.NewForbidden(schema.GroupResource{Group: "discovery.k8s.io", Resource: "endpointslices"}, "lb", errors.New("denied"))
			if operation == "list" {
				cl.PrependReactor("list", "endpointslices", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, denied })
			} else {
				cl.PrependWatchReactor("endpointslices", func(ktesting.Action) (bool, watch.Interface, error) { return true, nil, denied })
			}
			core, logs := observer.New(zap.WarnLevel)
			m, err := newCentralQueueMembership(cl, zap.New(core), "lb.customer", time.Minute)
			require.NoError(t, err)
			_, tb, reader := getTelemetryAssetsWithReader(t)
			controller := newCentralQueueConsumerController(30, 256<<10, 1)
			m.bind(t.Context(), controller, nil, tb)
			t.Cleanup(m.shutdown)
			requireMembership(t, m, 1, true)
			var active atomic.Int64
			decision, acquired, _ := controller.tryAcquire(&active, 1<<30, 18, false)
			require.True(t, acquired)
			require.Equal(t, 18, decision.effectiveConsumers)
			measurement, err := reader.GetMetric("otelcol_loadbalancer_num_resolutions")
			require.NoError(t, err)
			attrs := attribute.NewSet(k8sResolverAttr, attribute.String("scope", "load_balancer_membership"), attribute.Bool("success", false), attribute.String("reason", operation+"_denied"))
			matches := 0
			for _, point := range measurement.Data.(metricdata.Sum[int64]).DataPoints {
				if point.Attributes.Equals(&attrs) {
					require.EqualValues(t, 1, point.Value)
					matches++
				}
			}
			require.Equal(t, 1, matches)
			require.Equal(t, 1, logs.Len())
			require.Equal(t, operation+"_denied", logs.All()[0].ContextMap()["reason"])
		})
	}
}

func TestCentralQueueMembershipWiresBothSignals(t *testing.T) {
	for _, signal := range []string{"logs", "metrics"} {
		t.Run(signal, func(t *testing.T) {
			cl := fake.NewClientset(membershipSlice("a", "lb", "customer", "one", "two", "three", "four", "five", "six"))
			m, err := newCentralQueueMembership(cl, zap.NewNop(), "lb.customer", time.Minute)
			require.NoError(t, err)
			cfg := simpleConfig()
			cfg.CentralQueue.Enabled = true
			var controller *centralQueueConsumerController
			if signal == "logs" {
				e, createErr := newLogsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, createErr)
				e.centralQueueMembership = m
				require.NoError(t, e.Start(t.Context(), componenttest.NewNopHost()))
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					require.NoError(t, e.Shutdown(ctx))
				})
				controller = e.centralQueueConsumers
			} else {
				e, createErr := newMetricsExporter(exportertest.NewNopSettings(metadata.Type), cfg)
				require.NoError(t, createErr)
				e.centralQueueMembership = m
				require.NoError(t, e.Start(t.Context(), componenttest.NewNopHost()))
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					require.NoError(t, e.Shutdown(ctx))
				})
				controller = e.centralQueueConsumers
			}
			requireMembership(t, m, 6, true)
			var active atomic.Int64
			decision, acquired, _ := controller.tryAcquire(&active, 1<<30, 18, false)
			require.True(t, acquired)
			require.Equal(t, 3, decision.backendSafeConsumersPerLB)
			m.shutdown()
			active.Store(0)
			decision, acquired, _ = controller.tryAcquire(&active, 1<<30, 18, false)
			require.False(t, acquired)
			require.Equal(t, centralQueueConsumerLimitReasonMembershipUnknown, decision.limitReason)
		})
	}
}
