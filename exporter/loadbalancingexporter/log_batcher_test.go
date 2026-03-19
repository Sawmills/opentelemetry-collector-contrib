// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestLogBatcherMergesSameBackendOnShutdown(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	sink := new(consumertest.LogsSink)
	p, _ := newTestLogsExporter(t, ts, tb, simpleConfig(), func(_ context.Context, _ string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})

	p.batcher, _ = newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, p.consumeBatch)
	p.loadBalancer.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
		return p.batcher.Remove(ctx, endpoint, exp)
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), logsWithTraceIDs([16]byte{1}, [16]byte{2})))
	require.NoError(t, p.Shutdown(t.Context()))

	require.Len(t, sink.AllLogs(), 1)
	assert.Equal(t, 2, sink.AllLogs()[0].LogRecordCount())
}

func TestLogBatcherFlushesOnTimeout(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	sink := new(consumertest.LogsSink)
	p, _ := newTestLogsExporter(t, ts, tb, simpleConfig(), func(_ context.Context, _ string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})

	p.batcher, _ = newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: 25 * time.Millisecond,
	}, p.consumeBatch)
	p.loadBalancer.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
		return p.batcher.Remove(ctx, endpoint, exp)
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(t.Context())) }()
	require.NoError(t, p.ConsumeLogs(t.Context(), simpleLogs()))

	require.Eventually(t, func() bool {
		return len(sink.AllLogs()) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestLogBatcherFlushesOnMaxBytes(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	sink := new(consumertest.LogsSink)
	p, _ := newTestLogsExporter(t, ts, tb, simpleConfig(), func(_ context.Context, _ string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})

	first := sizedLogWithID(pcommon.TraceID([16]byte{1}), 512)
	maxBytes := (&plog.ProtoMarshaler{}).LogsSize(first) + 1

	p.batcher, _ = newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      maxBytes,
		flushInterval: time.Hour,
	}, p.consumeBatch)
	p.loadBalancer.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
		return p.batcher.Remove(ctx, endpoint, exp)
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(t.Context())) }()
	require.NoError(t, p.ConsumeLogs(t.Context(), mergeLogs(first, sizedLogWithID(pcommon.TraceID([16]byte{2}), 512))))

	require.Eventually(t, func() bool {
		return len(sink.AllLogs()) == 1 && sink.AllLogs()[0].LogRecordCount() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestLogBatcherMaxBytesUsesMergedPayloadSize(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	sink := new(consumertest.LogsSink)
	p, _ := newTestLogsExporter(t, ts, tb, simpleConfig(), func(_ context.Context, _ string) (component.Component, error) {
		return newMockLogsExporter(sink.ConsumeLogs), nil
	})

	first := sharedResourceScopeLog("first")
	second := sharedResourceScopeLog("second")
	sizer := &plog.ProtoMarshaler{}
	separateBytes := sizer.LogsSize(first) + sizer.LogsSize(second)
	merged := mergeLogs(sharedResourceScopeLog("first"), sharedResourceScopeLog("second"))
	mergedBytes := sizer.LogsSize(merged)
	require.Greater(t, separateBytes, mergedBytes)

	p.batcher, _ = newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      mergedBytes + 1,
		flushInterval: time.Hour,
	}, p.consumeBatch)
	p.loadBalancer.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
		return p.batcher.Remove(ctx, endpoint, exp)
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, p.ConsumeLogs(t.Context(), mergeLogs(sharedResourceScopeLog("first"), sharedResourceScopeLog("second"))))
	assert.Empty(t, sink.AllLogs())
	require.NoError(t, p.Shutdown(t.Context()))
	require.Len(t, sink.AllLogs(), 1)
	assert.Equal(t, 2, sink.AllLogs()[0].LogRecordCount())
}

func TestLogBatcherDoesNotBlockOtherBackends(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	blockFirst := make(chan struct{})
	firstStarted := make(chan struct{}, 1)
	var secondCalls atomic.Int64
	batcher, err := newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    1,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, func(ctx context.Context, exp *wrappedExporter, ld plog.Logs, reason string) error {
		_ = reason
		exp.consumeWG.Add(1)
		defer exp.consumeWG.Done()
		return exp.ConsumeLogs(ctx, ld)
	})
	require.NoError(t, err)
	defer func() {
		close(blockFirst)
		require.NoError(t, batcher.Shutdown(t.Context()))
	}()

	firstExporter := newWrappedExporter(newMockLogsExporter(func(_ context.Context, _ plog.Logs) error {
		firstStarted <- struct{}{}
		<-blockFirst
		return nil
	}), "endpoint-1:4317")
	secondExporter := newWrappedExporter(newMockLogsExporter(func(_ context.Context, _ plog.Logs) error {
		secondCalls.Add(1)
		return nil
	}), "endpoint-2:4317")

	require.NoError(t, batcher.Enqueue(t.Context(), "endpoint-1:4317", firstExporter, simpleLogs()))
	<-firstStarted
	require.NoError(t, batcher.Enqueue(t.Context(), "endpoint-2:4317", secondExporter, simpleLogs()))

	require.Eventually(t, func() bool {
		return secondCalls.Load() == 1
	}, time.Second, 10*time.Millisecond)
}

func TestLogBatcherShutdownWaitsForInflightEnqueue(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	var calls atomic.Int64
	batcher, err := newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, func(ctx context.Context, exp *wrappedExporter, ld plog.Logs, reason string) error {
		_ = ctx
		_ = exp
		_ = reason
		calls.Add(int64(ld.LogRecordCount()))
		return nil
	})
	require.NoError(t, err)

	backend, err := batcher.acquireBackend("endpoint-1:4317", newWrappedExporter(newNopMockLogsExporter(), "endpoint-1:4317"))
	require.NoError(t, err)

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- batcher.Shutdown(t.Context())
	}()

	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before inflight enqueue finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	backend.requests <- logBatcherRequest{kind: logBatcherRequestEnqueue, logs: simpleLogs()}
	backend.inflight.Done()

	require.NoError(t, <-shutdownDone)
	assert.Equal(t, int64(1), calls.Load())
}

func TestLogBatcherShutdownRespectsContextWhileWaitingForInflight(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	batcher, err := newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, func(context.Context, *wrappedExporter, plog.Logs, string) error {
		return nil
	})
	require.NoError(t, err)

	backend, err := batcher.acquireBackend("endpoint-1:4317", newWrappedExporter(newNopMockLogsExporter(), "endpoint-1:4317"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, batcher.Shutdown(ctx), context.Canceled)
	backend.inflight.Done()
	require.Eventually(t, func() bool {
		select {
		case <-backend.done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestLogBatcherRemoveRespectsContextWhileWaitingForInflight(t *testing.T) {
	ts, _ := getTelemetryAssets(t)
	batcher, err := newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, func(context.Context, *wrappedExporter, plog.Logs, string) error {
		return nil
	})
	require.NoError(t, err)
	defer batcher.telemetry.shutdown()

	backend, err := batcher.acquireBackend("endpoint-1:4317", newWrappedExporter(newNopMockLogsExporter(), "endpoint-1:4317"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, batcher.Remove(ctx, "endpoint-1:4317", backend.exporter()), context.Canceled)
	backend.inflight.Done()
	require.Eventually(t, func() bool {
		select {
		case <-backend.done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestLogBatcherFlushesRemovedBackendToOldExporter(t *testing.T) {
	ts, tb := getTelemetryAssets(t)
	var endpoint1Calls atomic.Int64
	var endpoint2Calls atomic.Int64
	endpoints := []string{"endpoint-1"}

	p, lb := newTestLogsExporter(t, ts, tb, &Config{
		Resolver: ResolverSettings{
			Static: configoptional.Some(StaticResolver{Hostnames: []string{"endpoint-1"}}),
		},
	}, func(_ context.Context, endpoint string) (component.Component, error) {
		switch endpoint {
		case "endpoint-1:4317":
			return newMockLogsExporter(func(_ context.Context, _ plog.Logs) error {
				endpoint1Calls.Add(1)
				return nil
			}), nil
		case "endpoint-2:4317":
			return newMockLogsExporter(func(_ context.Context, _ plog.Logs) error {
				endpoint2Calls.Add(1)
				return nil
			}), nil
		default:
			return newNopMockLogsExporter(), nil
		}
	})

	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(_ context.Context) ([]string, error) {
			return endpoints, nil
		},
	}
	p.loadBalancer = lb
	p.batcher, _ = newLogBatcher(ts.Logger, ts.TelemetrySettings, logBatcherSettings{
		maxRecords:    100,
		maxBytes:      1 << 20,
		flushInterval: time.Hour,
	}, p.consumeBatch)
	p.loadBalancer.onExporterRemove = func(ctx context.Context, endpoint string, exp *wrappedExporter) error {
		return p.batcher.Remove(ctx, endpoint, exp)
	}

	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

	require.NoError(t, p.ConsumeLogs(t.Context(), simpleLogs()))
	endpoints = []string{"endpoint-2"}
	_, err := lb.res.resolve(t.Context())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return endpoint1Calls.Load() == 1
	}, time.Second, 10*time.Millisecond)
	assert.Zero(t, endpoint2Calls.Load())
}

func newTestLogsExporter(
	t *testing.T,
	ts exporter.Settings,
	tb *metadata.TelemetryBuilder,
	cfg *Config,
	componentFactory func(context.Context, string) (component.Component, error),
) (*logExporterImp, *loadBalancer) {
	t.Helper()

	lb, err := newLoadBalancer(ts.Logger, cfg, componentFactory, tb)
	require.NoError(t, err)
	lb.addMissingExporters(t.Context(), cfg.Resolver.Static.Get().Hostnames)
	lb.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(_ context.Context) ([]string, error) {
			return cfg.Resolver.Static.Get().Hostnames, nil
		},
	}

	p, err := newLogsExporter(ts, cfg)
	require.NoError(t, err)
	p.loadBalancer = lb
	return p, lb
}

func logsWithTraceIDs(ids ...pcommon.TraceID) plog.Logs {
	logs := plog.NewLogs()
	for _, id := range ids {
		single := simpleLogWithID(id)
		mergeLogs(logs, single)
	}
	return logs
}

func sizedLogWithID(id pcommon.TraceID, size int) plog.Logs {
	logs := simpleLogWithID(id)
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(string(make([]byte, size)))
	return logs
}
