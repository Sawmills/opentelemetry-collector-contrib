// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestOuterRetryBoundsPendingLogsMemory(t *testing.T) {
	control := measureOuterRetryMemory(t, false)
	retrying := measureOuterRetryMemory(t, true)
	const bound = 4 * 2 * ((32 << 10) + (1 << 20))
	t.Logf("outer retry off: growth=%d held=%d admitted=%d rejected=%d; on: growth=%d held=%d admitted=%d rejected=%d; allowance=%d",
		control.growth, control.held, control.admitted, control.rejected,
		retrying.growth, retrying.held, retrying.admitted, retrying.rejected, bound)
	require.LessOrEqual(t, control.growth, int64(bound), "non-retrying control exceeds the retained-memory allowance")
	require.LessOrEqual(t, retrying.growth-control.growth, int64(bound), "outer retries retain pending request heap outside the queue budgets")
}

type outerRetryMemoryResult struct {
	growth   int64
	held     int64
	admitted int
	rejected int
}

type outerRetryCallResult struct {
	request int
	err     error
}

type outerRetryCalls struct {
	started   atomic.Int64
	completed atomic.Int64
	results   chan outerRetryCallResult
}

func measureOuterRetryMemory(t *testing.T, retryEnabled bool) outerRetryMemoryResult {
	t.Helper()
	const requests = 64
	const records = 256
	oldProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(oldProcs)
	backend := newMemoryTestBackend(t)
	core, observed := observer.New(zap.InfoLevel)
	settings := exportertest.NewNopSettings(metadata.Type)
	settings.Logger = zap.New(core)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.Resolver = ResolverSettings{Static: configoptional.Some(StaticResolver{Hostnames: []string{backend.address}})}
	cfg.Protocol.OTLP.ClientConfig.TLS.Insecure = true
	cfg.Protocol.OTLP.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.Protocol.OTLP.RetryConfig.Enabled = false
	cfg.BackOffConfig = configretry.NewDefaultBackOffConfig()
	cfg.Enabled = retryEnabled
	cfg.InitialInterval = 100 * time.Millisecond
	cfg.MaxInterval = 100 * time.Millisecond
	cfg.Multiplier = 1
	cfg.RandomizationFactor = 0
	cfg.MaxElapsedTime = 15 * time.Second
	cfg.CentralQueue.Enabled = true
	cfg.CentralQueue.MaxCompressedBytes = 32 << 10
	cfg.CentralQueue.MaxInflightUncompressedBytes = 1 << 20
	cfg.CentralQueue.MaxUncompressedBatchBytes = 256 << 10
	cfg.CentralQueue.TargetCompressedBytes = 1024
	cfg.CentralQueue.MaxBatchDelay = time.Millisecond
	cfg.CentralQueue.NumConsumers = 2
	cfg.CentralQueue.LaneCount = 1
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		backend.refuse.Store(false)
		cancel()
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, exp.Shutdown(shutdownCtx))
	})

	// Warm the codecs with the same concurrency and payload sizes. Do not keep
	// caller pdata alive across the idle baseline or the pressure measurement.
	warm := startOuterRetryCalls(ctx, exp, requests, records)
	require.Eventually(t, func() bool { return warm.completed.Load() == requests }, 20*time.Second, 10*time.Millisecond)
	warmAccepted := 0
	for range requests {
		result := <-warm.results
		if result.err == nil {
			warmAccepted += records
		} else {
			require.ErrorIs(t, result.err, errCentralQueueFull)
		}
	}
	require.Eventually(t, func() bool { return backend.received.Load() == int64(warmAccepted) }, 15*time.Second, 10*time.Millisecond)
	backend.reset()
	observed.TakeAll()
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	backend.refuse.Store(true)
	calls := startOuterRetryCalls(ctx, exp, requests, records)
	require.Eventually(t, func() bool { return calls.started.Load() == requests }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, backend.retryObserved.Load, 5*time.Second, 10*time.Millisecond)
	if retryEnabled {
		// Inner OTLP retries are disabled: this exporter log identifies the
		// public factory's outer retry wrapper, not the central delivery loop.
		require.Eventually(t, func() bool { return observed.FilterMessageSnippet("Will retry").Len() > 0 }, 5*time.Second, 10*time.Millisecond)
		require.Positive(t, calls.started.Load()-calls.completed.Load(), "the retrying scenario must hold callers during queue pressure")
	} else {
		require.Eventually(t, func() bool { return calls.completed.Load() == requests }, 10*time.Second, 10*time.Millisecond)
	}
	runtime.GC()
	var pressured runtime.MemStats
	runtime.ReadMemStats(&pressured)
	result := outerRetryMemoryResult{
		growth: int64(pressured.HeapAlloc) - int64(baseline.HeapAlloc),
		held:   calls.started.Load() - calls.completed.Load(),
	}

	// Always release backpressure and verify delivery before asserting memory.
	backend.refuse.Store(false)
	require.Eventually(t, func() bool { return calls.completed.Load() == requests }, 20*time.Second, 10*time.Millisecond)
	want := make(map[[2]int]int)
	for range requests {
		call := <-calls.results
		if call.err != nil {
			result.rejected++
			require.ErrorIs(t, call.err, errCentralQueueFull)
			require.False(t, consumererror.IsPermanent(call.err), "queue rejection must remain retryable")
			continue
		}
		result.admitted++
		for ordinal := range records {
			want[[2]int{call.request, ordinal}] = 1
		}
	}
	if !retryEnabled {
		require.Positive(t, result.rejected, "the control must actually reach queue admission pressure")
	}
	require.Positive(t, result.admitted)
	require.Eventually(t, func() bool { return backend.received.Load() == int64(result.admitted*records) }, 15*time.Second, 10*time.Millisecond)
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	err = exp.Shutdown(shutdownCtx)
	stop()
	require.NoError(t, err)
	backend.mu.Lock()
	got := make(map[[2]int]int, len(backend.byRecord))
	for identity, count := range backend.byRecord {
		got[identity] = count
	}
	backend.mu.Unlock()
	require.Equal(t, want, got, "every admitted record must arrive exactly once; rejected requests must not arrive")
	return result
}

func startOuterRetryCalls(ctx context.Context, exp exporter.Logs, requests, records int) *outerRetryCalls {
	calls := &outerRetryCalls{results: make(chan outerRetryCallResult, requests)}
	for request := range requests {
		go func() {
			logs := plog.NewLogs()
			scope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
			for ordinal := range records {
				record := scope.LogRecords().AppendEmpty()
				record.Body().SetStr(strings.Repeat("synthetic-log ", 315))
				record.Attributes().PutInt("request", int64(request))
				record.Attributes().PutInt("ordinal", int64(ordinal))
			}
			logs.MarkReadOnly()
			calls.started.Add(1)
			err := exp.ConsumeLogs(ctx, logs)
			calls.results <- outerRetryCallResult{request: request, err: err}
			calls.completed.Add(1)
		}()
	}
	return calls
}
