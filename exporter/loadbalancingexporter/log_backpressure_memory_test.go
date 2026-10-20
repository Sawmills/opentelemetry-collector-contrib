// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

// Exercise admission, compression, real OTLP sends and retries together. Both
// exporters receive the same synthetic requests; only their backend differs.
func TestLogsBackpressureBoundsMemoryAndPreservesDelivery(t *testing.T) {
	if runMemoryTestInSubprocess(t) {
		return
	}
	result := runLogsBackpressureMemoryScenario(t)

	require.True(t, result.retryObserved, "the same refused record must be retried")
	require.Equal(t, result.expected, result.delivered, "every admitted record must arrive exactly once after recovery")
	require.LessOrEqual(t, result.retainedGrowth, result.memoryBound, "refusing backend retained unbudgeted heap")
}

type logsBackpressureMemoryResult struct {
	retryObserved  bool
	expected       []map[[2]int]int
	delivered      []map[[2]int]int
	retainedGrowth int64
	memoryBound    int64
}

func runLogsBackpressureMemoryScenario(t *testing.T) logsBackpressureMemoryResult {
	t.Helper()
	oldProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(oldProcs) })
	const requests = 128
	const records = 128
	const workers = 8
	const compressedBudget = 32 << 10
	const inflightBudget = 1 << 20
	// Compare against the idle heap after healthy load warms all codec
	// workers. Allow four times the two configured wire-byte budgets for live
	// pdata/protobuf objects in exporter and peer; retain no per-backlog copy.
	const memoryBound = 4 * 2 * (compressedBudget + inflightBudget)

	input := make([]plog.Logs, requests)
	for request := range input {
		logs := plog.NewLogs()
		scope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		for ordinal := range records {
			record := scope.LogRecords().AppendEmpty()
			record.Body().SetStr(strings.Repeat("synthetic-log ", 315))
			record.Attributes().PutInt("request", int64(request))
			record.Attributes().PutInt("ordinal", int64(ordinal))
		}
		logs.MarkReadOnly()
		input[request] = logs
	}

	healthy := newMemoryTestBackend(t)
	pressured := newMemoryTestBackend(t)
	backends := []*memoryTestBackend{healthy, pressured}
	exporters := make([]exporter.Logs, len(backends))
	for index, backend := range backends {
		factory := NewFactory()
		cfg := factory.CreateDefaultConfig().(*Config)
		cfg.Resolver = ResolverSettings{Static: configoptional.Some(StaticResolver{Hostnames: []string{backend.address}})}
		cfg.Protocol.OTLP.ClientConfig.TLS.Insecure = true
		cfg.Protocol.OTLP.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
		cfg.Protocol.OTLP.RetryConfig.Enabled = false
		// The fixture owns the retry barrier; a send deadline must not end it.
		cfg.Protocol.OTLP.TimeoutConfig.Timeout = 0
		cfg.CentralQueue.Enabled = true
		cfg.CentralQueue.MaxCompressedBytes = compressedBudget
		cfg.CentralQueue.MaxInflightUncompressedBytes = inflightBudget
		cfg.CentralQueue.MaxUncompressedBatchBytes = 256 << 10
		cfg.CentralQueue.TargetCompressedBytes = 1024
		cfg.CentralQueue.MaxBatchDelay = time.Millisecond
		cfg.CentralQueue.NumConsumers = 2
		cfg.CentralQueue.LaneCount = 1
		exp, err := factory.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
		t.Cleanup(func() {
			backend.refuse.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, exp.Shutdown(ctx))
		})
		exporters[index] = exp
		// Initialize encode, decode and transport with the measured payload size.
		require.NoError(t, exp.ConsumeLogs(t.Context(), input[0]))
		backend.waitForIdleDelivery(t, records, 5*time.Second)
	}
	control := memoryTestAdmissions(t, exporters, input, workers)
	for index, backend := range backends {
		backend.waitForIdleDelivery(t, records+memoryTestAcceptedRecords(control[index], records), 15*time.Second)
		backend.reset()
	}

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	retryBarrier := newMemoryTestRPCBarrier(t)
	pressured.retryBarrier.Store(retryBarrier)
	pressured.refuse.Store(true)
	accepted := memoryTestAdmissions(t, exporters, input, workers)
	require.Less(t, memoryTestAcceptedRecords(accepted[1], records), int64(requests*records), "the pressured queue must actually reject intake")
	require.Eventually(t, pressured.retryObserved.Load, 5*time.Second, 10*time.Millisecond)
	// Keep the healthy peer idle and the pressured backend's single send slot
	// inside its retried RPC, with the decoded request live during the snapshot.
	healthyExpected := memoryTestAcceptedRecords(accepted[0], records)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.True(c, memoryTestSnapshotReady(healthy, healthyExpected, retryBarrier),
			"healthy received=%d expected=%d active RPCs=%d; held retries=%d expected=1",
			healthy.received.Load(), healthyExpected, healthy.activeRPCs.Load(), retryBarrier.held.Load())
	}, 15*time.Second, 10*time.Millisecond)
	heldRetry := retryBarrier.current.Load()
	require.True(t, retryBarrier.holds(heldRetry),
		"before snapshot: held=%d expected=1 same entry=%t expected=true live=%t expected=true",
		retryBarrier.held.Load(), retryBarrier.current.Load() == heldRetry, heldRetry != nil && heldRetry.ctx.Err() == nil)
	_, hasDeadline := heldRetry.ctx.Deadline()
	require.False(t, hasDeadline, "the held retry must not expire during the heap snapshot")

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	require.True(t, retryBarrier.holds(heldRetry),
		"after snapshot: held=%d expected=1 same entry=%t expected=true live=%t expected=true",
		retryBarrier.held.Load(), retryBarrier.current.Load() == heldRetry, heldRetry != nil && heldRetry.ctx.Err() == nil)
	runtime.KeepAlive(input)
	runtime.KeepAlive(exporters)
	retainedGrowth := int64(after.HeapAlloc) - int64(baseline.HeapAlloc)
	result := logsBackpressureMemoryResult{retryObserved: pressured.retryObserved.Load(), retainedGrowth: retainedGrowth, memoryBound: memoryBound}
	// Recover before assertions so failed memory checks cannot strand retries.
	pressured.refuse.Store(false)
	retryBarrier.release()
	for index, backend := range backends {
		expected := int64(0)
		wantRecords := make(map[[2]int]int)
		for request, admitted := range accepted[index] {
			if admitted {
				expected += records
				for ordinal := range records {
					wantRecords[[2]int{request, ordinal}] = 1
				}
			}
		}
		require.Positive(t, expected)
		require.Eventually(t, func() bool { return backend.received.Load() >= expected }, 15*time.Second, 10*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := exporters[index].Shutdown(ctx)
		cancel()
		require.NoError(t, err, "drain and stop before the final delivery snapshot")
		backend.mu.Lock()
		counts := make(map[[2]int]int, len(backend.byRecord))
		for record, count := range backend.byRecord {
			counts[record] = count
		}
		backend.mu.Unlock()
		result.expected = append(result.expected, wantRecords)
		result.delivered = append(result.delivered, counts)
	}
	t.Logf("equal attempted intake: %d requests x %d logs per exporter; live heap growth from idle warmed baseline=%d bound=%d", requests, records, retainedGrowth, memoryBound)
	return result
}

func memoryTestAdmissions(t *testing.T, exporters []exporter.Logs, input []plog.Logs, workers int) [][]bool {
	t.Helper()
	accepted := make([][]bool, len(exporters))
	errorsSeen := make(chan error, len(input)*len(exporters))
	var admissions sync.WaitGroup
	for index, exp := range exporters {
		accepted[index] = make([]bool, len(input))
		for worker := range workers {
			admissions.Go(func() {
				for request := worker; request < len(input); request += workers {
					err := exp.ConsumeLogs(t.Context(), input[request])
					if err == nil {
						accepted[index][request] = true
					} else {
						errorsSeen <- err
					}
				}
			})
		}
	}
	admissions.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		require.ErrorIs(t, err, errCentralQueueFull)
		require.False(t, consumererror.IsPermanent(err), "queue pressure must remain retryable")
	}
	return accepted
}

func memoryTestAcceptedRecords(accepted []bool, records int) int64 {
	count := int64(0)
	for _, admitted := range accepted {
		if admitted {
			count += int64(records)
		}
	}
	return count
}

type memoryTestBackend struct {
	plogotlp.UnimplementedGRPCServer
	address       string
	refuse        atomic.Bool
	retryObserved atomic.Bool
	received      atomic.Int64
	activeRPCs    atomic.Int64
	retryBarrier  atomic.Pointer[memoryTestRPCBarrier]
	returnBarrier atomic.Pointer[memoryTestRPCBarrier]
	mu            sync.Mutex
	byRecord      map[[2]int]int
	attempts      map[[2]int]int
}

func newMemoryTestBackend(t *testing.T) *memoryTestBackend {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	backend := &memoryTestBackend{address: listener.Addr().String(), byRecord: make(map[[2]int]int), attempts: make(map[[2]int]int)}
	server := grpc.NewServer(grpc.StatsHandler(backend))
	plogotlp.RegisterGRPCServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return backend
}

func (b *memoryTestBackend) Export(ctx context.Context, request plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	refuse := b.refuse.Load()
	count := int64(0)
	retried := false
	b.mu.Lock()
	for _, resource := range request.Logs().ResourceLogs().All() {
		for _, scope := range resource.ScopeLogs().All() {
			for _, record := range scope.LogRecords().All() {
				identity := [2]int{-1, -1}
				requestID, _ := record.Attributes().Get("request")
				ordinal, _ := record.Attributes().Get("ordinal")
				identity[0] = int(requestID.Int())
				identity[1] = int(ordinal.Int())
				if refuse {
					b.attempts[identity]++
					if b.attempts[identity] > 1 {
						b.retryObserved.Store(true)
						retried = true
					}
				} else {
					b.byRecord[identity]++
				}
				count++
			}
		}
	}
	b.mu.Unlock()
	if refuse {
		if retried {
			if barrier := b.retryBarrier.Load(); barrier != nil {
				barrier.wait(ctx)
				runtime.KeepAlive(request)
			}
		}
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "synthetic backend pressure")
	}
	b.received.Add(count)
	if barrier := b.returnBarrier.Load(); barrier != nil {
		barrier.wait(ctx)
	}
	return plogotlp.NewExportResponse(), nil
}

func (b *memoryTestBackend) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (b *memoryTestBackend) HandleRPC(_ context.Context, event stats.RPCStats) {
	switch event.(type) {
	case *stats.Begin:
		b.activeRPCs.Add(1)
	case *stats.End:
		b.activeRPCs.Add(-1)
	}
}

func (b *memoryTestBackend) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*memoryTestBackend) HandleConn(context.Context, stats.ConnStats) {}

func (b *memoryTestBackend) waitForIdleDelivery(t *testing.T, expected int64, timeout time.Duration) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.GreaterOrEqual(c, b.received.Load(), expected)
		require.Zero(c, b.activeRPCs.Load(), "wait for RPC completion, not only the received counter")
	}, timeout, 10*time.Millisecond)
}

func memoryTestSnapshotReady(healthy *memoryTestBackend, expected int64, retries *memoryTestRPCBarrier) bool {
	return healthy.received.Load() >= expected && healthy.activeRPCs.Load() == 0 && retries.held.Load() == 1
}

type memoryTestRPCBarrier struct {
	held    atomic.Int64
	current atomic.Pointer[memoryTestRPCEntry]
	done    chan struct{}
	once    sync.Once
}

type memoryTestRPCEntry struct {
	ctx context.Context
}

func newMemoryTestRPCBarrier(t *testing.T) *memoryTestRPCBarrier {
	t.Helper()
	b := &memoryTestRPCBarrier{done: make(chan struct{})}
	t.Cleanup(b.release)
	return b
}

func (b *memoryTestRPCBarrier) wait(ctx context.Context) {
	entry := &memoryTestRPCEntry{ctx: ctx}
	b.current.Store(entry)
	b.held.Add(1)
	defer func() {
		b.held.Add(-1)
		b.current.CompareAndSwap(entry, nil)
	}()
	select {
	case <-b.done:
	case <-ctx.Done():
	}
}

func (b *memoryTestRPCBarrier) holds(entry *memoryTestRPCEntry) bool {
	return entry != nil && b.held.Load() == 1 && b.current.Load() == entry && entry.ctx.Err() == nil
}

func (b *memoryTestRPCBarrier) release() {
	b.once.Do(func() { close(b.done) })
}

func (b *memoryTestBackend) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.byRecord = make(map[[2]int]int)
	b.received.Store(0)
}
