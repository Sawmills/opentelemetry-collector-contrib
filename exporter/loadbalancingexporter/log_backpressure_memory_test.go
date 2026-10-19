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
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

// Exercise admission, compression, real OTLP sends and retries together. Both
// exporters receive the same synthetic requests; only their backend differs.
func TestLogsBackpressureBoundsMemoryAndPreservesDelivery(t *testing.T) {
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
		require.Eventually(t, func() bool { return backend.received.Load() == records }, 5*time.Second, 10*time.Millisecond)
	}
	control := memoryTestAdmissions(t, exporters, input, workers)
	for index, backend := range backends {
		require.Eventually(t, func() bool {
			return backend.received.Load() >= records+memoryTestAcceptedRecords(control[index], records)
		}, 15*time.Second, 10*time.Millisecond)
		backend.reset()
	}

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	pressured.refuse.Store(true)
	accepted := memoryTestAdmissions(t, exporters, input, workers)
	require.Less(t, memoryTestAcceptedRecords(accepted[1], records), int64(requests*records), "the pressured queue must actually reject intake")
	require.Eventually(t, pressured.retryObserved.Load, 5*time.Second, 10*time.Millisecond)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(input)
	runtime.KeepAlive(exporters)
	retainedGrowth := int64(after.HeapAlloc) - int64(baseline.HeapAlloc)
	result := logsBackpressureMemoryResult{retryObserved: pressured.retryObserved.Load(), retainedGrowth: retainedGrowth, memoryBound: memoryBound}
	// Recover before assertions so failed memory checks cannot strand retries.
	pressured.refuse.Store(false)
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
	mu            sync.Mutex
	byRecord      map[[2]int]int
	attempts      map[[2]int]int
}

func newMemoryTestBackend(t *testing.T) *memoryTestBackend {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	backend := &memoryTestBackend{address: listener.Addr().String(), byRecord: make(map[[2]int]int), attempts: make(map[[2]int]int)}
	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return backend
}

func (b *memoryTestBackend) Export(_ context.Context, request plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	refuse := b.refuse.Load()
	count := int64(0)
	b.mu.Lock()
	defer b.mu.Unlock()
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
					}
				} else {
					b.byRecord[identity]++
				}
				count++
			}
		}
	}
	if refuse {
		return plogotlp.NewExportResponse(), status.Error(codes.Unavailable, "synthetic backend pressure")
	}
	b.received.Add(count)
	return plogotlp.NewExportResponse(), nil
}

func (b *memoryTestBackend) reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.byRecord = make(map[[2]int]int)
	b.received.Store(0)
}
