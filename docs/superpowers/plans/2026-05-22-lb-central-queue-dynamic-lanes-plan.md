# LB Central Queue Dynamic Lanes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce LB CPU/GC bottleneck by replacing global queue scans with bucketed lane queues and choosing lane count from backend fanout plus lane fill-rate math.

**Architecture:** `num_consumers` remains send concurrency. Lane count becomes an internal batching/fairness decision with guardrails, computed from healthy backend count, recent compressed ingest rate, target compressed bytes, and target fill time. Existing queued items keep their current lane key and drain naturally when effective lane count changes.

**Tech Stack:** Go, OpenTelemetry Collector loadbalancing exporter, `go test`, pprof benchmarks, existing central queue metrics.

---

## Evidence And Root Cause

A production pprof capture from a high-throughput deployment showed the LB hot path inside central queue leasing/window selection, not backend network wait.

- Hot cumulative CPU was in `runCentralQueue`, queue lease/try-lease, and the current queue window candidate builder.
- Heap allocation profile showed queue candidate selection as the dominant allocator.
- GC was a visible share of CPU because each lease allocates temporary maps/slices/keys while scanning the global queue.
- Current code stores queued work in one `[]centralQueueItem` and, for every lease, scans queue items and routing keys to find a ready window.
- Current `centralQueueLaneCount(numConsumers)` returns at least `64` lanes, which is too many for low backend count or low traffic because lane fill time grows with lane count.

The target cost model is:

```text
lease cost = O(log ready_lanes + selected_window_items)
```

The current observed cost model behaves closer to:

```text
lease cost = O(total_queue_items * effective_routing_keys_seen)
```

## Files

- Modify: `exporter/loadbalancingexporter/central_queue.go`
- Modify: `exporter/loadbalancingexporter/central_queue_item.go`
- Create: `exporter/loadbalancingexporter/central_queue_bucket.go`
- Create: `exporter/loadbalancingexporter/central_queue_lane_policy.go`
- Modify: `exporter/loadbalancingexporter/log_exporter.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter.go`
- Modify: `exporter/loadbalancingexporter/loadbalancer.go`
- Modify: `exporter/loadbalancingexporter/config.go`
- Modify: `exporter/loadbalancingexporter/config.schema.yaml`
- Modify: `exporter/loadbalancingexporter/metadata.yaml`
- Modify: `exporter/loadbalancingexporter/central_queue_telemetry.go`
- Modify: `exporter/loadbalancingexporter/central_queue_test.go`
- Create: `exporter/loadbalancingexporter/central_queue_bucket_test.go`
- Create: `exporter/loadbalancingexporter/central_queue_lane_policy_test.go`
- Create: `exporter/loadbalancingexporter/central_queue_benchmark_test.go`
- Modify: `exporter/loadbalancingexporter/log_exporter_test.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter_test.go`
- Modify: `exporter/loadbalancingexporter/documentation.md`

## Design Rules

1. `num_consumers` controls parallel sends only.
2. Effective lanes control queue partitioning and batching only.
3. Many workers must not make queue leasing CPU scale linearly with queue depth.
4. Few workers must not create excessive lanes that wait too long to fill.
5. Effective lane changes apply only to newly enqueued items.
6. Already queued and in-flight items keep their original lane/routing key.
7. Backpressure remains based on queue bytes, oldest age, in-flight bytes, backend latency, refusals, and fallback.

## Effective Lane Policy

Add an internal policy:

```text
backend_lanes = healthy_backends * backend_multiplier
fill_lanes = compressed_ingest_bytes_per_second * target_fill_duration / target_compressed_bytes
candidate = min(backend_lanes, fill_lanes, max_lanes)
effective_lanes = clamp(candidate, min_lanes, max_lanes)
```

Use stable behavior:

- Recompute on backend changes and no more than once per 30 seconds from enqueue traffic.
- Use a 2x hysteresis threshold before changing lane count.
- If recent ingest rate is unknown, use `min(max(healthy_backends, min_lanes), max_lanes)`.
- If no healthy backend count is available, fall back to resolved backend count.
- If all backends are unhealthy and endpoint health is in fail-open mode, use resolved backend count.

Initial defaults:

```text
min_lanes = 1
max_lanes = 256
backend_multiplier = 2
target_fill_duration = min(max_batch_delay / 2, 500ms)
```

For high-throughput traffic this can still settle high. For 4 workers or low traffic it should stay low enough to avoid tiny batches and delay-heavy flushing.

Implementation update from review evidence: the shipped default `max_lanes` is `256`, not `64`. The dynamic policy still contracts low-backend/low-rate clusters through backend-count and fill-rate bounds, while high-traffic many-backend clusters can reach the 256-lane spread without requiring a per-cluster override.

## Task 1: Add Red Benchmarks And Policy Tests

**Files:**
- Create: `exporter/loadbalancingexporter/central_queue_benchmark_test.go`
- Create: `exporter/loadbalancingexporter/central_queue_lane_policy_test.go`
- Modify: `exporter/loadbalancingexporter/central_queue_test.go`

- [ ] **Step 1: Add a benchmark showing global scan cost**

Create `exporter/loadbalancingexporter/central_queue_benchmark_test.go`:

```go
package loadbalancingexporter

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkCentralQueueCollectManyLanesManyItems(b *testing.B) {
	const laneCount = 64
	const itemsPerLane = 1024
	now := time.Unix(1_700_000_000, 0)

	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1 << 30,
		maxInflightUncompressedBytes: 1 << 30,
		maxUncompressedBatchBytes:    1 << 20,
		targetCompressedBytes:        256 << 10,
		maxBatchDelay:                time.Second,
		maxReadyWindows:              centralQueueReadyWindowLimit(defaultCentralQueueNumConsumers),
	})

	for lane := 0; lane < laneCount; lane++ {
		for i := 0; i < itemsPerLane; i++ {
			err := q.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        []byte(fmt.Sprintf("lane-%02d", lane)),
				compressedBytes:   512,
				uncompressedBytes: 4096,
				count:             1,
			}, now)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.mu.Lock()
		targetCandidates, fallbackCandidates, hasReady := q.collectWindowCandidatesLocked(now.Add(2 * time.Second))
		q.mu.Unlock()
		if !hasReady || len(targetCandidates)+len(fallbackCandidates) == 0 {
			b.Fatal("expected ready candidates")
		}
	}
}
```

- [ ] **Step 2: Add policy tests that fail before the new policy exists**

Create `exporter/loadbalancingexporter/central_queue_lane_policy_test.go`:

```go
package loadbalancingexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueLanePolicyCapsFewBackendsByFillRate(t *testing.T) {
	policy := centralQueueLanePolicy{
		minLanes:           1,
		maxLanes:           64,
		backendMultiplier:  2,
		targetFillDuration: 500 * time.Millisecond,
		targetBytes:        256 << 10,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:               2,
		compressedIngestBytesPerSec:   512 << 10,
		previousEffectiveLaneCount:    64,
		previousEffectiveLaneCountSet: true,
	})

	require.Equal(t, 1, lanes)
}

func TestCentralQueueLanePolicyAllowsManyBackendsWhenFillRateSupportsIt(t *testing.T) {
	policy := centralQueueLanePolicy{
		minLanes:           1,
		maxLanes:           64,
		backendMultiplier:  2,
		targetFillDuration: 500 * time.Millisecond,
		targetBytes:        256 << 10,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:             40,
		compressedIngestBytesPerSec: 64 << 20,
	})

	require.Equal(t, 64, lanes)
}

func TestCentralQueueLanePolicyUsesHysteresis(t *testing.T) {
	policy := centralQueueLanePolicy{
		minLanes:           1,
		maxLanes:           64,
		backendMultiplier:  2,
		targetFillDuration: 500 * time.Millisecond,
		targetBytes:        256 << 10,
		hysteresisFactor:   2,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:               8,
		compressedIngestBytesPerSec:   7 << 20,
		previousEffectiveLaneCount:    16,
		previousEffectiveLaneCountSet: true,
	})

	require.Equal(t, 16, lanes)
}
```

- [ ] **Step 3: Run red tests and benchmark**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueLanePolicy' -count=1
go test . -run '^$' -bench 'BenchmarkCentralQueueCollectManyLanesManyItems' -benchmem -count=3
```

Expected:

- Policy tests fail to compile because the policy type does not exist yet.
- Benchmark records current allocation and CPU baseline for lease cost.

## Task 2: Implement Dynamic Lane Policy

**Files:**
- Create: `exporter/loadbalancingexporter/central_queue_lane_policy.go`
- Modify: `exporter/loadbalancingexporter/config.go`
- Modify: `exporter/loadbalancingexporter/config.schema.yaml`
- Modify: `exporter/loadbalancingexporter/config_test.go`

- [ ] **Step 1: Add the lane policy implementation**

Create `exporter/loadbalancingexporter/central_queue_lane_policy.go`:

```go
package loadbalancingexporter

import (
	"math"
	"time"
)

const (
	defaultCentralQueueMinLanes          = 1
	defaultCentralQueueMaxLanes          = 256
	defaultCentralQueueBackendMultiplier = 2
	defaultCentralQueueHysteresisFactor  = 2
)

type centralQueueLanePolicy struct {
	minLanes           int
	maxLanes           int
	backendMultiplier  int
	targetFillDuration time.Duration
	targetBytes        int64
	hysteresisFactor   int
}

type centralQueueLaneInputs struct {
	healthyBackends               int
	compressedIngestBytesPerSec   int64
	previousEffectiveLaneCount    int
	previousEffectiveLaneCountSet bool
}

func newCentralQueueLanePolicy(cfg CentralQueueConfig) centralQueueLanePolicy {
	minLanes := cfg.MinLanes
	if minLanes <= 0 {
		minLanes = defaultCentralQueueMinLanes
	}
	maxLanes := cfg.MaxLanes
	if maxLanes <= 0 {
		maxLanes = defaultCentralQueueMaxLanes
	}
	if maxLanes < minLanes {
		maxLanes = minLanes
	}
	backendMultiplier := cfg.BackendLaneMultiplier
	if backendMultiplier <= 0 {
		backendMultiplier = defaultCentralQueueBackendMultiplier
	}
	hysteresisFactor := cfg.LaneHysteresisFactor
	if hysteresisFactor <= 1 {
		hysteresisFactor = defaultCentralQueueHysteresisFactor
	}
	targetFillDuration := cfg.TargetLaneFillDuration
	if targetFillDuration <= 0 {
		targetFillDuration = minDuration(cfg.MaxBatchDelay/2, 500*time.Millisecond)
	}
	if targetFillDuration <= 0 {
		targetFillDuration = 500 * time.Millisecond
	}
	targetBytes := cfg.TargetCompressedBytes
	if targetBytes <= 0 {
		targetBytes = 1
	}
	return centralQueueLanePolicy{
		minLanes:           minLanes,
		maxLanes:           maxLanes,
		backendMultiplier:  backendMultiplier,
		targetFillDuration: targetFillDuration,
		targetBytes:        targetBytes,
		hysteresisFactor:   hysteresisFactor,
	}
}

func (p centralQueueLanePolicy) compute(inputs centralQueueLaneInputs) int {
	backendLimit := p.maxLanes
	if inputs.healthyBackends > 0 {
		backendLimit = inputs.healthyBackends * p.backendMultiplier
	}

	fillLimit := p.maxLanes
	if inputs.compressedIngestBytesPerSec > 0 && p.targetBytes > 0 && p.targetFillDuration > 0 {
		fillBytes := float64(inputs.compressedIngestBytesPerSec) * p.targetFillDuration.Seconds()
		fillLimit = int(math.Floor(fillBytes / float64(p.targetBytes)))
	}

	candidate := min(backendLimit, fillLimit, p.maxLanes)
	candidate = clampInt(candidate, p.minLanes, p.maxLanes)

	if !inputs.previousEffectiveLaneCountSet || p.hysteresisFactor <= 1 {
		return candidate
	}
	previous := clampInt(inputs.previousEffectiveLaneCount, p.minLanes, p.maxLanes)
	if candidate > previous && candidate < previous*p.hysteresisFactor {
		return previous
	}
	if candidate < previous && candidate*p.hysteresisFactor > previous {
		return previous
	}
	return candidate
}

func clampInt(v, low, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}
```

- [ ] **Step 2: Add config fields**

Extend `CentralQueueConfig` in `exporter/loadbalancingexporter/config.go`:

```go
type CentralQueueConfig struct {
	Enabled                      bool                    `mapstructure:"enabled"`
	MaxCompressedBytes           int64                   `mapstructure:"max_compressed_bytes"`
	PayloadCompression           QueuePayloadCompression `mapstructure:"payload_compression"`
	MaxUncompressedBatchBytes    int                     `mapstructure:"max_uncompressed_batch_bytes"`
	MaxInflightUncompressedBytes int64                   `mapstructure:"max_inflight_uncompressed_bytes"`
	TargetCompressedBytes        int64                   `mapstructure:"target_compressed_bytes"`
	MaxBatchDelay                time.Duration           `mapstructure:"max_batch_delay"`
	NumConsumers                 int                     `mapstructure:"num_consumers"`
	LaneCount                    int                     `mapstructure:"lane_count"`
	MinLanes                     int                     `mapstructure:"min_lanes"`
	MaxLanes                     int                     `mapstructure:"max_lanes"`
	BackendLaneMultiplier        int                     `mapstructure:"backend_lane_multiplier"`
	TargetLaneFillDuration       time.Duration           `mapstructure:"target_lane_fill_duration"`
	LaneHysteresisFactor         int                     `mapstructure:"lane_hysteresis_factor"`
}
```

Keep `lane_count` as an override for compatibility:

```go
func (c CentralQueueConfig) staticLaneCountOverride() int {
	if c.LaneCount > 0 {
		return c.LaneCount
	}
	return 0
}
```

- [ ] **Step 3: Update validation**

Add validation in `Config.Validate` for:

```text
central_queue.min_lanes >= 0
central_queue.max_lanes >= 0
central_queue.max_lanes >= central_queue.min_lanes when both are set
central_queue.backend_lane_multiplier >= 0
central_queue.target_lane_fill_duration >= 0
central_queue.lane_hysteresis_factor >= 0
```

- [ ] **Step 4: Run policy tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueLanePolicy|TestConfig' -count=1
```

Expected: lane policy tests pass and existing config tests pass.

## Task 3: Introduce Bucketed Queue Data Structure

**Files:**
- Create: `exporter/loadbalancingexporter/central_queue_bucket.go`
- Modify: `exporter/loadbalancingexporter/central_queue.go`
- Create: `exporter/loadbalancingexporter/central_queue_bucket_test.go`
- Modify: `exporter/loadbalancingexporter/central_queue_test.go`

- [ ] **Step 1: Add bucket structs**

Create `exporter/loadbalancingexporter/central_queue_bucket.go` with:

```go
package loadbalancingexporter

type centralQueueBucketKey string

type centralQueueBucket struct {
	key               centralQueueBucketKey
	routingKey        []byte
	items             []centralQueueItem
	compressedBytes   int
	uncompressedBytes int
	oldestEnqueuedAt  int64
	nextAttemptAt     int64
	readyHeapIndex    int
}

func newCentralQueueBucket(routingKey []byte) *centralQueueBucket {
	return &centralQueueBucket{
		key:            centralQueueBucketKey(routingKey),
		routingKey:     append([]byte(nil), routingKey...),
		readyHeapIndex: -1,
	}
}
```

- [ ] **Step 2: Add bucket append/remove helpers**

Add helpers in `central_queue_bucket.go`:

```go
func (b *centralQueueBucket) append(item centralQueueItem) {
	b.items = append(b.items, item)
	b.compressedBytes += item.compressedBytes
	b.uncompressedBytes += item.uncompressedBytes
	if b.oldestEnqueuedAt == 0 || item.enqueuedAtUnixNano < b.oldestEnqueuedAt {
		b.oldestEnqueuedAt = item.enqueuedAtUnixNano
	}
	if item.nextAttemptUnixNano > 0 && (b.nextAttemptAt == 0 || item.nextAttemptUnixNano < b.nextAttemptAt) {
		b.nextAttemptAt = item.nextAttemptUnixNano
	}
}

func (b *centralQueueBucket) rebuildStats() {
	b.compressedBytes = 0
	b.uncompressedBytes = 0
	b.oldestEnqueuedAt = 0
	b.nextAttemptAt = 0
	for _, item := range b.items {
		b.compressedBytes += item.compressedBytes
		b.uncompressedBytes += item.uncompressedBytes
		if b.oldestEnqueuedAt == 0 || item.enqueuedAtUnixNano < b.oldestEnqueuedAt {
			b.oldestEnqueuedAt = item.enqueuedAtUnixNano
		}
		if item.nextAttemptUnixNano > 0 && (b.nextAttemptAt == 0 || item.nextAttemptUnixNano < b.nextAttemptAt) {
			b.nextAttemptAt = item.nextAttemptUnixNano
		}
	}
}
```

- [ ] **Step 3: Add tests for bucket state**

Create `exporter/loadbalancingexporter/central_queue_bucket_test.go`:

```go
package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueBucketTracksStats(t *testing.T) {
	bucket := newCentralQueueBucket([]byte("lane-a"))
	bucket.append(centralQueueItem{
		compressedBytes:    10,
		uncompressedBytes:  100,
		enqueuedAtUnixNano: 20,
	})
	bucket.append(centralQueueItem{
		compressedBytes:     20,
		uncompressedBytes:   200,
		enqueuedAtUnixNano:  10,
		nextAttemptUnixNano: 50,
	})

	require.Equal(t, 30, bucket.compressedBytes)
	require.Equal(t, 300, bucket.uncompressedBytes)
	require.Equal(t, int64(10), bucket.oldestEnqueuedAt)
	require.Equal(t, int64(50), bucket.nextAttemptAt)
	require.Equal(t, []byte("lane-a"), bucket.routingKey)
}
```

- [ ] **Step 4: Run bucket tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueBucket' -count=1
```

Expected: bucket tests pass.

## Task 4: Replace Global Queue Scans With Bucketed Leasing

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue.go`
- Modify: `exporter/loadbalancingexporter/central_queue_test.go`
- Modify: `exporter/loadbalancingexporter/central_queue_benchmark_test.go`

- [ ] **Step 1: Add bucket fields to `centralQueue`**

Change `centralQueue` to keep buckets:

```go
type centralQueue struct {
	settings centralQueueSettings
	mu       sync.Mutex
	notify   chan struct{}

	bucketsByKey map[centralQueueBucketKey]*centralQueueBucket
	readyBuckets centralQueueReadyHeap

	currentCompressedBytes int64
	currentInflightBytes   int64
	stopped                bool

	enqueuedAtCounts      map[int64]int
	enqueuedAtHeapEntries map[int64]struct{}
	oldestEnqueuedAt      centralQueueEnqueuedAtHeap
}
```

Initialize `bucketsByKey` and `notify` in `newCentralQueue`.

- [ ] **Step 2: Route enqueue to a bucket**

In `enqueueAt`, replace `q.items = append(q.items, item)` with:

```go
key := centralQueueBucketKey(item.routingKey)
bucket := q.bucketsByKey[key]
if bucket == nil {
	bucket = newCentralQueueBucket(item.routingKey)
	q.bucketsByKey[key] = bucket
}
bucket.append(item)
q.updateReadyBucketLocked(bucket, now.UnixNano())
q.signalNotEmptyLocked()
```

- [ ] **Step 3: Implement ready heap**

Add `centralQueueReadyHeap` in `central_queue_bucket.go`:

```go
type centralQueueReadyHeap []*centralQueueBucket

func (h centralQueueReadyHeap) Len() int { return len(h) }

func (h centralQueueReadyHeap) Less(i, j int) bool {
	left := h[i]
	right := h[j]
	if left.nextAttemptAt != right.nextAttemptAt {
		if left.nextAttemptAt == 0 {
			return true
		}
		if right.nextAttemptAt == 0 {
			return false
		}
		return left.nextAttemptAt < right.nextAttemptAt
	}
	return left.oldestEnqueuedAt < right.oldestEnqueuedAt
}

func (h centralQueueReadyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].readyHeapIndex = i
	h[j].readyHeapIndex = j
}

func (h *centralQueueReadyHeap) Push(x any) {
	bucket := x.(*centralQueueBucket)
	bucket.readyHeapIndex = len(*h)
	*h = append(*h, bucket)
}

func (h *centralQueueReadyHeap) Pop() any {
	old := *h
	n := len(old)
	bucket := old[n-1]
	old[n-1] = nil
	bucket.readyHeapIndex = -1
	*h = old[:n-1]
	return bucket
}
```

- [ ] **Step 4: Lease from one bucket**

Replace `tryLease` global scan with:

```go
func (q *centralQueue) tryLease(now time.Time) (*centralQueueLease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.currentCompressedBytes == 0 {
		if q.stopped {
			return nil, errCentralQueueStopped
		}
		return nil, nil
	}

	nowUnixNano := now.UnixNano()
	readyInflightBlocked := false

	for q.readyBuckets.Len() > 0 {
		bucket := q.readyBuckets[0]
		if bucket.nextAttemptAt > nowUnixNano {
			break
		}
		candidate, ok := q.buildWindowCandidateFromBucketLocked(bucket, now)
		if !ok {
			q.updateReadyBucketLocked(bucket, nowUnixNano)
			break
		}
		if q.currentInflightBytes+int64(candidate.window.uncompressedBytes) > q.settings.maxInflightUncompressedBytes {
			readyInflightBlocked = true
			break
		}
		q.removeWindowFromBucketLocked(bucket, candidate.window.count)
		q.currentInflightBytes += int64(candidate.window.uncompressedBytes)
		snapshot := q.snapshotLocked()
		q.settings.telemetry.record(context.Background(), snapshot)
		q.settings.telemetry.recordWindow(context.Background(), candidate.window, q.settings.targetCompressedBytes)
		lease := &centralQueueLease{queue: q, window: candidate.window}
		if len(candidate.window.items) > 0 {
			lease.item = candidate.window.items[0]
		}
		return lease, nil
	}

	if readyInflightBlocked {
		return nil, errCentralQueueInflightFull
	}
	return nil, nil
}
```

- [ ] **Step 5: Add bucket candidate builder**

Add:

```go
func (q *centralQueue) buildWindowCandidateFromBucketLocked(bucket *centralQueueBucket, now time.Time) (centralQueueWindowCandidate, bool) {
	nowUnixNano := now.UnixNano()
	candidate := centralQueueWindowCandidate{
		window: centralQueueWindow{
			routingKey: append([]byte(nil), bucket.routingKey...),
			items:      make([]centralQueueItem, 0, 1),
		},
	}
	blockedByHardLimit := false
	for _, item := range bucket.items {
		if item.nextAttemptUnixNano > nowUnixNano {
			continue
		}
		if len(candidate.window.items) > 0 && q.windowWouldExceedLimit(candidate.window, item) {
			blockedByHardLimit = true
			break
		}
		candidate.window.items = append(candidate.window.items, item)
		candidate.window.compressedBytes += item.compressedBytes
		candidate.window.uncompressedBytes += item.uncompressedBytes
		candidate.window.count += item.count
		if item.attempt > candidate.window.maxAttempt {
			candidate.window.maxAttempt = item.attempt
		}
		if candidate.window.oldestEnqueuedAt == 0 || item.enqueuedAtUnixNano < candidate.window.oldestEnqueuedAt {
			candidate.window.oldestEnqueuedAt = item.enqueuedAtUnixNano
		}
		if int64(candidate.window.compressedBytes) >= q.settings.targetCompressedBytes {
			candidate.window.flushReason = centralQueueFlushReasonTargetReached
			return candidate, true
		}
	}
	if len(candidate.window.items) == 0 {
		return centralQueueWindowCandidate{}, false
	}
	if q.stopped {
		candidate.window.flushReason = centralQueueFlushReasonShutdown
		return candidate, true
	}
	if blockedByHardLimit {
		candidate.window.flushReason = centralQueueFlushReasonHardCap
		return candidate, true
	}
	if q.settings.maxBatchDelay <= 0 {
		candidate.window.flushReason = centralQueueFlushReasonMaxDelayLowTraffic
		return candidate, true
	}
	oldest := time.Unix(0, candidate.window.oldestEnqueuedAt)
	if !oldest.IsZero() && now.Sub(oldest) >= q.settings.maxBatchDelay {
		candidate.window.flushReason = centralQueueFlushReasonMaxDelayLowTraffic
		return candidate, true
	}
	return centralQueueWindowCandidate{}, false
}
```

- [ ] **Step 6: Preserve current behavior tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueue' -count=1
```

Expected: existing central queue behavior tests pass.

## Task 5: Add Wakeups And Avoid Poll-Only Leasing

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue.go`
- Modify: `exporter/loadbalancingexporter/central_queue_test.go`

- [ ] **Step 1: Signal consumers when work may be ready**

Add:

```go
func (q *centralQueue) signalNotEmptyLocked() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
```

- [ ] **Step 2: Wait on enqueue, retry deadline, or context**

Update `lease` so it waits on `q.notify`, context cancellation, or the next retry timer. Keep a bounded fallback timer of `centralQueueLeasePollInterval` only as a safety net.

- [ ] **Step 3: Test wakeup behavior**

Add a test:

```go
func TestCentralQueueLeaseWakesOnEnqueue(t *testing.T) {
	q := newCentralQueue(centralQueueSettings{
		maxCompressedBytes:           1024,
		maxInflightUncompressedBytes: 1024,
		targetCompressedBytes:        10,
		maxBatchDelay:                time.Second,
		telemetry:                    newNopCentralQueueTelemetry(),
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	leases := make(chan *centralQueueLease, 1)
	go func() {
		lease, err := q.lease(ctx)
		require.NoError(t, err)
		leases <- lease
	}()

	require.NoError(t, q.enqueue(centralQueueItem{
		signal:            signalKindLogs,
		routingKey:        []byte("lane-a"),
		compressedBytes:   10,
		uncompressedBytes: 10,
		count:             1,
	}))

	require.NotNil(t, <-leases)
}
```

- [ ] **Step 4: Run wakeup tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'TestCentralQueueEnqueueSignalsLeaseWaiters|TestCentralQueueDoneSignalsInflightWaiters' -count=1
```

Expected: queue consumers wake without waiting for repeated 10ms polling.

## Task 6: Integrate Dynamic Effective Lanes With Log And Metric Exporters

**Files:**
- Modify: `exporter/loadbalancingexporter/log_exporter.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter.go`
- Modify: `exporter/loadbalancingexporter/loadbalancer.go`
- Modify: `exporter/loadbalancingexporter/log_exporter_test.go`
- Modify: `exporter/loadbalancingexporter/metrics_exporter_test.go`

- [ ] **Step 1: Add load balancer backend count helper**

Add to `loadbalancer.go`:

```go
func (lb *loadBalancer) backendCount() int {
	lb.updateLock.RLock()
	defer lb.updateLock.RUnlock()
	if lb.ring == nil {
		return 0
	}
	return len(lb.ring.items())
}
```

If `hashRing` does not expose items, add an internal method that returns the unique endpoint count.

- [ ] **Step 2: Track recent compressed ingest bytes**

Add to `centralQueue`:

```go
compressedIngestBytesWindow int64
compressedIngestWindowStart time.Time
effectiveLaneCount          int
effectiveLaneCountSet       bool
lanePolicy                  centralQueueLanePolicy
```

On enqueue, add compressed bytes into the current 30s window. When the window rolls, compute bytes/sec and recompute effective lanes.

- [ ] **Step 3: Route new items through current effective lanes**

In `log_exporter.go` and `metrics_exporter.go`, replace static `centralQueueLaneCount` usage with a method:

```go
func (q *centralQueue) laneRoutingKey(signal signalKind, routingKey []byte, backendCount int) []byte {
	laneCount := q.effectiveLaneCountForEnqueue(backendCount)
	return centralQueueLaneRoutingKey(signal, routingKey, laneCount)
}
```

Use this for newly enqueued logs/metrics. Existing queued items retain their already computed `routingKey`.

- [ ] **Step 4: Respect static `lane_count` override**

If `central_queue.lane_count > 0`, keep existing fixed lane behavior. Record a metric attribute or log line showing the static override is active.

- [ ] **Step 5: Test few-backend lane cap**

Add a test where backend count is `2`, ingest rate is low, and `laneRoutingKey` produces no more than the computed effective lane count.

- [ ] **Step 6: Test many-backend high-throughput lane growth**

Add a test where backend count is high, ingest rate is high, and effective lane count reaches `256`.

- [ ] **Step 7: Run exporter tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run 'Test.*CentralQueue|Test.*Lane|Test.*ByteBatching' -count=1
```

Expected: log and metric exporter queue tests pass.

## Task 7: Telemetry, Docs, And Compatibility

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue_telemetry.go`
- Modify: `exporter/loadbalancingexporter/central_queue_telemetry_test.go`
- Modify: `exporter/loadbalancingexporter/metadata.yaml`
- Modify: `exporter/loadbalancingexporter/documentation.md`

- [ ] **Step 1: Add lane telemetry**

Expose:

```text
otelcol_loadbalancer_central_queue_effective_lanes
otelcol_loadbalancer_central_queue_ready_lanes
otelcol_loadbalancer_central_queue_lane_fill_seconds
otelcol_loadbalancer_central_queue_lease_inspected_items
```

Use `lease_inspected_items` only as low-cardinality aggregate telemetry.

- [ ] **Step 2: Document operator knobs**

Document:

```yaml
central_queue:
  num_consumers: 30
  min_lanes: 1
  max_lanes: 256
  backend_lane_multiplier: 2
  target_lane_fill_duration: 500ms
  lane_hysteresis_factor: 2
```

State that `lane_count` is a compatibility override and should be avoided unless directed by engineering.

- [ ] **Step 3: Run generated metadata checks**

Run:

```bash
make -C exporter/loadbalancingexporter generate
cd exporter/loadbalancingexporter
go test . -run 'TestGenerated|TestCentralQueueTelemetry' -count=1
```

Expected: generated metadata and telemetry tests pass.

## Task 8: Green Benchmarks And Local Pprof Proof

**Files:**
- Modify: `exporter/loadbalancingexporter/central_queue_benchmark_test.go`
- Create: `artifacts/lb-central-queue-dynamic-lanes/README.md`

- [ ] **Step 1: Run before/after benchmarks**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run '^$' -bench 'BenchmarkCentralQueueCollectManyLanesManyItems' -benchmem -count=10 | tee /tmp/lb-central-queue-bench.txt
```

Expected:

- `allocs/op` drops materially versus the red baseline.
- `B/op` drops materially versus the red baseline.
- `ns/op` drops materially versus the red baseline.

- [ ] **Step 2: Capture local CPU pprof**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -run '^$' -bench 'BenchmarkCentralQueueCollectManyLanesManyItems' -benchmem -cpuprofile /tmp/lb-central-queue.cpu.pb.gz -memprofile /tmp/lb-central-queue.heap.pb.gz
go tool pprof -top /tmp/lb-central-queue.cpu.pb.gz | tee /tmp/lb-central-queue.cpu.top.txt
go tool pprof -top -alloc_space /tmp/lb-central-queue.heap.pb.gz | tee /tmp/lb-central-queue.heap.top.txt
```

Expected:

- Queue lease/window selection is no longer the dominant allocator.
- GC share is lower than the production pprof pattern.
- No new hot path replaces the old global scan with similar cost.

- [ ] **Step 3: Run full package tests**

Run:

```bash
cd exporter/loadbalancingexporter
go test . -count=1
```

Expected: package tests pass.

## Task 9: Rollout Guardrails

**Files:**
- Modify: `exporter/loadbalancingexporter/config.go`
- Modify: `exporter/loadbalancingexporter/documentation.md`
- Modify downstream configuration rendering after this PR merges so managed deployments use dynamic-lane guardrails.

- [ ] **Step 1: Keep old behavior available**

Ensure `central_queue.lane_count` preserves fixed lane behavior when set.

- [ ] **Step 2: Default new behavior through guardrails**

When `lane_count` is unset, use dynamic effective lanes with defaults:

```text
min_lanes=1
max_lanes=256
backend_lane_multiplier=2
target_lane_fill_duration=500ms
lane_hysteresis_factor=2
```

- [ ] **Step 3: Prepare downstream configuration changes**

Render these fields for LB-enabled deployments:

```yaml
central_queue:
  num_consumers: 30
  min_lanes: 1
  max_lanes: 256
  backend_lane_multiplier: 2
  target_lane_fill_duration: 500ms
  lane_hysteresis_factor: 2
```

Do not set `lane_count` in normal managed variations.

- [ ] **Step 4: Validate in staging before customer rollout**

Use staging cluster with LB queue enabled and capture:

```text
LB CPU
LB GC share
effective lanes
ready lanes
queue compressed bytes
oldest queue age
backend p95/p99
receiver refusals
HAProxy fallback
generated vs delivered
```

- [ ] **Step 5: Validate in production canary before customer rollout**

Repeat staging validation in an owned production canary. Only prepare broader customer rollout after the canary shows lower LB CPU/GC and no latency/refusal regression.

## Acceptance Gates

Green means all of the following are true:

- Red policy tests pass.
- Existing central queue behavior tests pass.
- Log and metric exporter central queue tests pass.
- Benchmark shows lower `allocs/op`, `B/op`, and `ns/op` for many-lane/many-item lease path.
- Local pprof no longer has queue candidate selection as dominant allocator.
- Few-backend scenario computes low effective lanes and avoids tiny delay-flushed batches.
- Many-backend/high-throughput scenario can use high effective lanes without global queue scan cost.
- Static `lane_count` override still works for rollback.
- Staging pprof confirms lower LB CPU/GC before broader customer rollout.

## Open Decision For Review

The default `max_lanes=256` preserves the high-traffic ceiling needed for large clusters. The fill-rate and backend-count terms remain the first-principles controls that keep low-worker or low-rate clusters from over-sharding.
