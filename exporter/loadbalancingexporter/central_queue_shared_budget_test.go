// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueSixControllersShareFiniteBackend(t *testing.T) {
	t.Run("default count sustains admission pressure", func(t *testing.T) {
		fleet := newSharedBudgetFleet(t, 1, 18)
		for range 4 {
			wave := fleet.drainWave(t, false)
			require.Positive(t, wave.refused, "six independent sole-owner budgets keep oversubscribing 18 slots")
			fleet.requireRetained(t)
		}
	})

	t.Run("converged membership bounds sends and recovers", func(t *testing.T) {
		fleet := newSharedBudgetFleet(t, 6, 18)
		healthy := fleet.drainWave(t, false)
		// For B=18 and N=6, each peer gets at most 3 outstanding sends.
		// The general converged bound is max(B,N), retaining minimum-one
		// progress only when N>B. This fixture has no fractional allowance.
		require.LessOrEqual(t, healthy.attempted, 18, "healthy ramp must remain inside the converged shared budget")
		require.Zero(t, healthy.refused)
		require.Equal(t, 18, healthy.completed)
		fleet.requireRetained(t)

		pressured := fleet.drainWave(t, true)
		require.LessOrEqual(t, pressured.attempted, 18)
		require.Zero(t, pressured.refused)
		require.Less(t, pressured.completed, healthy.completed)
		fleet.requireRetained(t)

		recovered := fleet.drainWave(t, false)
		require.LessOrEqual(t, recovered.attempted, 18)
		require.Zero(t, recovered.refused)
		require.Greater(t, recovered.completed, pressured.completed, "healthy admission advances recovery without a stale pressure latch")
		fleet.requireRetained(t)
	})

	t.Run("fractional share keeps documented minimum-one allowance", func(t *testing.T) {
		fleet := newSharedBudgetFleet(t, 6, 4)
		wave := fleet.drainWave(t, false)
		require.Equal(t, 6, wave.attempted, "six minimum-one peers may exceed four slots by exactly two")
		require.Equal(t, 2, wave.refused)
		require.Equal(t, 4, wave.completed)
		fleet.requireRetained(t)
	})
}

type sharedBudgetPeer struct {
	controller *centralQueueConsumerController
	queue      *centralQueue
	active     atomic.Int64
	pressure   bool
}

func TestCentralQueueConsumerControllerRetainsBatchesUntilMembershipIsKnown(t *testing.T) {
	fleet := newSharedBudgetFleet(t, 1, 18)
	replicas, known := 0, false
	for _, peer := range fleet.peers {
		peer.controller.setMembershipSource(func() (int, bool) { return replicas, known })
	}

	unknown := fleet.drainWave(t, false)
	require.Zero(t, unknown.attempted, "unknown membership must retain batches rather than default to one LB")
	fleet.requireRetained(t)

	known = true
	empty := fleet.drainWave(t, false)
	require.Zero(t, empty.attempted, "an empty membership must not become the legacy default of one")
	fleet.requireRetained(t)

	replicas, known = 6, true
	converged := fleet.drainWave(t, false)
	require.Equal(t, 18, converged.attempted)
	require.Zero(t, converged.refused)
	fleet.requireRetained(t)

	replicas = 12
	for _, peer := range fleet.peers {
		limit, budgetKnown := centralQueueEffectiveConsumersForLanes(peer.controller, 30, 18)
		require.True(t, budgetKnown)
		require.Equal(t, 1, limit, "lane planning uses the fresh share rather than a cached healthy limit")
	}
	scaled := fleet.drainWave(t, false)
	require.Equal(t, 6, scaled.attempted, "a larger membership immediately lowers each peer to its minimum-one share")
	require.Zero(t, scaled.refused)
	fleet.requireRetained(t)

	known = false
	for _, peer := range fleet.peers {
		limit, budgetKnown := centralQueueEffectiveConsumersForLanes(peer.controller, 30, 18)
		require.True(t, budgetKnown)
		require.Zero(t, limit, "unknown membership gives a holding budget, not a backend-count fallback")
	}
	stale := fleet.drainWave(t, false)
	require.Zero(t, stale.attempted, "lost freshness must not reuse the previous healthy budget")
	fleet.requireRetained(t)

	replicas, known = 6, true
	recovered := fleet.drainWave(t, false)
	require.Equal(t, 18, recovered.attempted)
	require.Zero(t, recovered.refused)
	fleet.requireRetained(t)
}

type sharedBudgetFleet struct {
	peers     []*sharedBudgetPeer
	slots     chan struct{}
	delivered map[string]bool
	now       time.Time
}

type sharedBudgetWave struct {
	attempted int
	refused   int
	completed int
}

func newSharedBudgetFleet(t *testing.T, membership, backendSlots int) *sharedBudgetFleet {
	t.Helper()
	fleet := &sharedBudgetFleet{
		slots:     make(chan struct{}, backendSlots),
		delivered: make(map[string]bool),
		now:       time.Unix(1700000000, 0),
	}
	for peerID := range 6 {
		peer := &sharedBudgetPeer{
			controller: newCentralQueueConsumerController(30, 1, membership),
			queue: newCentralQueue(centralQueueSettings{
				maxCompressedBytes:           4096,
				maxInflightUncompressedBytes: 4096,
				maxUncompressedBatchBytes:    1,
				targetCompressedBytes:        1,
			}),
		}
		for batchID := range 30 {
			require.NoError(t, peer.queue.enqueueAt(centralQueueItem{
				signal:            signalKindLogs,
				routingKey:        []byte(fmt.Sprintf("%d/%d", peerID, batchID)),
				payload:           []byte{byte(batchID)},
				compressedBytes:   1,
				uncompressedBytes: 1,
				count:             1,
			}, fleet.now))
		}
		fleet.peers = append(fleet.peers, peer)
	}
	return fleet
}

func (f *sharedBudgetFleet) drainWave(t *testing.T, forcedPressure bool) sharedBudgetWave {
	t.Helper()
	type pendingSend struct {
		lease    *centralQueueLease
		accepted bool
	}
	var wave sharedBudgetWave
	var pending []pendingSend
	blocked := make([]bool, len(f.peers))
	refused := make([]bool, len(f.peers))
	f.now = f.now.Add(10 * time.Second)
	// Hold successful and refused responses until the wave ends, so attempted
	// measures simultaneous outstanding sends rather than cumulative retries.
	for range 30 {
		for peerID, peer := range f.peers {
			if blocked[peerID] {
				continue
			}
			lease, err := peer.queue.tryLeaseWithAcquire(f.now, func(bytes int64, _ centralQueueWindow) (func(), bool) {
				_, acquired, _ := peer.controller.tryAcquire(&peer.active, bytes, cap(f.slots), forcedPressure || peer.pressure)
				return func() { peer.active.Add(-1) }, acquired
			})
			if errors.Is(err, errCentralQueueConsumersFull) || lease == nil {
				require.True(t, err == nil || errors.Is(err, errCentralQueueConsumersFull))
				blocked[peerID] = true
				continue
			}
			require.NoError(t, err)
			wave.attempted++
			select {
			case f.slots <- struct{}{}:
				pending = append(pending, pendingSend{lease: lease, accepted: true})
			default:
				wave.refused++
				refused[peerID] = true
				pending = append(pending, pendingSend{lease: lease})
			}
		}
	}
	for _, send := range pending {
		if send.accepted {
			key := string(send.lease.window.routingKey)
			require.False(t, f.delivered[key], "a retained batch must not be completed twice")
			f.delivered[key] = true
			wave.completed += send.lease.window.count
			send.lease.done()
			<-f.slots
		} else {
			require.NoError(t, send.lease.requeue(f.now))
		}
		send.lease.releaseConsumer()
	}
	for peerID, peer := range f.peers {
		peer.pressure = refused[peerID]
		require.Zero(t, peer.active.Load())
	}
	t.Logf("attempted=%d refused=%d completed=%d", wave.attempted, wave.refused, wave.completed)
	return wave
}

func (f *sharedBudgetFleet) requireRetained(t *testing.T) {
	t.Helper()
	retained := 0
	for _, peer := range f.peers {
		retained += peer.queue.len()
	}
	require.Equal(t, 180, retained+len(f.delivered), "real queues retain every batch until finite-backend completion")
}
