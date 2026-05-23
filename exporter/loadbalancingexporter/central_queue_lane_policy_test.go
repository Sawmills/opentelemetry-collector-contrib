// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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
		hysteresisFactor:   2,
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
		hysteresisFactor:   2,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:             40,
		compressedIngestBytesPerSec: 64 << 20,
	})

	require.Equal(t, 64, lanes)
}

func TestCentralQueueLanePolicyDefaultAllowsBigIDScaleWhenFillRateSupportsIt(t *testing.T) {
	policy := newCentralQueueLanePolicy(createDefaultConfig().(*Config).CentralQueue)

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:             200,
		compressedIngestBytesPerSec: 512 << 20,
	})

	require.Equal(t, 256, lanes)
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

func TestCentralQueueLanePolicyUsesBackendCountWhenRateUnknown(t *testing.T) {
	policy := centralQueueLanePolicy{
		minLanes:           1,
		maxLanes:           64,
		backendMultiplier:  2,
		targetFillDuration: 500 * time.Millisecond,
		targetBytes:        256 << 10,
		hysteresisFactor:   2,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:             4,
		compressedIngestBytesPerSec: 0,
	})

	require.Equal(t, 4, lanes)
}

func TestCentralQueueLanePolicyUsesEffectiveConsumersForBackendLaneCap(t *testing.T) {
	policy := centralQueueLanePolicy{
		minLanes:           1,
		maxLanes:           64,
		backendMultiplier:  2,
		targetFillDuration: 500 * time.Millisecond,
		targetBytes:        256 << 10,
		hysteresisFactor:   2,
	}

	lanes := policy.compute(centralQueueLaneInputs{
		healthyBackends:             100,
		effectiveConsumers:          4,
		compressedIngestBytesPerSec: 64 << 20,
	})

	require.Equal(t, 8, lanes)
}

func TestCentralQueueLaneControllerRecomputesFromBackendCountAndRate(t *testing.T) {
	cfg := createDefaultConfig().(*Config).CentralQueue
	controller := newCentralQueueLaneController(cfg)
	now := time.Unix(10, 0)

	require.Equal(t, 4, controller.laneCount(4, now))

	for i := range 30 {
		controller.observeCompressedBytes(20<<20, now.Add(time.Duration(i)*time.Second))
	}

	require.Equal(t, 8, controller.laneCount(4, now.Add(31*time.Second)))
}

func TestCentralQueueLaneControllerRateRollWithBackendChangeUsesStablePreviousLanes(t *testing.T) {
	cfg := createDefaultConfig().(*Config).CentralQueue
	cfg.MaxLanes = 64
	cfg.TargetLaneFillDuration = 500 * time.Millisecond
	controller := newCentralQueueLaneController(cfg)
	now := time.Unix(10, 0)

	require.Equal(t, 4, controller.laneCount(4, now))
	require.Equal(t, 4, controller.observeCompressedBytes(150<<20, now))

	require.Equal(t, 10, controller.laneCount(5, now.Add(31*time.Second)))
}

func TestCentralQueueLaneControllerHonorsFixedOverride(t *testing.T) {
	cfg := createDefaultConfig().(*Config).CentralQueue
	cfg.LaneCount = 3
	controller := newCentralQueueLaneController(cfg)
	now := time.Unix(10, 0)

	require.Equal(t, 3, controller.laneCount(40, now))
	require.Equal(t, 3, controller.observeCompressedBytes(64<<20, now.Add(31*time.Second)))
}
