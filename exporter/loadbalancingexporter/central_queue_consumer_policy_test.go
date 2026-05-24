// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"math"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCentralQueueConsumerPolicyCapsFewBackendsAcrossLBReplicas(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:               120,
		minConsumers:               1,
		targetCompressedBytes:      256 << 10,
		maxInflightSendsPerBackend: 1,
		activeLoadBalancerReplicas: 3,
	}

	decision := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 1 << 30,
		readyBackends:        6,
	})

	require.Equal(t, 4096, decision.queueDemandConsumers)
	require.Equal(t, 2, decision.backendSafeConsumersPerLB)
	require.Equal(t, 2, decision.effectiveConsumers)
}

func TestCentralQueueConsumerControllerUsesConfiguredActiveLoadBalancerReplicas(t *testing.T) {
	controller := newCentralQueueConsumerController(120, 256<<10, 3)
	var active atomic.Int64

	decision, acquired, _ := controller.tryAcquire(&active, 1<<30, 6, false)

	require.True(t, acquired)
	require.Equal(t, 2, decision.backendSafeConsumersPerLB)
	require.Equal(t, 2, decision.effectiveConsumers)
}

func TestCentralQueueConsumerControllerReportsDecisionChanges(t *testing.T) {
	controller := newCentralQueueConsumerController(120, 256<<10, 1)
	var active atomic.Int64

	first, acquired, changed := controller.tryAcquire(&active, 1<<30, 2, false)
	require.True(t, acquired)
	require.True(t, changed)
	require.Equal(t, 2, first.effectiveConsumers)
	active.Store(0)

	second, acquired, changed := controller.tryAcquire(&active, 1<<30, 2, false)
	require.True(t, acquired)
	require.False(t, changed)
	require.Equal(t, first, second)
	active.Store(0)

	third, acquired, changed := controller.tryAcquire(&active, 1<<30, 3, false)
	require.True(t, acquired)
	require.True(t, changed)
	require.Equal(t, 3, third.effectiveConsumers)
}

func TestCentralQueueConsumerPolicyStopsWhenBackendShareIsFractional(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:               120,
		minConsumers:               1,
		targetCompressedBytes:      256 << 10,
		maxInflightSendsPerBackend: 1,
		activeLoadBalancerReplicas: 4,
	}

	decision := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 1 << 30,
		readyBackends:        3,
	})

	require.Equal(t, 0, decision.backendSafeConsumersPerLB)
	require.Equal(t, 0, decision.effectiveConsumers)
	require.Equal(t, centralQueueConsumerLimitReasonBackendCapacity, decision.limitReason)
}

func TestCentralQueueConsumerPolicyQueueGrowthIncreasesDemandUntilBackendSafeCap(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:               120,
		minConsumers:               1,
		targetCompressedBytes:      256 << 10,
		maxInflightSendsPerBackend: 1,
	}

	lowDemand := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 256 << 10,
		readyBackends:        20,
	})
	require.Equal(t, 1, lowDemand.effectiveConsumers)

	higherDemand := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 3 << 20,
		readyBackends:        20,
	})
	require.Equal(t, 12, higherDemand.effectiveConsumers)

	backendCapped := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 64 << 20,
		readyBackends:        20,
	})
	require.Equal(t, 20, backendCapped.effectiveConsumers)
}

func TestCentralQueueConsumerPolicyBackendPressureReducesQuickly(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:                 120,
		minConsumers:                 1,
		targetCompressedBytes:        256 << 10,
		maxInflightSendsPerBackend:   1,
		previousEffectiveConsumers:   16,
		previousEffectiveConsumersOK: true,
	}

	decision := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 64 << 20,
		readyBackends:        16,
		backendPressure:      true,
	})

	require.Equal(t, centralQueueConsumerPressureReducing, decision.pressureState)
	require.Equal(t, 8, decision.effectiveConsumers)
}

func TestCentralQueueConsumerPolicyBackendPressureReducesFirstSample(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:               120,
		minConsumers:               1,
		targetCompressedBytes:      256 << 10,
		maxInflightSendsPerBackend: 1,
	}

	decision := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 64 << 20,
		readyBackends:        16,
		backendPressure:      true,
	})

	require.Equal(t, centralQueueConsumerPressureReducing, decision.pressureState)
	require.Equal(t, 8, decision.effectiveConsumers)
}

func TestCentralQueueConsumerPolicyPressureRecoveryIsGradual(t *testing.T) {
	policy := centralQueueConsumerPolicy{
		maxConsumers:                 120,
		minConsumers:                 1,
		targetCompressedBytes:        256 << 10,
		maxInflightSendsPerBackend:   1,
		pressureRecoveryStep:         1,
		previousEffectiveConsumers:   4,
		previousEffectiveConsumersOK: true,
		pressureRecoveryActive:       true,
	}

	decision := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: 64 << 20,
		readyBackends:        16,
	})

	require.Equal(t, centralQueueConsumerPressureRecovering, decision.pressureState)
	require.Equal(t, 5, decision.effectiveConsumers)
}

func TestCeilDivInt64ToIntDoesNotOverflow(t *testing.T) {
	require.Equal(t, int(math.MaxInt64/2+1), ceilDivInt64ToInt(math.MaxInt64, 2))
}

func TestCeilDivInt64ToIntClampsToMaxIntOn32Bit(t *testing.T) {
	if strconv.IntSize == 64 {
		t.Skip("int64 quotient cannot exceed int on 64-bit builds")
	}
	maxInt64 := int64(math.MaxInt)
	require.Equal(t, math.MaxInt, ceilDivInt64ToInt(maxInt64+1, 1))
}
