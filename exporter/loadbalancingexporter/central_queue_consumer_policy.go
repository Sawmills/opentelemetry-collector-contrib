// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"math"
	"sync"
)

type centralQueueConsumerLimitReason string

const (
	centralQueueConsumerLimitReasonQueueEmpty      centralQueueConsumerLimitReason = "queue_empty"
	centralQueueConsumerLimitReasonQueueDemand     centralQueueConsumerLimitReason = "queue_demand"
	centralQueueConsumerLimitReasonBackendCapacity centralQueueConsumerLimitReason = "backend_capacity"
	centralQueueConsumerLimitReasonConfiguredMax   centralQueueConsumerLimitReason = "configured_max"
	centralQueueConsumerLimitReasonBackendPressure centralQueueConsumerLimitReason = "backend_pressure"
	centralQueueConsumerLimitReasonRecovery        centralQueueConsumerLimitReason = "recovery"
	defaultCentralQueueMinConsumers                                                = 1
	defaultCentralQueueMaxInflightSendsPerBackend                                  = 1
)

var centralQueueConsumerLimitReasons = []centralQueueConsumerLimitReason{
	centralQueueConsumerLimitReasonQueueEmpty,
	centralQueueConsumerLimitReasonQueueDemand,
	centralQueueConsumerLimitReasonBackendCapacity,
	centralQueueConsumerLimitReasonConfiguredMax,
	centralQueueConsumerLimitReasonBackendPressure,
	centralQueueConsumerLimitReasonRecovery,
}

type centralQueueConsumerPressureState string

const (
	centralQueueConsumerPressureStable     centralQueueConsumerPressureState = "stable"
	centralQueueConsumerPressureReducing   centralQueueConsumerPressureState = "reducing"
	centralQueueConsumerPressureRecovering centralQueueConsumerPressureState = "recovering"
)

var centralQueueConsumerPressureStates = []centralQueueConsumerPressureState{
	centralQueueConsumerPressureStable,
	centralQueueConsumerPressureReducing,
	centralQueueConsumerPressureRecovering,
}

type centralQueueConsumerPolicy struct {
	minConsumers                 int
	maxConsumers                 int
	targetCompressedBytes        int64
	maxInflightSendsPerBackend   int
	activeLoadBalancerReplicas   int
	pressureRecoveryStep         int
	previousEffectiveConsumers   int
	previousEffectiveConsumersOK bool
	pressureRecoveryActive       bool
}

type centralQueueConsumerInputs struct {
	queueCompressedBytes int64
	readyBackends        int
	backendPressure      bool
}

type centralQueueConsumerResult struct {
	effectiveConsumers        int
	queueDemandConsumers      int
	backendSafeConsumersPerLB int
	limitReason               centralQueueConsumerLimitReason
	pressureState             centralQueueConsumerPressureState
}

func (p centralQueueConsumerPolicy) compute(inputs centralQueueConsumerInputs) centralQueueConsumerResult {
	if inputs.queueCompressedBytes <= 0 {
		return centralQueueConsumerResult{
			limitReason:   centralQueueConsumerLimitReasonQueueEmpty,
			pressureState: centralQueueConsumerPressureStable,
		}
	}

	maxConsumers := p.maxConsumers
	if maxConsumers <= 0 {
		maxConsumers = defaultCentralQueueNumConsumers
	}
	minConsumers := p.minConsumers
	if minConsumers <= 0 {
		minConsumers = defaultCentralQueueMinConsumers
	}
	if minConsumers > maxConsumers {
		minConsumers = maxConsumers
	}

	targetBytes := p.targetCompressedBytes
	if targetBytes <= 0 {
		targetBytes = defaultCentralQueueTargetCompressedBytes
	}
	queueDemand := ceilDivInt64ToInt(inputs.queueCompressedBytes, targetBytes)
	if queueDemand <= 0 {
		queueDemand = minConsumers
	}

	backendSafe := p.backendSafeConsumersPerLB(inputs.readyBackends)
	if backendSafe <= 0 {
		return centralQueueConsumerResult{
			queueDemandConsumers:      queueDemand,
			backendSafeConsumersPerLB: backendSafe,
			limitReason:               centralQueueConsumerLimitReasonBackendCapacity,
			pressureState:             centralQueueConsumerPressureStable,
		}
	}

	effective := min(maxConsumers, queueDemand, backendSafe)
	reason := centralQueueConsumerLimitReasonConfiguredMax
	switch effective {
	case backendSafe:
		reason = centralQueueConsumerLimitReasonBackendCapacity
	case queueDemand:
		reason = centralQueueConsumerLimitReasonQueueDemand
	}
	effective = clampInt(effective, min(minConsumers, backendSafe), maxConsumers)
	pressureState := centralQueueConsumerPressureStable

	if inputs.backendPressure {
		pressureBase := effective
		if p.previousEffectiveConsumersOK && p.previousEffectiveConsumers > 0 {
			pressureBase = p.previousEffectiveConsumers
		}
		pressureLimit := max(minConsumers, pressureBase/2)
		if pressureLimit < effective {
			effective = pressureLimit
			reason = centralQueueConsumerLimitReasonBackendPressure
			pressureState = centralQueueConsumerPressureReducing
		}
	}

	if !inputs.backendPressure && p.pressureRecoveryActive && p.previousEffectiveConsumersOK && p.previousEffectiveConsumers > 0 && effective > p.previousEffectiveConsumers {
		step := p.pressureRecoveryStep
		if step <= 0 {
			step = max(1, maxConsumers/10)
		}
		recoveryLimit := p.previousEffectiveConsumers + step
		if recoveryLimit < effective {
			effective = recoveryLimit
			reason = centralQueueConsumerLimitReasonRecovery
			pressureState = centralQueueConsumerPressureRecovering
		}
	}

	return centralQueueConsumerResult{
		effectiveConsumers:        effective,
		queueDemandConsumers:      queueDemand,
		backendSafeConsumersPerLB: backendSafe,
		limitReason:               reason,
		pressureState:             pressureState,
	}
}

func (p centralQueueConsumerPolicy) backendSafeConsumersPerLB(readyBackends int) int {
	if readyBackends <= 0 {
		return 0
	}
	inflightPerBackend := p.maxInflightSendsPerBackend
	if inflightPerBackend <= 0 {
		inflightPerBackend = defaultCentralQueueMaxInflightSendsPerBackend
	}
	lbReplicas := p.activeLoadBalancerReplicas
	if lbReplicas <= 0 {
		lbReplicas = 1
	}
	totalBackendCapacity := readyBackends * inflightPerBackend
	return totalBackendCapacity / lbReplicas
}

func ceilDivInt64ToInt(numerator, denominator int64) int {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	quotient := numerator / denominator
	if numerator%denominator != 0 {
		quotient++
	}
	if quotient > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(quotient)
}

type centralQueueConsumerController struct {
	mu     sync.Mutex
	policy centralQueueConsumerPolicy
	last   centralQueueConsumerResult
}

func newCentralQueueConsumerController(maxConsumers int, targetCompressedBytes int64, activeLoadBalancerReplicas int) *centralQueueConsumerController {
	return &centralQueueConsumerController{
		policy: centralQueueConsumerPolicy{
			maxConsumers:               configuredCentralQueueConsumers(maxConsumers),
			targetCompressedBytes:      targetCompressedBytes,
			activeLoadBalancerReplicas: configuredCentralQueueActiveLBReplicas(activeLoadBalancerReplicas),
		},
	}
}

func (c *centralQueueConsumerController) compute(queueCompressedBytes int64, readyBackends int, backendPressure bool) centralQueueConsumerResult {
	result, _ := c.computeWithChange(queueCompressedBytes, readyBackends, backendPressure)
	return result
}

func (c *centralQueueConsumerController) computeWithChange(queueCompressedBytes int64, readyBackends int, backendPressure bool) (centralQueueConsumerResult, bool) {
	if c == nil {
		return centralQueueConsumerPolicy{}.compute(centralQueueConsumerInputs{
			queueCompressedBytes: queueCompressedBytes,
			readyBackends:        readyBackends,
			backendPressure:      backendPressure,
		}), true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	previous := c.last
	previousOK := previous.limitReason != ""
	policy := c.policy
	if c.last.effectiveConsumers > 0 {
		policy.previousEffectiveConsumers = c.last.effectiveConsumers
		policy.previousEffectiveConsumersOK = true
	}
	policy.pressureRecoveryActive = c.last.pressureState == centralQueueConsumerPressureReducing ||
		c.last.pressureState == centralQueueConsumerPressureRecovering
	result := policy.compute(centralQueueConsumerInputs{
		queueCompressedBytes: queueCompressedBytes,
		readyBackends:        readyBackends,
		backendPressure:      backendPressure,
	})
	c.last = result
	return result, !previousOK || result != previous
}

func (c *centralQueueConsumerController) lastEffectiveConsumers() (int, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last.limitReason == "" {
		return 0, false
	}
	return c.last.effectiveConsumers, true
}

func (c *centralQueueConsumerController) backendSafeConsumersForLanes(readyBackends int) (int, bool) {
	if c == nil || readyBackends <= 0 {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	backendSafe := c.policy.backendSafeConsumersPerLB(readyBackends)
	if backendSafe <= 0 {
		return 0, true
	}
	return min(configuredCentralQueueConsumers(c.policy.maxConsumers), backendSafe), true
}

func configuredCentralQueueConsumers(consumers int) int {
	if consumers <= 0 {
		return defaultCentralQueueNumConsumers
	}
	return consumers
}

func configuredCentralQueueActiveLBReplicas(replicas int) int {
	if replicas <= 0 {
		return defaultCentralQueueActiveLBReplicas
	}
	return replicas
}

func centralQueueConsumerBackendCapacity(configuredConsumers, readyBackends int) int {
	if readyBackends <= 0 {
		return 0
	}
	return min(configuredCentralQueueConsumers(configuredConsumers), readyBackends)
}

func centralQueueEffectiveConsumersForLanes(controller *centralQueueConsumerController, configuredConsumers, readyBackends int) (int, bool) {
	if readyBackends <= 0 {
		return 0, false
	}
	if effectiveConsumers, ok := controller.lastEffectiveConsumers(); ok {
		if backendSafe, backendSafeKnown := controller.backendSafeConsumersForLanes(readyBackends); backendSafeKnown {
			return min(effectiveConsumers, backendSafe), true
		}
		return effectiveConsumers, true
	}
	if effectiveConsumers, ok := controller.backendSafeConsumersForLanes(readyBackends); ok {
		return effectiveConsumers, true
	}
	return centralQueueConsumerBackendCapacity(configuredConsumers, readyBackends), true
}
