// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"math"
	"sync"
	"time"
)

const (
	defaultCentralQueueMinLanes              = 1
	defaultCentralQueueMaxLanes              = 256
	defaultCentralQueueBackendLaneMultiplier = 2
	defaultCentralQueueLaneHysteresisFactor  = 2
	centralQueueLaneRateWindow               = 30 * time.Second
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
	resolvedBackends              int
	effectiveConsumers            int
	effectiveConsumersKnown       bool
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
		backendMultiplier = defaultCentralQueueBackendLaneMultiplier
	}
	targetFillDuration := cfg.TargetLaneFillDuration
	if targetFillDuration <= 0 {
		targetFillDuration = cfg.MaxBatchDelay / 2
		if targetFillDuration <= 0 {
			targetFillDuration = defaultCentralQueueMaxBatchDelay / 2
		}
		if targetFillDuration > 500*time.Millisecond {
			targetFillDuration = 500 * time.Millisecond
		}
	}
	targetBytes := cfg.TargetCompressedBytes
	if targetBytes <= 0 {
		targetBytes = defaultCentralQueueTargetCompressedBytes
	}
	hysteresisFactor := cfg.LaneHysteresisFactor
	if hysteresisFactor <= 0 {
		hysteresisFactor = defaultCentralQueueLaneHysteresisFactor
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
	minLanes := max(p.minLanes, 1)
	maxLanes := max(p.maxLanes, minLanes)

	backends := inputs.healthyBackends
	if backends <= 0 && inputs.effectiveConsumersKnown {
		backends = inputs.effectiveConsumers
	}
	if backends <= 0 {
		backends = inputs.resolvedBackends
	}
	if backends <= 0 {
		backends = minLanes
	}

	backendFloor := clampInt(backends, minLanes, maxLanes)
	candidate := backendFloor
	if inputs.compressedIngestBytesPerSec > 0 && p.targetFillDuration > 0 && p.targetBytes > 0 {
		backendLanes := backends * max(p.backendMultiplier, 1)
		fillLanes := int(math.Ceil(float64(inputs.compressedIngestBytesPerSec) * p.targetFillDuration.Seconds() / float64(p.targetBytes)))
		candidate = max(backendFloor, min(backendLanes, fillLanes, maxLanes))
		candidate = clampInt(candidate, minLanes, maxLanes)
	}

	if inputs.previousEffectiveLaneCountSet {
		previous := clampInt(inputs.previousEffectiveLaneCount, minLanes, maxLanes)
		if previous > 0 && p.hysteresisFactor > 1 {
			factor := p.hysteresisFactor
			if candidate > previous/factor && candidate < previous*factor {
				return previous
			}
		}
	}
	return candidate
}

func clampInt(value, lower, upper int) int {
	if value < lower {
		return lower
	}
	if value > upper {
		return upper
	}
	return value
}

type centralQueueLaneController struct {
	policy         centralQueueLanePolicy
	fixedLaneCount int
	rateWindow     time.Duration

	mu                 sync.Mutex
	effectiveLaneCount int
	effectiveSet       bool
	lastBackendCount   int
	lastConsumerCount  int
	lastConsumerKnown  bool
	bytesPerSec        int64
	windowStart        time.Time
	windowBytes        int64
}

func newCentralQueueLaneController(cfg CentralQueueConfig) *centralQueueLaneController {
	return &centralQueueLaneController{
		policy:         newCentralQueueLanePolicy(cfg),
		fixedLaneCount: cfg.LaneCount,
		rateWindow:     centralQueueLaneRateWindow,
	}
}

func (c *centralQueueLaneController) laneCount(backendCount int, now time.Time) int {
	return c.laneCountWithConsumers(backendCount, 0, false, now)
}

func (c *centralQueueLaneController) laneCountWithConsumers(backendCount, consumerCount int, consumerKnown bool, now time.Time) int {
	if c == nil {
		return defaultCentralQueueMaxLanes
	}
	if c.fixedLaneCount > 0 {
		return c.fixedLaneCount
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rateRolled := c.rollRateWindowLocked(now)
	if rateRolled || !c.effectiveSet || backendCount != c.lastBackendCount || consumerCount != c.lastConsumerCount || consumerKnown != c.lastConsumerKnown {
		c.recomputeLocked(backendCount, consumerCount, consumerKnown)
	}
	return c.effectiveLaneCount
}

func (c *centralQueueLaneController) observeCompressedBytes(compressedBytes int, now time.Time) int {
	if c == nil {
		return defaultCentralQueueMaxLanes
	}
	if c.fixedLaneCount > 0 {
		return c.fixedLaneCount
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rateRolled := c.rollRateWindowLocked(now)
	if c.windowStart.IsZero() {
		c.windowStart = now
	}
	if compressedBytes > 0 {
		c.windowBytes += int64(compressedBytes)
	}
	if rateRolled || !c.effectiveSet {
		c.recomputeLocked(c.lastBackendCount, c.lastConsumerCount, c.lastConsumerKnown)
	}
	return c.effectiveLaneCount
}

func (c *centralQueueLaneController) rollRateWindowLocked(now time.Time) bool {
	if c.rateWindow <= 0 {
		c.rateWindow = centralQueueLaneRateWindow
	}
	if c.windowStart.IsZero() {
		return false
	}
	elapsed := now.Sub(c.windowStart)
	if elapsed < c.rateWindow {
		return false
	}
	if elapsed > 0 {
		c.bytesPerSec = int64(float64(c.windowBytes) / elapsed.Seconds())
	}
	c.windowStart = now
	c.windowBytes = 0
	return true
}

func (c *centralQueueLaneController) recomputeLocked(backendCount, consumerCount int, consumerKnown bool) {
	c.effectiveLaneCount = c.policy.compute(centralQueueLaneInputs{
		healthyBackends:               backendCount,
		effectiveConsumers:            consumerCount,
		effectiveConsumersKnown:       consumerKnown,
		compressedIngestBytesPerSec:   c.bytesPerSec,
		previousEffectiveLaneCount:    c.effectiveLaneCount,
		previousEffectiveLaneCountSet: c.effectiveSet,
	})
	c.effectiveSet = true
	c.lastBackendCount = backendCount
	c.lastConsumerCount = consumerCount
	c.lastConsumerKnown = consumerKnown
}

func centralQueueEffectiveLaneCount(controller *centralQueueLaneController, staticLaneCount int, lb *loadBalancer, consumerCount int, consumerKnown bool, now time.Time) int {
	if controller != nil {
		return controller.laneCountWithConsumers(centralQueueRoutableBackendCount(lb), consumerCount, consumerKnown, now)
	}
	if staticLaneCount > 0 {
		return staticLaneCount
	}
	return 0
}

func centralQueueStableLaneCount(controller *centralQueueLaneController, staticLaneCount int) int {
	if staticLaneCount > 0 {
		return staticLaneCount
	}
	if controller != nil {
		return controller.stableLaneCount()
	}
	return 0
}

func (c *centralQueueLaneController) stableLaneCount() int {
	if c == nil {
		return 0
	}
	if c.fixedLaneCount > 0 {
		return c.fixedLaneCount
	}
	return max(c.policy.maxLanes, c.policy.minLanes)
}

func observeCentralQueueLaneBytes(telemetry *centralQueueTelemetry, controller *centralQueueLaneController, staticLaneCount, compressedBytes int, now time.Time) {
	lanes := 0
	if controller != nil {
		lanes = controller.observeCompressedBytes(compressedBytes, now)
	} else if staticLaneCount > 0 {
		lanes = staticLaneCount
	}
	telemetry.recordEffectiveLanes(context.Background(), int64(lanes))
}

func centralQueueRoutableBackendCount(lb *loadBalancer) int {
	if lb == nil {
		return 0
	}
	return lb.routableBackendCount()
}
