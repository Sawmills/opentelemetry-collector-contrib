// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"math"
	"sync"
	"time"
)

const (
	defaultCentralQueueMinLanes              = 1
	defaultCentralQueueMaxLanes              = 64
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
	maxLanes := p.maxLanes
	if maxLanes < minLanes {
		maxLanes = minLanes
	}

	backends := inputs.healthyBackends
	if backends <= 0 {
		backends = inputs.resolvedBackends
	}
	if backends <= 0 {
		backends = minLanes
	}

	candidate := clampInt(backends, minLanes, maxLanes)
	if inputs.compressedIngestBytesPerSec > 0 && p.targetFillDuration > 0 && p.targetBytes > 0 {
		backendLanes := backends * max(p.backendMultiplier, 1)
		fillLanes := int(math.Ceil(float64(inputs.compressedIngestBytesPerSec) * p.targetFillDuration.Seconds() / float64(p.targetBytes)))
		candidate = min(backendLanes, fillLanes, maxLanes)
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
	if c == nil {
		return defaultCentralQueueMaxLanes
	}
	if c.fixedLaneCount > 0 {
		return c.fixedLaneCount
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollRateWindowLocked(now)
	if !c.effectiveSet || backendCount != c.lastBackendCount {
		c.recomputeLocked(backendCount)
	}
	return c.effectiveLaneCount
}

func (c *centralQueueLaneController) observeCompressedBytes(compressedBytes int, backendCount int, now time.Time) int {
	if c == nil {
		return defaultCentralQueueMaxLanes
	}
	if c.fixedLaneCount > 0 {
		return c.fixedLaneCount
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rollRateWindowLocked(now)
	if c.windowStart.IsZero() {
		c.windowStart = now
	}
	if compressedBytes > 0 {
		c.windowBytes += int64(compressedBytes)
	}
	if !c.effectiveSet || backendCount != c.lastBackendCount {
		c.recomputeLocked(backendCount)
	}
	return c.effectiveLaneCount
}

func (c *centralQueueLaneController) rollRateWindowLocked(now time.Time) {
	if c.rateWindow <= 0 {
		c.rateWindow = centralQueueLaneRateWindow
	}
	if c.windowStart.IsZero() {
		return
	}
	elapsed := now.Sub(c.windowStart)
	if elapsed < c.rateWindow {
		return
	}
	if elapsed > 0 {
		c.bytesPerSec = int64(float64(c.windowBytes) / elapsed.Seconds())
	}
	c.windowStart = now
	c.windowBytes = 0
	c.recomputeLocked(c.lastBackendCount)
}

func (c *centralQueueLaneController) recomputeLocked(backendCount int) {
	c.effectiveLaneCount = c.policy.compute(centralQueueLaneInputs{
		healthyBackends:               backendCount,
		compressedIngestBytesPerSec:   c.bytesPerSec,
		previousEffectiveLaneCount:    c.effectiveLaneCount,
		previousEffectiveLaneCountSet: c.effectiveSet,
	})
	c.effectiveSet = true
	c.lastBackendCount = backendCount
}
