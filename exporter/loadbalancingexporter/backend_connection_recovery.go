// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"

	"go.uber.org/zap"
)

// Observe results only after releasing the caller's in-flight reference. The
// cleanup lifecycle may synchronously retire a child during shutdown.
func (lb *loadBalancer) recordBackendConnectionResult(ctx context.Context, failed *wrappedExporter, err error) {
	if lb.reconnectAfterTimeouts <= 0 || ctx.Err() != nil {
		return
	}
	lb.updateLock.Lock()
	if lb.exporters[failed.endpoint] != failed || failed.isStopping() {
		lb.updateLock.Unlock()
		return
	}
	if !isBackendTimeout(err) {
		failed.consecutiveTimeouts = 0
		lb.updateLock.Unlock()
		return
	}
	failed.consecutiveTimeouts = min(failed.consecutiveTimeouts+1, lb.reconnectAfterTimeouts)
	if failed.consecutiveTimeouts < lb.reconnectAfterTimeouts || failed.rebuilding {
		lb.updateLock.Unlock()
		return
	}
	failed.rebuilding = true
	lb.updateLock.Unlock()

	done, admitted := lb.beginCleanup()
	if !admitted {
		lb.updateLock.Lock()
		failed.rebuilding = false
		lb.updateLock.Unlock()
		return
	}
	go func() {
		defer done()
		defer func() {
			lb.updateLock.Lock()
			failed.rebuilding = false
			lb.updateLock.Unlock()
		}()
		lb.rebuildTimedOutBackend(ctx, failed)
	}()
}

func (lb *loadBalancer) timeoutGenerationCurrent(failed *wrappedExporter) bool {
	lb.updateLock.RLock()
	defer lb.updateLock.RUnlock()
	lb.cleanupMu.Lock()
	defer lb.cleanupMu.Unlock()
	return !lb.stopped && lb.exporters[failed.endpoint] == failed && !failed.isStopping() &&
		failed.consecutiveTimeouts >= lb.reconnectAfterTimeouts
}

func (lb *loadBalancer) rebuildTimedOutBackend(ctx context.Context, failed *wrappedExporter) {
	if !lb.timeoutGenerationCurrent(failed) {
		lb.recoveryTelemetry.recordConnectionRebuild(ctx, "superseded")
		return
	}
	child, err := lb.componentFactory(ctx, failed.endpoint)
	if err != nil {
		if ctx.Err() != nil {
			lb.recoveryTelemetry.recordConnectionRebuild(ctx, "superseded")
			return
		}
		lb.recoveryTelemetry.recordConnectionRebuild(ctx, "create_failed")
		lb.logger.Warn("failed to rebuild backend connection", zap.String("endpoint", failed.endpoint), zap.String("stage", "create"), zap.Error(err))
		return
	}
	if !lb.timeoutGenerationCurrent(failed) {
		lb.shutdownSkippedExporter(ctx, failed.endpoint, child)
		lb.recoveryTelemetry.recordConnectionRebuild(ctx, "superseded")
		return
	}
	replacement := newWrappedExporter(child, failed.endpoint)
	if err = replacement.Start(ctx, lb.host); err != nil {
		lb.shutdownSkippedExporter(ctx, failed.endpoint, child)
		if ctx.Err() != nil {
			lb.recoveryTelemetry.recordConnectionRebuild(ctx, "superseded")
			return
		}
		lb.recoveryTelemetry.recordConnectionRebuild(ctx, "start_failed")
		lb.logger.Warn("failed to rebuild backend connection", zap.String("endpoint", failed.endpoint), zap.String("stage", "start"), zap.Error(err))
		return
	}
	installed := false
	lb.updateLock.Lock()
	lb.cleanupMu.Lock()
	if !lb.stopped && lb.exporters[failed.endpoint] == failed && !failed.isStopping() &&
		failed.consecutiveTimeouts >= lb.reconnectAfterTimeouts {
		failed.markStopping()
		lb.exporters[failed.endpoint] = replacement
		installed = true
	}
	lb.cleanupMu.Unlock()
	lb.updateLock.Unlock()
	if !installed {
		lb.shutdownSkippedExporter(ctx, failed.endpoint, replacement)
		lb.recoveryTelemetry.recordConnectionRebuild(ctx, "superseded")
		return
	}
	lb.recoveryTelemetry.recordConnectionRebuild(ctx, "success")
	lb.logger.Info("rebuilt backend connection after consecutive timeouts",
		zap.String("endpoint", failed.endpoint), zap.Int("consecutive_timeouts", lb.reconnectAfterTimeouts))
	// Queue ownership and ring membership never move. The retired generation
	// only owns its remaining in-flight calls, which its normal shutdown awaits.
	lb.drainRemovedExporters(ctx, []removedExporter{{endpoint: failed.endpoint, exporter: failed}})
}
