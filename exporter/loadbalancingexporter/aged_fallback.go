// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

func (e *logExporterImp) startFallback(host component.Host) error {
	if e.fallbackExporterID == nil {
		return nil
	}
	if *e.fallbackExporterID == e.centralExporterID {
		return errors.New("central_queue.fallback_exporter must not reference this exporter")
	}
	// This is the host's existing exporter capability. Keep the service module
	// out of the exporter dependency graph; the service owns target lifecycle.
	provider, ok := host.(interface {
		GetExporters() map[pipeline.Signal]map[component.ID]component.Component
	})
	if !ok {
		return errors.New("host does not expose central_queue.fallback_exporter")
	}
	target := provider.GetExporters()[pipeline.SignalLogs][*e.fallbackExporterID]
	var valid bool
	e.fallback, valid = target.(consumer.Logs)
	if !valid || e.fallback == nil {
		return fmt.Errorf("central_queue.fallback_exporter %s is not an available logs exporter", e.fallbackExporterID)
	}
	return nil
}

func (e *logExporterImp) hasAgedLogs(window centralQueueWindow, now time.Time) bool {
	return e.maxRetryAge > 0 && window.oldestEnqueuedAt > 0 &&
		now.Sub(time.Unix(0, window.oldestEnqueuedAt)) >= e.maxRetryAge
}

func (e *logExporterImp) agedLogCount(window centralQueueWindow, now time.Time) int {
	var count int
	for i := range window.items {
		if now.Sub(time.Unix(0, window.items[i].enqueuedAtUnixNano)) >= e.maxRetryAge {
			count += window.items[i].count
		}
	}
	return count
}

func (e *logExporterImp) acquireFallbackConsumer(ctx context.Context) bool {
	for {
		active := e.centralActiveConsumers.Load()
		if active >= int64(e.centralQueueNumConsumers) {
			return false
		}
		if e.centralActiveConsumers.CompareAndSwap(active, active+1) {
			e.centralQueue.settings.telemetry.recordActiveConsumers(ctx, active+1)
			return true
		}
	}
}

// A mixed window has two independent owners. Accepting its aged group must
// never cause fresh records to divert, or let their retry replay that group.
func (e *logExporterImp) splitAgedLease(original *centralQueueLease, now time.Time) (aged, fresh *centralQueueLease) {
	aged = &centralQueueLease{queue: original.queue}
	fresh = &centralQueueLease{queue: original.queue, consumerRelease: original.consumerRelease}
	for i := range original.window.items {
		item := original.window.items[i]
		target := fresh
		if now.Sub(time.Unix(0, item.enqueuedAtUnixNano)) >= e.maxRetryAge {
			target = aged
		}
		target.window.routingKey = original.window.routingKey
		target.window.flushReason = original.window.flushReason
		target.window.items = append(target.window.items, item)
		appendCentralQueueWindowItemStats(&target.window, &item)
	}
	if len(aged.window.items) == 0 {
		return nil, original
	}
	if len(fresh.window.items) == 0 {
		return original, nil
	}
	return aged, fresh
}

func (e *logExporterImp) sendFallback(ctx context.Context, lease *centralQueueLease) {
	logs := plog.NewLogs()
	var err error
	for i := range lease.window.items {
		var decoded plog.Logs
		decoded, err = decodeCentralQueueLogsItem(lease.window.items[i], e.centralCodec)
		if err != nil {
			break
		}
		mergeLogChunksByMove(logs, decoded)
	}
	if err == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = e.fallback.ConsumeLogs(attemptCtx, logs)
		cancel()
	}
	reason := "max_retry_age"
	if err == nil && e.centralStorage != nil {
		// A fallback consumer acknowledges ownership directly. It cannot use
		// the worker-only OTLP middleware receipt or permanent-error drop path.
		err = e.centralStorage.acknowledge(ctx, lease.window.items, 0, "fallback")
		if err != nil {
			reason = "fallback_commit_failed"
		}
	}
	if err == nil {
		e.recordFallback(ctx, lease.window.count, reason)
		lease.done()
		return
	}
	if reason != "fallback_commit_failed" {
		reason = "fallback_failed"
	}
	e.recordFallback(ctx, lease.window.count, reason)
	// Every refusal, including a permanent or partial error, keeps ownership.
	if ctx.Err() != nil {
		lease.discardOnShutdown()
	} else if requeueErr := lease.requeue(time.Now()); requeueErr != nil {
		e.logger.Warn("failed to retain central queue fallback batch", zap.Error(requeueErr))
	}
}

func (e *logExporterImp) recordFallback(ctx context.Context, records int, reason string) {
	e.centralQueue.settings.telemetry.fallbackRecords.Add(ctx, int64(records),
		e.centralQueue.settings.telemetry.signalAttrs, metric.WithAttributes(attribute.String("reason", reason)))
	e.logger.Info("central queue fallback batch", zap.String("reason", reason), zap.Int("records", records))
}
