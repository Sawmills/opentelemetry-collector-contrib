// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const centralQueueSendRetryBackoff = 25 * time.Millisecond

type centralQueueConsumeFunc func(context.Context, centralQueueWindow) error

type centralQueueRuntime struct {
	queue        *centralQueue
	codec        *queuePayloadCodec
	numConsumers int
	signal       centralQueueSignal
	telemetry    *metadata.TelemetryBuilder

	signalAttrs         attribute.Set
	enqueueSuccessAttrs attribute.Set
	enqueueFailureAttrs attribute.Set
	flushReasonAttrs    map[centralQueueFlushReason]attribute.Set

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stopping atomic.Bool
	errMu    sync.Mutex
	err      error
}

func newCentralQueueRuntime(cfg CentralQueueConfig, signal centralQueueSignal, telemetry *metadata.TelemetryBuilder) *centralQueueRuntime {
	cfg.ApplyDefaults()
	signalAttr := attribute.String("signal", string(signal))
	return &centralQueueRuntime{
		queue:               newCentralQueue(centralQueueSettingsFromConfig(cfg)),
		codec:               newQueuePayloadCodec(cfg.PayloadCompression),
		numConsumers:        cfg.NumConsumers,
		signal:              signal,
		telemetry:           telemetry,
		signalAttrs:         attribute.NewSet(signalAttr),
		enqueueSuccessAttrs: attribute.NewSet(signalAttr, attribute.String("result", "success")),
		enqueueFailureAttrs: attribute.NewSet(signalAttr, attribute.String("result", "failure")),
		flushReasonAttrs: map[centralQueueFlushReason]attribute.Set{
			centralQueueFlushReasonTargetReached:      attribute.NewSet(signalAttr, attribute.String("reason", string(centralQueueFlushReasonTargetReached))),
			centralQueueFlushReasonHardCap:            attribute.NewSet(signalAttr, attribute.String("reason", string(centralQueueFlushReasonHardCap))),
			centralQueueFlushReasonMaxDelayLowTraffic: attribute.NewSet(signalAttr, attribute.String("reason", string(centralQueueFlushReasonMaxDelayLowTraffic))),
			centralQueueFlushReasonShutdown:           attribute.NewSet(signalAttr, attribute.String("reason", string(centralQueueFlushReasonShutdown))),
		},
	}
}

func (r *centralQueueRuntime) start(consume centralQueueConsumeFunc, logger *zap.Logger) {
	if r == nil {
		return
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.recordState(r.ctx)
	for i := 0; i < r.numConsumers; i++ {
		r.wg.Add(1)
		go r.runConsumer(consume, logger)
	}
}

func (r *centralQueueRuntime) runConsumer(consume centralQueueConsumeFunc, logger *zap.Logger) {
	defer r.wg.Done()
	for {
		window, ok := r.queue.nextWindow(r.ctx)
		if !ok {
			return
		}
		r.recordWindow(r.ctx, window)
		if err := consume(r.ctx, window); err != nil {
			if r.stopping.Load() {
				r.recordError(err)
				return
			}
			r.queue.requeueFront(window)
			r.recordState(r.ctx)
			if logger != nil {
				logger.Debug("failed to send central queue window", zap.Error(err))
			}
			if !centralQueueSleep(r.ctx, centralQueueSendRetryBackoff) {
				return
			}
		}
	}
}

func (r *centralQueueRuntime) shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.stopping.Store(true)
	r.queue.stop()
	if err := waitForInflight(ctx, &r.wg); err != nil {
		if r.cancel != nil {
			r.cancel()
		}
		return err
	}
	if r.cancel != nil {
		r.cancel()
	}
	return errors.Join(r.err, r.codec.Close())
}

func (r *centralQueueRuntime) enqueue(ctx context.Context, item centralQueueItem) error {
	err := r.queue.enqueue(ctx, item)
	if err != nil {
		if r.telemetry != nil {
			r.telemetry.LoadbalancerCentralQueueEnqueueTotal.Add(ctx, 1, metric.WithAttributeSet(r.enqueueFailureAttrs))
		}
		r.recordState(ctx)
		return err
	}
	if r.telemetry != nil {
		r.telemetry.LoadbalancerCentralQueueEnqueueTotal.Add(ctx, 1, metric.WithAttributeSet(r.enqueueSuccessAttrs))
	}
	r.recordState(ctx)
	return nil
}

func (r *centralQueueRuntime) recordWindow(ctx context.Context, window centralQueueWindow) {
	if r.telemetry == nil {
		return
	}
	attrs := metric.WithAttributeSet(r.signalAttrs)
	r.telemetry.LoadbalancerCentralQueueWindowCompressedBytes.Record(ctx, int64(window.compressedBytes), attrs)
	r.telemetry.LoadbalancerCentralQueueWindowUncompressedBytes.Record(ctx, int64(window.uncompressedBytes), attrs)
	r.telemetry.LoadbalancerCentralQueueWindowItems.Record(ctx, int64(window.itemCount), attrs)
	r.telemetry.LoadbalancerCentralQueueWindowPayloads.Record(ctx, int64(len(window.items)), attrs)
	reasonAttrs := metric.WithAttributeSet(r.flushAttrs(window.flushReason))
	r.telemetry.LoadbalancerCentralQueueWindowFlushTotal.Add(ctx, 1, reasonAttrs)
	if window.compressedBytes < int(r.queue.settings.batching.TargetCompressedBytes) {
		r.telemetry.LoadbalancerCentralQueueWindowUnderfilledTotal.Add(ctx, 1, reasonAttrs)
	}
	r.recordState(ctx)
}

func (r *centralQueueRuntime) flushAttrs(reason centralQueueFlushReason) attribute.Set {
	if attrs, ok := r.flushReasonAttrs[reason]; ok {
		return attrs
	}
	return attribute.NewSet(attribute.String("signal", string(r.signal)), attribute.String("reason", string(reason)))
}

func (r *centralQueueRuntime) recordState(ctx context.Context) {
	if r.telemetry == nil {
		return
	}
	attrs := metric.WithAttributeSet(r.signalAttrs)
	now := r.queue.settings.now()
	r.telemetry.LoadbalancerCentralQueueCapacityBytes.Record(ctx, r.queue.settings.capacityBytes, attrs)
	r.telemetry.LoadbalancerCentralQueueSizeBytes.Record(ctx, r.queue.currentBytes(), attrs)
	r.telemetry.LoadbalancerCentralQueueActiveLanes.Record(ctx, int64(r.queue.activeLaneCount(r.signal)), attrs)
	r.telemetry.LoadbalancerCentralQueueOldestItemAge.Record(ctx, r.queue.oldestItemAge(r.signal, now).Milliseconds(), attrs)
}

func (r *centralQueueRuntime) recordError(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.err = errors.Join(r.err, err)
}

func centralQueueSleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
