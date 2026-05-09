// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
	expmetrics "github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics"
)

func newMetricCentralQueueRuntime(cfg CentralQueueConfig, telemetry *metadata.TelemetryBuilder) *centralQueueRuntime {
	if !cfg.Enabled {
		return nil
	}
	return newCentralQueueRuntime(cfg, centralQueueSignalMetrics, telemetry)
}

func (e *metricExporterImp) consumeMetricsCentralQueue(ctx context.Context, batches map[string]pmetric.Metrics) error {
	var errs error
	failed := pmetric.NewMetrics()
	for routingID, md := range batches {
		if err := e.enqueueMetricsCentralQueue(ctx, routingID, md); err != nil {
			errs = multierr.Append(errs, err)
			expmetrics.Merge(failed, md)
		}
	}
	if failed.DataPointCount() > 0 {
		return consumererror.NewMetrics(errs, failed)
	}
	return errs
}

func (e *metricExporterImp) enqueueMetricsCentralQueue(ctx context.Context, routingID string, md pmetric.Metrics) error {
	if md.DataPointCount() == 0 {
		return nil
	}
	marshaler := &pmetric.ProtoMarshaler{}
	payload, err := marshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}
	encoded, err := e.centralQueue.codec.Encode(payload)
	if err != nil {
		return err
	}

	item := centralQueueItem{
		key: centralQueueKey{
			signal:    centralQueueSignalMetrics,
			backendID: defaultCentralQueueBackendID,
			laneID:    centralQueueLaneID([]byte(routingID), e.centralQueue.queue.settings.batching.LaneCount),
		},
		payload:           encoded,
		compressedBytes:   len(encoded),
		uncompressedBytes: len(payload),
		itemCount:         md.DataPointCount(),
	}
	return e.centralQueue.enqueue(ctx, item)
}

func (e *metricExporterImp) consumeMetricCentralQueueWindow(ctx context.Context, window centralQueueWindow) error {
	md, err := e.decodeCentralQueueMetricWindow(window)
	if err != nil {
		return err
	}
	we, _, err := e.loadBalancer.randomExporterAndEndpoint()
	if err != nil {
		return err
	}
	return e.consumeCentralQueueMetrics(ctx, we, md)
}

func (e *metricExporterImp) decodeCentralQueueMetricWindow(window centralQueueWindow) (pmetric.Metrics, error) {
	unmarshaler := &pmetric.ProtoUnmarshaler{}
	chunks := make([]pmetric.Metrics, 0, len(window.items))
	for _, item := range window.items {
		payload, err := e.centralQueue.codec.Decode(item.payload)
		if err != nil {
			return pmetric.Metrics{}, err
		}
		md, err := unmarshaler.UnmarshalMetrics(payload)
		if err != nil {
			return pmetric.Metrics{}, err
		}
		chunks = append(chunks, md)
	}
	return mergePendingMetricChunks(chunks), nil
}

func (e *metricExporterImp) consumeCentralQueueMetrics(ctx context.Context, we *wrappedExporter, md pmetric.Metrics) error {
	if !we.tryStartConsume() {
		return errExporterIsStopping
	}
	defer we.doneConsume()

	recordBackendRequest(ctx, e.telemetry, we.endpoint, backendRequestSignalMetrics, md.DataPointCount(), (&pmetric.ProtoMarshaler{}).MetricsSize(md))
	start := time.Now()
	err := we.ConsumeMetrics(ctx, md)
	duration := time.Since(start)
	decision := e.recordBackendResultHealthOnly(ctx, we, duration, err, true)
	if err != nil && shouldRerouteDirectFailure(e.loadBalancer, we.endpoint, decision, 0) {
		e.loadBalancer.cleanupBackendWithoutDrain(ctx, we.endpoint)
	}
	return err
}
