// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const logFlushReasonCentralQueue = "central_queue"

func newLogCentralQueueRuntime(cfg CentralQueueConfig, ignoreTraceID bool, telemetry *metadata.TelemetryBuilder) *centralQueueRuntime {
	if !cfg.Enabled || !ignoreTraceID {
		return nil
	}
	return newCentralQueueRuntime(cfg, centralQueueSignalLogs, telemetry)
}

func (e *logExporterImp) consumeLogsCentralQueue(ctx context.Context, ld plog.Logs) error {
	if ld.LogRecordCount() == 0 {
		return nil
	}
	marshaler := &plog.ProtoMarshaler{}
	payload, err := marshaler.MarshalLogs(ld)
	if err != nil {
		return err
	}
	encoded, err := e.centralQueue.codec.Encode(payload)
	if err != nil {
		return err
	}

	item := centralQueueItem{
		key: centralQueueKey{
			signal:    centralQueueSignalLogs,
			backendID: defaultCentralQueueBackendID,
		},
		payload:           encoded,
		compressedBytes:   len(encoded),
		uncompressedBytes: len(payload),
		itemCount:         ld.LogRecordCount(),
	}
	return e.centralQueue.enqueue(ctx, item)
}

func (e *logExporterImp) consumeLogCentralQueueWindow(ctx context.Context, window centralQueueWindow) error {
	ld, err := e.decodeCentralQueueLogWindow(window)
	if err != nil {
		return err
	}
	le, _, err := e.loadBalancer.randomExporterAndEndpoint()
	if err != nil {
		return err
	}
	_, err = e.consumeBatchWithDecision(ctx, le, ld, logFlushReasonCentralQueue, true, false, true)
	return err
}

func (e *logExporterImp) decodeCentralQueueLogWindow(window centralQueueWindow) (plog.Logs, error) {
	unmarshaler := &plog.ProtoUnmarshaler{}
	var merged plog.Logs
	for i, item := range window.items {
		payload, err := e.centralQueue.codec.Decode(item.payload)
		if err != nil {
			return plog.Logs{}, err
		}
		logs, err := unmarshaler.UnmarshalLogs(payload)
		if err != nil {
			return plog.Logs{}, err
		}
		if i == 0 {
			merged = logs
			continue
		}
		mergeLogChunksByMove(merged, logs)
	}
	if len(window.items) == 0 {
		return plog.NewLogs(), nil
	}
	return merged, nil
}
