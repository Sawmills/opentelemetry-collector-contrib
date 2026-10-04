// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	backendRequestSignalLogs    = "logs"
	backendRequestSignalMetrics = "metrics"
	backendRequestSignalTraces  = "traces"
)

func backendRequestAttributeSet(signal, endpoint string) attribute.Set {
	return attribute.NewSet(attribute.String("endpoint", endpoint), attribute.String("signal", signal))
}

func backendRequestSignalAttributeSet(signal string) attribute.Set {
	return attribute.NewSet(attribute.String("signal", signal))
}

func backendRequestMetricOptions(attrs attribute.Set) metric.MeasurementOption {
	return metric.WithAttributeSet(attrs)
}

func recordLogBackendRequest(ctx context.Context, tb *metadata.TelemetryBuilder, signalAttrs, endpointAttrs attribute.Set, ld plog.Logs) {
	if tb == nil {
		return
	}

	signalOpts := backendRequestMetricOptions(signalAttrs)
	bytes := serializedLogsSize(ld)
	items := int64(ld.LogRecordCount())
	endpointOpts := backendRequestMetricOptions(endpointAttrs)
	tb.LoadbalancerBackendRequestBytes.Record(ctx, bytes, signalOpts)
	tb.LoadbalancerBackendRequestItems.Record(ctx, items, signalOpts)
	tb.LoadbalancerBackendRequestBytesTotal.Add(ctx, bytes, endpointOpts)
	tb.LoadbalancerBackendRequestItemsTotal.Add(ctx, items, endpointOpts)
	tb.LoadbalancerBackendRequestTotal.Add(ctx, 1, endpointOpts)
}

func recordMetricBackendRequest(ctx context.Context, tb *metadata.TelemetryBuilder, signalAttrs, endpointAttrs attribute.Set, md pmetric.Metrics) {
	if tb == nil {
		return
	}

	signalOpts := backendRequestMetricOptions(signalAttrs)
	bytes := serializedMetricsSize(md)
	items := int64(md.DataPointCount())
	endpointOpts := backendRequestMetricOptions(endpointAttrs)
	tb.LoadbalancerBackendRequestBytes.Record(ctx, bytes, signalOpts)
	tb.LoadbalancerBackendRequestItems.Record(ctx, items, signalOpts)
	tb.LoadbalancerBackendRequestBytesTotal.Add(ctx, bytes, endpointOpts)
	tb.LoadbalancerBackendRequestItemsTotal.Add(ctx, items, endpointOpts)
	tb.LoadbalancerBackendRequestTotal.Add(ctx, 1, endpointOpts)
}

func recordBackendTimeout(ctx context.Context, tb *metadata.TelemetryBuilder, endpointAttrs attribute.Set, err error) {
	if tb == nil {
		return
	}
	if !isBackendTimeout(err) {
		return
	}
	tb.LoadbalancerBackendTimeoutTotal.Add(ctx, 1, backendRequestMetricOptions(endpointAttrs))
}

const backendOutcomeSuccessReason endpointFailureReason = "success"
const backendOutcomeOtherReason endpointFailureReason = "other"

// recordBackendFailedOutcome records one backend attempt. Successful non-empty
// attempts add zero to a stable success series so active clean backends remain
// queryable after quiet-state filtering. Failure reasons are classified into
// the bounded endpoint health vocabulary and never include error text.
func recordBackendFailedOutcome(ctx context.Context, tb *metadata.TelemetryBuilder, signalAttrs attribute.Set, err error) {
	if tb == nil {
		return
	}
	reason := backendOutcomeSuccessReason
	value := int64(0)
	if err != nil {
		var classified bool
		reason, classified = classifyEndpointFailure(err)
		if !classified {
			reason = backendOutcomeOtherReason
		}
		value = 1
	}
	attrs := append(signalAttrs.ToSlice(), attribute.String("reason", string(reason)))
	tb.LoadbalancerBackendFailed.Add(ctx, value, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

func serializedLogsSize(ld plog.Logs) int64 {
	marshaler := plog.ProtoMarshaler{}
	return int64(marshaler.LogsSize(ld))
}

func serializedMetricsSize(md pmetric.Metrics) int64 {
	marshaler := pmetric.ProtoMarshaler{}
	return int64(marshaler.MetricsSize(md))
}
