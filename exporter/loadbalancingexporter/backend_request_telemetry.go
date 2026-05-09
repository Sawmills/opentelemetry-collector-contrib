// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	backendRequestSignalTraces  = "traces"
	backendRequestSignalLogs    = "logs"
	backendRequestSignalMetrics = "metrics"
)

func recordBackendRequest(ctx context.Context, telemetry *metadata.TelemetryBuilder, endpoint, signal string, items, bytes int) {
	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("endpoint", endpoint), attribute.String("signal", signal)))
	telemetry.LoadbalancerBackendRequestTotal.Add(ctx, 1, attrs)
	telemetry.LoadbalancerBackendRequestItems.Record(ctx, int64(items), attrs)
	telemetry.LoadbalancerBackendRequestBytes.Record(ctx, int64(bytes), attrs)
}
