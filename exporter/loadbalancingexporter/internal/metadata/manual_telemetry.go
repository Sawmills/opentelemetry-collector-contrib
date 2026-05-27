// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import "go.opentelemetry.io/otel/metric"

func NewLoadbalancerResolverStaleAgeGauge(builder *TelemetryBuilder) (metric.Int64Gauge, error) {
	return builder.meter.Int64Gauge(
		"otelcol_loadbalancer_resolver_stale_age",
		metric.WithDescription("Age in milliseconds of the last successful resolver result currently kept after a resolver failure. [Development]"),
		metric.WithUnit("ms"),
	)
}
