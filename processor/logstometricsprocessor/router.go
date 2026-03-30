// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor"

import (
	"context"

	"github.com/observiq/bindplane-agent/receiver/routereceiver"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func routeMetrics(ctx context.Context, route string, metrics pmetric.Metrics) error {
	return routereceiver.RouteMetrics(ctx, route, metrics)
}
