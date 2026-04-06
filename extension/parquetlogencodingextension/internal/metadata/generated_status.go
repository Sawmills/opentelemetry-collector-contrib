// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import "go.opentelemetry.io/collector/component"

var (
	Type      = component.MustNewType("parquet_log_encoding")
	ScopeName = "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension"
)

const ExtensionStability = component.StabilityLevelAlpha
