// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension"

import (
	"go.opentelemetry.io/collector/extension"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/internal/metadata"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		metadata.Type,
		CreateDefaultConfig,
		NewParquetLogExtension,
		metadata.ExtensionStability,
	)
}
