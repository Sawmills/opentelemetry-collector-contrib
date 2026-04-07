// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapters // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
)

type ParquetAdapter interface {
	ConvertToParquet(context.Context, plog.Logs) ([]any, error)
	Schema() any
}
