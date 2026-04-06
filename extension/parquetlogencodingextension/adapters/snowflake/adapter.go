// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflake

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
)

// ParquetLog is a compile-time placeholder for Task 2 routing.
// Task 3 replaces this with the real Snowflake row contract.
type ParquetLog struct {
	TS string `parquet:"name=ts, type=BYTE_ARRAY, logicaltype=STRING"`
}

type snowflakeParquetAdapter struct{}

func NewSnowflakeParquetAdapter(_ extension.Settings) (adapters.ParquetAdapter, error) {
	return &snowflakeParquetAdapter{}, nil
}

func (a *snowflakeParquetAdapter) ConvertToParquet(_ context.Context, _ plog.Logs) ([]any, error) {
	return nil, fmt.Errorf("snowflake parquet adapter is not implemented yet")
}

func (a *snowflakeParquetAdapter) Schema() any {
	return &ParquetLog{}
}
