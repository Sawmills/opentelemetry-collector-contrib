// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension

import (
	"errors"
	"strings"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultMaxFileSizeBytes   = 100 * 1024 * 1024
	defaultNumberOfGoRoutines = 4
	defaultRowGroupSizeBytes  = defaultMaxFileSizeBytes
	defaultPageSizeBytes      = 1 * 1024 * 1024
	defaultCompressionCodec   = "snappy"
	defaultSchema             = "datadog"
)

type Config struct {
	MaxFileSizeBytes   int64  `mapstructure:"max_file_size_bytes"`
	NumberOfGoRoutines int64  `mapstructure:"number_of_go_routines"`
	RowGroupSizeBytes  int64  `mapstructure:"row_group_size_bytes"`
	PageSizeBytes      int64  `mapstructure:"page_size_bytes"`
	CompressionCodec   string `mapstructure:"compression_codec"`
	Schema             string `mapstructure:"schema"`
}

func CreateDefaultConfig() component.Config {
	return &Config{
		MaxFileSizeBytes:   defaultMaxFileSizeBytes,
		NumberOfGoRoutines: defaultNumberOfGoRoutines,
		RowGroupSizeBytes:  defaultRowGroupSizeBytes,
		PageSizeBytes:      defaultPageSizeBytes,
		CompressionCodec:   defaultCompressionCodec,
		Schema:             defaultSchema,
	}
}

func (c *Config) Validate() error {
	if c.MaxFileSizeBytes <= 0 {
		return errors.New("max_file_size_bytes must be greater than 0")
	}
	if c.NumberOfGoRoutines < 1 {
		return errors.New("number_of_go_routines must be greater than 0")
	}
	if c.RowGroupSizeBytes <= 0 {
		return errors.New("row_group_size_bytes must be greater than 0")
	}
	if c.RowGroupSizeBytes > c.MaxFileSizeBytes {
		return errors.New("row_group_size_bytes must be less than or equal to max_file_size_bytes")
	}
	if c.PageSizeBytes <= 0 {
		return errors.New("page_size_bytes must be greater than 0")
	}

	switch strings.ToLower(c.CompressionCodec) {
	case "snappy", "zstd", "gzip", "uncompressed":
	default:
		return errors.New("compression_codec must be one of [snappy, zstd, gzip, uncompressed]")
	}

	switch strings.ToLower(c.Schema) {
	case "", defaultSchema, "snowflake":
		return nil
	default:
		return errors.New("schema must be one of [datadog, snowflake]")
	}
}
