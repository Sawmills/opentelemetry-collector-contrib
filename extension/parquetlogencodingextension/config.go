package parquetlogencodingextension

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultMaxFileSizeBytes   = 100 * 1024 * 1024
	defaultNumberOfGoRoutines = 4
	defaultRowGroupSizeBytes  = 128 * 1024 * 1024
	defaultPageSizeBytes      = 1 * 1024 * 1024
	defaultCompressionCodec   = "snappy"
)

type Config struct {
	MaxFileSizeBytes   int64  `mapstructure:"max_file_size_bytes"`
	NumberOfGoRoutines int64  `mapstructure:"number_of_go_routines"`
	RowGroupSizeBytes  int64  `mapstructure:"row_group_size_bytes"`
	PageSizeBytes      int64  `mapstructure:"page_size_bytes"`
	CompressionCodec   string `mapstructure:"compression_codec"`
}

func CreateDefaultConfig() component.Config {
	return &Config{
		MaxFileSizeBytes:   defaultMaxFileSizeBytes,
		NumberOfGoRoutines: defaultNumberOfGoRoutines,
		RowGroupSizeBytes:  defaultRowGroupSizeBytes,
		PageSizeBytes:      defaultPageSizeBytes,
		CompressionCodec:   defaultCompressionCodec,
	}
}

func (c *Config) Validate() error {
	if c.MaxFileSizeBytes <= 0 {
		return fmt.Errorf("max_file_size_bytes must be greater than 0")
	}
	if c.NumberOfGoRoutines < 1 {
		return fmt.Errorf("number_of_go_routines must be greater than 0")
	}
	if c.RowGroupSizeBytes <= 0 {
		return fmt.Errorf("row_group_size_bytes must be greater than 0")
	}
	if c.PageSizeBytes <= 0 {
		return fmt.Errorf("page_size_bytes must be greater than 0")
	}

	switch strings.ToLower(c.CompressionCodec) {
	case "snappy", "zstd", "gzip", "uncompressed":
		return nil
	default:
		return fmt.Errorf(
			"compression_codec must be one of [snappy, zstd, gzip, uncompressed]",
		)
	}
}
