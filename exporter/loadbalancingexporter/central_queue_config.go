// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultCentralQueueCapacityBytes      int64 = 16 << 30
	defaultCentralQueueNumConsumers             = 60
	defaultCentralQueueTargetBytes        int64 = 256 << 10
	defaultCentralQueueMaxCompressedBytes int64 = 1 << 20
	defaultCentralQueueMaxUncompressed    int64 = 4 << 20
	defaultCentralQueueMaxMergedItems           = 10000
	defaultCentralQueueMaxDelay                 = 250 * time.Millisecond
	defaultCentralQueueLaneCount                = 64
)

type CentralQueueConfig struct {
	Enabled            bool                       `mapstructure:"enabled"`
	PayloadCompression QueuePayloadCompression    `mapstructure:"payload_compression"`
	CapacityBytes      int64                      `mapstructure:"capacity_bytes"`
	NumConsumers       int                        `mapstructure:"num_consumers"`
	RequestBatching    CentralQueueBatchingConfig `mapstructure:"request_batching"`

	// prevent unkeyed literal initialization
	_ struct{}
}

type CentralQueueBatchingConfig struct {
	TargetCompressedBytes int64         `mapstructure:"target_compressed_bytes"`
	MaxCompressedBytes    int64         `mapstructure:"max_compressed_bytes"`
	MaxUncompressedBytes  int64         `mapstructure:"max_uncompressed_bytes"`
	MaxMergedItems        int           `mapstructure:"max_merged_items"`
	MaxDelay              time.Duration `mapstructure:"max_delay"`
	LaneCount             int           `mapstructure:"lane_count"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func (c *CentralQueueConfig) ApplyDefaults() {
	if !c.Enabled {
		return
	}
	if c.PayloadCompression == "" {
		c.PayloadCompression = QueuePayloadCompressionZstd
	}
	if c.CapacityBytes == 0 {
		c.CapacityBytes = defaultCentralQueueCapacityBytes
	}
	if c.NumConsumers == 0 {
		c.NumConsumers = defaultCentralQueueNumConsumers
	}
	c.RequestBatching.ApplyDefaults()
}

func (c *CentralQueueBatchingConfig) ApplyDefaults() {
	if c.TargetCompressedBytes == 0 {
		c.TargetCompressedBytes = defaultCentralQueueTargetBytes
	}
	if c.MaxCompressedBytes == 0 {
		c.MaxCompressedBytes = defaultCentralQueueMaxCompressedBytes
	}
	if c.MaxUncompressedBytes == 0 {
		c.MaxUncompressedBytes = defaultCentralQueueMaxUncompressed
	}
	if c.MaxMergedItems == 0 {
		c.MaxMergedItems = defaultCentralQueueMaxMergedItems
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = defaultCentralQueueMaxDelay
	}
	if c.LaneCount == 0 {
		c.LaneCount = defaultCentralQueueLaneCount
	}
}

func (c CentralQueueConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	switch c.PayloadCompression {
	case QueuePayloadCompressionSnappy, QueuePayloadCompressionZstd:
		// Valid payload compression value.
	default:
		return fmt.Errorf("central_queue.payload_compression must be one of [snappy, zstd], found %q", c.PayloadCompression)
	}
	if c.CapacityBytes <= 0 {
		return errors.New("central_queue.capacity_bytes must be greater than 0")
	}
	if c.NumConsumers <= 0 {
		return errors.New("central_queue.num_consumers must be greater than 0")
	}
	return c.RequestBatching.validateEnabled()
}

func (c CentralQueueBatchingConfig) validateEnabled() error {
	if c.TargetCompressedBytes <= 0 {
		return errors.New("central_queue.request_batching.target_compressed_bytes must be greater than 0")
	}
	if c.MaxCompressedBytes < c.TargetCompressedBytes {
		return errors.New("central_queue.request_batching.max_compressed_bytes must be greater than or equal to target_compressed_bytes")
	}
	if c.MaxUncompressedBytes <= 0 {
		return errors.New("central_queue.request_batching.max_uncompressed_bytes must be greater than 0")
	}
	if c.MaxMergedItems <= 0 {
		return errors.New("central_queue.request_batching.max_merged_items must be greater than 0")
	}
	if c.MaxDelay <= 0 {
		return errors.New("central_queue.request_batching.max_delay must be greater than 0")
	}
	if c.LaneCount <= 0 {
		return errors.New("central_queue.request_batching.lane_count must be greater than 0")
	}
	return nil
}
