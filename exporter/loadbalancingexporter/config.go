// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
)

type routingKey int

const (
	traceIDRouting routingKey = iota
	svcRouting
	metricNameRouting
	resourceRouting
	streamIDRouting
	attrRouting
)

const (
	svcRoutingStr        = "service"
	traceIDRoutingStr    = "traceID"
	metricNameRoutingStr = "metric"
	resourceRoutingStr   = "resource"
	streamIDRoutingStr   = "streamID"
	attrRoutingStr       = "attributes"
)

// Config defines configuration for the exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             QueueSettings    `mapstructure:"sending_queue"`
	LogBatcher                LogBatcherConfig `mapstructure:"log_batcher"`

	Protocol Protocol         `mapstructure:"protocol"`
	Resolver ResolverSettings `mapstructure:"resolver"`

	// RoutingKey is a single routing key value
	RoutingKey string `mapstructure:"routing_key"`

	// RoutingAttributes creates a composite routing key, based on several resource attributes of the application.
	//
	// Supports all attributes available (both resource and span), as well as the pseudo attributes "span.kind" and
	// "span.name".
	RoutingAttributes []string `mapstructure:"routing_attributes"`
}

type QueueSettings struct {
	exporterhelper.QueueBatchConfig `mapstructure:",squash"`
	PayloadCompression              QueuePayloadCompression `mapstructure:"payload_compression"`
	CompressInMemory                bool                    `mapstructure:"compress_in_memory"`
}

type LogBatcherConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	MaxRecords    int           `mapstructure:"max_records"`
	MaxBytes      int           `mapstructure:"max_bytes"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
}

func (q *QueueSettings) Unmarshal(conf *confmap.Conf) error {
	if conf == nil {
		return nil
	}

	// QueueBatchConfig has its own Unmarshal implementation that rejects unknown fields.
	// Parse the queue payload compression fields separately so they can coexist with queue settings.
	queueCfg := conf.ToStringMap()
	payloadRaw, hasPayload := queueCfg["payload_compression"]
	compressRaw, hasCompressInMemory := queueCfg["compress_in_memory"]
	delete(queueCfg, "payload_compression")
	delete(queueCfg, "compress_in_memory")

	queueConf := confmap.NewFromStringMap(queueCfg)
	if err := queueConf.Unmarshal(&q.QueueBatchConfig); err != nil {
		return err
	}

	if hasPayload {
		payload, ok := payloadRaw.(string)
		if !ok {
			return errors.New("sending_queue.payload_compression must be a string")
		}
		q.PayloadCompression = QueuePayloadCompression(payload)
	}

	if hasCompressInMemory {
		compressInMemory, ok := compressRaw.(bool)
		if !ok {
			return errors.New("sending_queue.compress_in_memory must be a bool")
		}
		q.CompressInMemory = compressInMemory
	}

	return nil
}

type QueuePayloadCompression string

const (
	QueuePayloadCompressionNone   QueuePayloadCompression = "none"
	QueuePayloadCompressionSnappy QueuePayloadCompression = "snappy"
	QueuePayloadCompressionZstd   QueuePayloadCompression = "zstd"
)

func (q QueueSettings) Validate() error {
	if err := q.QueueBatchConfig.Validate(); err != nil {
		return err
	}
	switch q.PayloadCompression {
	case "", QueuePayloadCompressionNone, QueuePayloadCompressionSnappy, QueuePayloadCompressionZstd:
		// Valid payload compression value.
	default:
		return fmt.Errorf("sending_queue.payload_compression must be one of [none, snappy, zstd], found %q", q.PayloadCompression)
	}

	if q.CompressInMemory && !q.Enabled {
		return errors.New("sending_queue.compress_in_memory requires sending_queue.enabled=true")
	}
	if q.CompressInMemory && (q.PayloadCompression == "" || q.PayloadCompression == QueuePayloadCompressionNone) {
		return errors.New("sending_queue.compress_in_memory requires sending_queue.payload_compression to be set to snappy or zstd")
	}
	if q.CompressInMemory {
		return errors.New("sending_queue.compress_in_memory is not supported by this exporter helper version; use sending_queue.payload_compression instead")
	}

	return nil
}

func (cfg *Config) Validate() error {
	if err := cfg.QueueSettings.Validate(); err != nil {
		return err
	}
	return cfg.LogBatcher.Validate()
}

func (c LogBatcherConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.MaxRecords <= 0 {
		return errors.New("log_batcher.max_records must be greater than 0 when log_batcher.enabled=true")
	}
	if c.MaxBytes <= 0 {
		return errors.New("log_batcher.max_bytes must be greater than 0 when log_batcher.enabled=true")
	}
	if c.FlushInterval <= 0 {
		return errors.New("log_batcher.flush_interval must be greater than 0 when log_batcher.enabled=true")
	}
	return nil
}

// Protocol holds the individual protocol-specific settings. Only OTLP is supported at the moment.
type Protocol struct {
	OTLP otlpexporter.Config `mapstructure:"otlp"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// ResolverSettings defines the configurations for the backend resolver
type ResolverSettings struct {
	Static      configoptional.Optional[StaticResolver]      `mapstructure:"static"`
	DNS         configoptional.Optional[DNSResolver]         `mapstructure:"dns"`
	K8sSvc      configoptional.Optional[K8sSvcResolver]      `mapstructure:"k8s"`
	AWSCloudMap configoptional.Optional[AWSCloudMapResolver] `mapstructure:"aws_cloud_map"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// StaticResolver defines the configuration for the resolver providing a fixed list of backends
type StaticResolver struct {
	Hostnames []string `mapstructure:"hostnames"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// DNSResolver defines the configuration for the DNS resolver
type DNSResolver struct {
	Hostname string        `mapstructure:"hostname"`
	Port     string        `mapstructure:"port"`
	Interval time.Duration `mapstructure:"interval"`
	Timeout  time.Duration `mapstructure:"timeout"`
	// prevent unkeyed literal initialization
	_ struct{}
}

// K8sSvcResolver defines the configuration for the DNS resolver
type K8sSvcResolver struct {
	Service         string        `mapstructure:"service"`
	Ports           []int32       `mapstructure:"ports"`
	Timeout         time.Duration `mapstructure:"timeout"`
	ReturnHostnames bool          `mapstructure:"return_hostnames"`
	// prevent unkeyed literal initialization
	_ struct{}
}

type AWSCloudMapResolver struct {
	NamespaceName string                   `mapstructure:"namespace"`
	ServiceName   string                   `mapstructure:"service_name"`
	HealthStatus  types.HealthStatusFilter `mapstructure:"health_status"`
	Interval      time.Duration            `mapstructure:"interval"`
	Timeout       time.Duration            `mapstructure:"timeout"`
	Port          *uint16                  `mapstructure:"port"`
}
