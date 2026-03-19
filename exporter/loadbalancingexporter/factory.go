// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

const (
	zapEndpointKey = "endpoint"
)

// NewFactory creates a factory for the exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	otlpFactory := otlpexporter.NewFactory()
	otlpDefaultCfg := otlpFactory.CreateDefaultConfig().(*otlpexporter.Config)
	otlpDefaultCfg.ClientConfig.Endpoint = "placeholder:4317"
	queueCfg := exporterhelper.NewDefaultQueueConfig()
	queueCfg.Enabled = false

	return &Config{
		// By default we disable resilience options on loadbalancing exporter level
		// to maintain compatibility with workflow in previous versions
		Protocol: Protocol{
			OTLP: *otlpDefaultCfg,
		},
		QueueSettings: QueueSettings{
			QueueBatchConfig:   queueCfg,
			PayloadCompression: QueuePayloadCompressionNone,
		},
		LogBatcher: LogBatcherConfig{
			Enabled:       false,
			MaxRecords:    defaultLogBatchMaxRecords,
			MaxBytes:      defaultLogBatchMaxBytes,
			FlushInterval: defaultLogBatchFlushTimeout,
		},
	}
}

func buildExporterConfig(cfg *Config, endpoint string) otlpexporter.Config {
	oCfg := cfg.Protocol.OTLP
	oCfg.ClientConfig.Endpoint = endpoint

	return oCfg
}

func buildExporterSettings(typ component.Type, params exporter.Settings, endpoint string) exporter.Settings {
	if name := params.ID.Name(); name != "" {
		params.ID = component.NewIDWithName(typ, name)
	} else {
		params.ID = component.NewID(typ)
	}
	telemetry := params.TelemetrySettings
	telemetry.MeterProvider = metricnoop.NewMeterProvider()
	telemetry.TracerProvider = tracenoop.NewTracerProvider()
	params.Logger = params.Logger.With(zap.String(zapEndpointKey, endpoint))
	telemetry.Logger = params.Logger
	params.TelemetrySettings = telemetry
	return params
}

func buildExporterResilienceOptions(options []exporterhelper.Option, cfg *Config, payloadCodec *queuePayloadCodec) []exporterhelper.Option {
	if cfg.TimeoutSettings.Timeout > 0 {
		options = append(options, exporterhelper.WithTimeout(cfg.TimeoutSettings))
	}
	if cfg.QueueSettings.Enabled {
		options = append(options, exporterhelper.WithQueue(cfg.QueueSettings.QueueBatchConfig))
		if payloadCodec != nil {
			options = append(options, exporterhelper.WithQueueBatchPayloadCodec(payloadCodec))
			if cfg.QueueSettings.CompressInMemory {
				options = append(options, exporterhelper.WithQueueBatchInMemoryEncoding(true))
			}
		}
	}
	if cfg.Enabled {
		options = append(options, exporterhelper.WithRetry(cfg.BackOffConfig))
	}

	return options
}

func createTracesExporter(ctx context.Context, params exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newTracesExporter(params, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot configure loadbalancing traces exporter: %w", err)
	}
	payloadCodec := newQueuePayloadCodecIfEnabled(c)

	options := []exporterhelper.Option{
		exporterhelper.WithStart(exp.Start),
		exporterhelper.WithShutdown(shutdownWithCodec(component.ShutdownFunc(exp.Shutdown), payloadCodec)),
		exporterhelper.WithCapabilities(exp.Capabilities()),
	}

	return exporterhelper.NewTraces(
		ctx,
		params,
		cfg,
		exp.ConsumeTraces,
		buildExporterResilienceOptions(options, c, payloadCodec)...,
	)
}

func createLogsExporter(ctx context.Context, params exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exporter, err := newLogsExporter(params, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot configure loadbalancing logs exporter: %w", err)
	}
	payloadCodec := newQueuePayloadCodecIfEnabled(c)

	options := []exporterhelper.Option{
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(shutdownWithCodec(component.ShutdownFunc(exporter.Shutdown), payloadCodec)),
		exporterhelper.WithCapabilities(exporter.Capabilities()),
	}

	return exporterhelper.NewLogs(
		ctx,
		params,
		cfg,
		exporter.ConsumeLogs,
		buildExporterResilienceOptions(options, c, payloadCodec)...,
	)
}

func createMetricsExporter(ctx context.Context, params exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exporter, err := newMetricsExporter(params, cfg)
	if err != nil {
		return nil, fmt.Errorf("cannot configure loadbalancing metrics exporter: %w", err)
	}
	payloadCodec := newQueuePayloadCodecIfEnabled(c)

	options := []exporterhelper.Option{
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(shutdownWithCodec(component.ShutdownFunc(exporter.Shutdown), payloadCodec)),
		exporterhelper.WithCapabilities(exporter.Capabilities()),
	}

	return exporterhelper.NewMetrics(
		ctx,
		params,
		cfg,
		exporter.ConsumeMetrics,
		buildExporterResilienceOptions(options, c, payloadCodec)...,
	)
}

func newQueuePayloadCodecIfEnabled(cfg *Config) *queuePayloadCodec {
	if !cfg.QueueSettings.Enabled {
		return nil
	}
	if cfg.QueueSettings.PayloadCompression == "" {
		return nil
	}

	return newQueuePayloadCodec(cfg.QueueSettings.PayloadCompression)
}

func shutdownWithCodec(shutdown component.ShutdownFunc, codec *queuePayloadCodec) component.ShutdownFunc {
	if codec == nil {
		return shutdown
	}
	return func(ctx context.Context) error {
		return multierr.Append(shutdown.Shutdown(ctx), codec.Close())
	}
}
