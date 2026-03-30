// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/hotreloadprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/hotreloadprocessor/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

type FactoryOption func(*factoryOptions)

type factoryOptions struct {
	processorFactories ProcessorFactoryRegistry
}

func newFactoryOptions() factoryOptions {
	return factoryOptions{
		processorFactories: defaultProcessorFactories(),
	}
}

func WithProcessorFactories(factories ...processor.Factory) FactoryOption {
	return func(opts *factoryOptions) {
		if opts.processorFactories == nil {
			opts.processorFactories = make(ProcessorFactoryRegistry)
		}
		for _, factory := range factories {
			opts.processorFactories[factory.Type()] = factory
		}
	}
}

func NewFactory(opts ...FactoryOption) processor.Factory {
	options := newFactoryOptions()
	for _, opt := range opts {
		opt(&options)
	}
	registry := cloneProcessorFactoryRegistry(options.processorFactories)

	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor(registry), metadata.LogsStability),
		processor.WithMetrics(createMetricsProcessor(registry), metadata.MetricsStability),
		processor.WithTraces(createTracesProcessor(registry), metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	config := &Config{
		Region:          "us-east-1",
		RefreshInterval: 60 * time.Second,
		ShutdownDelay:   10 * time.Second,
	}
	return config
}

func createLogsProcessor(
	registry ProcessorFactoryRegistry,
) processor.CreateLogsFunc {
	return func(
		ctx context.Context,
		set processor.Settings,
		cfg component.Config,
		nextConsumer consumer.Logs,
	) (processor.Logs, error) {
		hp, err := newHotReloadLogsProcessor(ctx, set, cfg.(*Config), nextConsumer, registry)
		if err != nil {
			return nil, err
		}
		return hp, nil
	}
}

func createMetricsProcessor(
	registry ProcessorFactoryRegistry,
) processor.CreateMetricsFunc {
	return func(
		ctx context.Context,
		set processor.Settings,
		cfg component.Config,
		nextConsumer consumer.Metrics,
	) (processor.Metrics, error) {
		hp, err := newHotReloadMetricsProcessor(ctx, set, cfg.(*Config), nextConsumer, registry)
		if err != nil {
			return nil, err
		}
		return hp, nil
	}
}

func createTracesProcessor(
	registry ProcessorFactoryRegistry,
) processor.CreateTracesFunc {
	return func(
		ctx context.Context,
		set processor.Settings,
		cfg component.Config,
		nextConsumer consumer.Traces,
	) (processor.Traces, error) {
		hp, err := newHotReloadTracesProcessor(ctx, set, cfg.(*Config), nextConsumer, registry)
		if err != nil {
			return nil, err
		}
		return hp, nil
	}
}
