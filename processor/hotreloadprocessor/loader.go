package hotreloadprocessor

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbyattrsprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/otelcol"
	otelpipeline "go.opentelemetry.io/collector/pipeline"
	otelprocessor "go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
)

type ProcessorFactoryRegistry map[component.Type]otelprocessor.Factory

func defaultProcessorFactories() ProcessorFactoryRegistry {
	factories := []otelprocessor.Factory{
		transformprocessor.NewFactory(),
		batchprocessor.NewFactory(),
		logstometricsprocessor.NewFactory(),
		metricstransformprocessor.NewFactory(),
		groupbyattrsprocessor.NewFactory(),
	}
	return newProcessorFactoryRegistry(factories...)
}

func newProcessorFactoryRegistry(
	factories ...otelprocessor.Factory,
) ProcessorFactoryRegistry {
	registry := make(ProcessorFactoryRegistry, len(factories))
	for _, factory := range factories {
		registry[factory.Type()] = factory
	}
	return registry
}

func cloneProcessorFactoryRegistry(
	registry ProcessorFactoryRegistry,
) ProcessorFactoryRegistry {
	cloned := make(ProcessorFactoryRegistry, len(registry))
	for typ, factory := range registry {
		cloned[typ] = factory
	}
	return cloned
}

type subprocessorLoader[T any] struct {
	factories ProcessorFactoryRegistry
}

func (l subprocessorLoader[T]) loadSubprocessors(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	signal otelpipeline.Signal,
	nextConsumer T,
	createConsumer func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer T, signal otelpipeline.Signal) (T, error),
) ([]T, error) {
	nc := nextConsumer

	if len(config.Service.Pipelines) == 0 {
		return nil, fmt.Errorf("no pipelines found")
	}

	found := false
	var subprocessors []T
	for id, pipeline := range config.Service.Pipelines {
		if id.Signal() != signal {
			continue
		}
		found = true
		subprocessors = make([]T, len(pipeline.Processors))
		for i := len(pipeline.Processors) - 1; i >= 0; i-- {
			processorID := pipeline.Processors[i]
			factory, ok := l.factories[processorID.Type()]
			if !ok {
				return nil, fmt.Errorf("processor factory not found for type: %s", processorID.Type())
			}
			conf := factory.CreateDefaultConfig()
			processorConfig := config.Processors[processorID].(map[string]any)
			newConfMap := confmap.NewFromStringMap(processorConfig)
			err := newConfMap.Unmarshal(&conf)
			if err != nil {
				return nil, err
			}

			nc, err = createConsumer(ctx, factory, otelprocessor.Settings{
				ID:                processorID,
				BuildInfo:         settings.BuildInfo,
				TelemetrySettings: settings.TelemetrySettings,
			}, conf, nc, id.Signal())
			if err != nil {
				return nil, err
			}
			subprocessors[i] = nc
		}
	}

	if !found {
		return nil, fmt.Errorf("no pipelines found for signal: %s", signal)
	}

	return subprocessors, nil
}

type loaderLogs struct {
	subprocessorLoader[consumer.Logs]
}

func (l loaderLogs) load(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Logs,
) ([]otelprocessor.Logs, error) {
	proc, err := l.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalLogs,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Logs, signal otelpipeline.Signal) (consumer.Logs, error) {
			if signal != otelpipeline.SignalLogs {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateLogs(ctx, settings, config, nextConsumer)
		},
	)
	if err != nil {
		return nil, err
	}

	if len(proc) == 0 {
		return []otelprocessor.Logs{&passthroughLogsProcessor{next: nextConsumer}}, nil
	}

	proc2 := make([]otelprocessor.Logs, len(proc))
	for i, p := range proc {
		proc2[i] = p.(otelprocessor.Logs)
	}
	return proc2, nil
}

type loaderMetrics struct {
	subprocessorLoader[consumer.Metrics]
}

func (l loaderMetrics) load(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Metrics,
) ([]otelprocessor.Metrics, error) {
	proc, err := l.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalMetrics,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Metrics, signal otelpipeline.Signal) (consumer.Metrics, error) {
			if signal != otelpipeline.SignalMetrics {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateMetrics(ctx, settings, config, nextConsumer)
		},
	)
	if err != nil {
		return nil, err
	}

	if len(proc) == 0 {
		return []otelprocessor.Metrics{&passthroughMetricsProcessor{next: nextConsumer}}, nil
	}

	proc2 := make([]otelprocessor.Metrics, len(proc))
	for i, p := range proc {
		proc2[i] = p.(otelprocessor.Metrics)
	}
	return proc2, nil
}

type loaderTraces struct {
	subprocessorLoader[consumer.Traces]
}

func (l loaderTraces) load(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Traces,
) ([]otelprocessor.Traces, error) {
	proc, err := l.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalTraces,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Traces, signal otelpipeline.Signal) (consumer.Traces, error) {
			if signal != otelpipeline.SignalTraces {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateTraces(ctx, settings, config, nextConsumer)
		},
	)
	if err != nil {
		return nil, err
	}

	if len(proc) == 0 {
		return []otelprocessor.Traces{&passthroughTracesProcessor{next: nextConsumer}}, nil
	}

	proc2 := make([]otelprocessor.Traces, len(proc))
	for i, p := range proc {
		proc2[i] = p.(otelprocessor.Traces)
	}
	return proc2, nil
}

func loadLogsSubprocessors(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Logs,
	factories ProcessorFactoryRegistry,
) ([]consumer.Logs, error) {
	return loaderLogs{subprocessorLoader: subprocessorLoader[consumer.Logs]{factories: factories}}.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalLogs,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Logs, signal otelpipeline.Signal) (consumer.Logs, error) {
			if signal != otelpipeline.SignalLogs {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateLogs(ctx, settings, config, nextConsumer)
		},
	)
}

func loadMetricsSubprocessors(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Metrics,
	factories ProcessorFactoryRegistry,
) ([]consumer.Metrics, error) {
	return loaderMetrics{subprocessorLoader: subprocessorLoader[consumer.Metrics]{factories: factories}}.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalMetrics,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Metrics, signal otelpipeline.Signal) (consumer.Metrics, error) {
			if signal != otelpipeline.SignalMetrics {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateMetrics(ctx, settings, config, nextConsumer)
		},
	)
}

func loadTracesSubprocessors(
	ctx context.Context,
	config otelcol.Config,
	settings otelprocessor.Settings,
	nextConsumer consumer.Traces,
	factories ProcessorFactoryRegistry,
) ([]consumer.Traces, error) {
	return loaderTraces{subprocessorLoader: subprocessorLoader[consumer.Traces]{factories: factories}}.loadSubprocessors(
		ctx,
		config,
		settings,
		otelpipeline.SignalTraces,
		nextConsumer,
		func(ctx context.Context, factory otelprocessor.Factory, settings otelprocessor.Settings, config component.Config, nextConsumer consumer.Traces, signal otelpipeline.Signal) (consumer.Traces, error) {
			if signal != otelpipeline.SignalTraces {
				return nil, fmt.Errorf("unsupported signal: %s", signal)
			}
			return factory.CreateTraces(ctx, settings, config, nextConsumer)
		},
	)
}
