package hotreloadprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	otelprocessor "go.opentelemetry.io/collector/processor"
)

type HotReloadMetricsProcessor struct {
	*HotReloadProcessor[consumer.Metrics, otelprocessor.Metrics]
}

func newHotReloadMetricsProcessor(
	context context.Context,
	set otelprocessor.Settings,
	cfg *Config,
	nextConsumer consumer.Metrics,
	registry ProcessorFactoryRegistry,
) (*HotReloadMetricsProcessor, error) {
	hp, err := newHotReloadProcessor[consumer.Metrics, otelprocessor.Metrics](
		context,
		set,
		cfg,
		nextConsumer,
		loaderMetrics{subprocessorLoader: subprocessorLoader[consumer.Metrics]{factories: registry}},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating hotreload processor: %w", err)
	}
	return &HotReloadMetricsProcessor{hp}, nil
}

func (hp *HotReloadMetricsProcessor) ConsumeMetrics(ctx context.Context, ld pmetric.Metrics) error {
	return hp.consume(func() error {
		fsp := hp.firstSubprocessor.Load()
		if fsp == nil {
			return nil
		}
		return (*fsp).ConsumeMetrics(ctx, ld)
	})
}

// passthroughMetricsProcessor is a fallback processor that just forwards metrics to nextConsumer.
type passthroughMetricsProcessor struct {
	next consumer.Metrics
}

func (p *passthroughMetricsProcessor) Start(ctx context.Context, host component.Host) error {
	// No-op
	return nil
}

func (p *passthroughMetricsProcessor) Shutdown(ctx context.Context) error {
	// No-op
	return nil
}

func (p *passthroughMetricsProcessor) ConsumeMetrics(
	ctx context.Context,
	ld pmetric.Metrics,
) error {
	return p.next.ConsumeMetrics(ctx, ld)
}

func (p *passthroughMetricsProcessor) Capabilities() consumer.Capabilities {
	return processorCapabilities
}
