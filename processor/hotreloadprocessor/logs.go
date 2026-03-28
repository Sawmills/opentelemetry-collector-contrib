package hotreloadprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	otelprocessor "go.opentelemetry.io/collector/processor"
)

type HotReloadLogsProcessor struct {
	*HotReloadProcessor[consumer.Logs, otelprocessor.Logs]
}

func newHotReloadLogsProcessor(
	context context.Context,
	set otelprocessor.Settings,
	cfg *Config,
	nextConsumer consumer.Logs,
	registry ProcessorFactoryRegistry,
) (*HotReloadLogsProcessor, error) {
	hp, err := newHotReloadProcessor(
		context,
		set,
		cfg,
		nextConsumer,
		loaderLogs{subprocessorLoader: subprocessorLoader[consumer.Logs]{factories: registry}},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating hotreload processor: %w", err)
	}
	return &HotReloadLogsProcessor{hp}, nil
}

func (hp *HotReloadLogsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return hp.consume(func() error {
		fsp := hp.firstSubprocessor.Load()
		if fsp == nil {
			return nil
		}
		return (*fsp).ConsumeLogs(ctx, ld)
	})
}

// passthroughLogsProcessor is a fallback processor that just forwards logs to nextConsumer.
type passthroughLogsProcessor struct {
	next consumer.Logs
}

func (p *passthroughLogsProcessor) Start(ctx context.Context, host component.Host) error {
	// No-op
	return nil
}

func (p *passthroughLogsProcessor) Shutdown(ctx context.Context) error {
	// No-op
	return nil
}

func (p *passthroughLogsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return p.next.ConsumeLogs(ctx, ld)
}

func (p *passthroughLogsProcessor) Capabilities() consumer.Capabilities {
	return processorCapabilities
}
