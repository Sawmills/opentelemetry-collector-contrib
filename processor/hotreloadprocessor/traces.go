// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/hotreloadprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	otelprocessor "go.opentelemetry.io/collector/processor"
)

type HotReloadTracesProcessor struct {
	*HotReloadProcessor[consumer.Traces, otelprocessor.Traces]
}

func newHotReloadTracesProcessor(
	context context.Context,
	set otelprocessor.Settings,
	cfg *Config,
	nextConsumer consumer.Traces,
	registry ProcessorFactoryRegistry,
) (*HotReloadTracesProcessor, error) {
	hp, err := newHotReloadProcessor[consumer.Traces, otelprocessor.Traces](
		context,
		set,
		cfg,
		nextConsumer,
		loaderTraces{subprocessorLoader: subprocessorLoader[consumer.Traces]{factories: registry}},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating hotreload processor: %w", err)
	}
	return &HotReloadTracesProcessor{hp}, nil
}

func (hp *HotReloadTracesProcessor) ConsumeTraces(ctx context.Context, ld ptrace.Traces) error {
	return hp.consume(func() error {
		fsp := hp.firstSubprocessor.Load()
		if fsp == nil {
			return nil
		}
		return (*fsp).ConsumeTraces(ctx, ld)
	})
}

// passthroughTracesProcessor is a fallback processor that just forwards traces to nextConsumer.
type passthroughTracesProcessor struct {
	next consumer.Traces
}

func (p *passthroughTracesProcessor) Start(ctx context.Context, host component.Host) error {
	// No-op
	return nil
}

func (p *passthroughTracesProcessor) Shutdown(ctx context.Context) error {
	// No-op
	return nil
}

func (p *passthroughTracesProcessor) ConsumeTraces(ctx context.Context, ld ptrace.Traces) error {
	return p.next.ConsumeTraces(ctx, ld)
}

func (p *passthroughTracesProcessor) Capabilities() consumer.Capabilities {
	return processorCapabilities
}
