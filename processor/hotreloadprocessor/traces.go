// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/hotreloadprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	otelcol "go.opentelemetry.io/collector/otelcol"
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
) (*HotReloadTracesProcessor, error) {
	hp, err := newHotReloadProcessor[consumer.Traces, otelprocessor.Traces](
		context,
		set,
		cfg,
		nextConsumer,
		loaderTraces{},
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

type loaderTraces struct{}

//nolint:unused // False positive from generic loader wiring.
func (loaderTraces) load(
	ctx context.Context,
	config otelcol.Config,
	set otelprocessor.Settings,
	host component.Host,
	nextConsumer consumer.Traces,
) ([]otelprocessor.Traces, error) {
	proc, err := loadTracesSubprocessors(ctx, config, set, host, nextConsumer)
	if err != nil {
		return nil, err
	}

	if len(proc) == 0 {
		// Return a passthrough processor if none are loaded
		return []otelprocessor.Traces{&passthroughTracesProcessor{next: nextConsumer}}, nil
	}

	proc2 := make([]otelprocessor.Traces, len(proc))
	for i, p := range proc {
		proc2[i] = p.(otelprocessor.Traces)
	}
	return proc2, nil
}

// passthroughTracesProcessor is a fallback processor that just forwards traces to nextConsumer.
//
//nolint:unused // False positive from interface implementation only used through generics.
type passthroughTracesProcessor struct {
	next consumer.Traces
}

//nolint:unused // False positive from interface implementation only used through generics.
func (*passthroughTracesProcessor) Start(_ context.Context, _ component.Host) error {
	// No-op
	return nil
}

//nolint:unused // False positive from interface implementation only used through generics.
func (*passthroughTracesProcessor) Shutdown(_ context.Context) error {
	// No-op
	return nil
}

//nolint:unused // False positive on interface method implementation.
func (p *passthroughTracesProcessor) ConsumeTraces(ctx context.Context, ld ptrace.Traces) error {
	return p.next.ConsumeTraces(ctx, ld)
}

//nolint:unused // False positive from interface implementation only used through generics.
func (*passthroughTracesProcessor) Capabilities() consumer.Capabilities {
	return processorCapabilities
}
