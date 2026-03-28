// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	otelprocessor "go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	"gopkg.in/yaml.v3"
)

var customProcessorType = component.MustNewType("customprocessor")

type customProcessorConfig struct {
	StandardAttributes []string `mapstructure:"standard_attributes"`
}

func TestWithProcessorFactoriesAddsCustomProcessor(t *testing.T) {
	factory := NewFactory(WithProcessorFactories(newFakeCustomFactory(t)))
	require.NotNil(t, factory)
}

func TestLoadLogsSubprocessorsWithInjectedFactory(t *testing.T) {
	ctx := context.Background()
	config := loadHotReloadTestConfig(t, "config-throughput.yaml")
	registry := cloneProcessorFactoryRegistry(defaultProcessorFactories())
	registry[customProcessorType] = newFakeCustomFactory(t)

	inputLogs := buildLoaderTestLogs()
	transformedLogs := plog.NewLogs()
	nextConsumer, _ := consumer.NewLogs(func(_ context.Context, ld plog.Logs) error {
		ld.CopyTo(transformedLogs)
		return nil
	})

	subprocessors, err := loadLogsSubprocessors(
		ctx,
		config,
		processortest.NewNopSettings(component.MustNewType("hotreloadprocessor")),
		nextConsumer,
		registry,
	)
	require.NoError(t, err)
	require.Len(t, subprocessors, 1)

	err = subprocessors[0].ConsumeLogs(ctx, inputLogs)
	require.NoError(t, err)
	require.JSONEq(t, marshalLogsJSON(t, inputLogs), marshalLogsJSON(t, transformedLogs))
}

func TestLoadMetricsSubprocessorsWithInjectedFactory(t *testing.T) {
	ctx := context.Background()
	config := loadHotReloadTestConfig(t, "config-throughput.yaml")
	registry := cloneProcessorFactoryRegistry(defaultProcessorFactories())
	registry[customProcessorType] = newFakeCustomFactory(t)

	inputMetrics := buildLoaderTestMetrics()
	transformedMetrics := pmetric.NewMetrics()
	nextConsumer, _ := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		md.CopyTo(transformedMetrics)
		return nil
	})

	subprocessors, err := loadMetricsSubprocessors(
		ctx,
		config,
		processortest.NewNopSettings(component.MustNewType("hotreloadprocessor")),
		nextConsumer,
		registry,
	)
	require.NoError(t, err)
	require.Len(t, subprocessors, 1)

	err = subprocessors[0].ConsumeMetrics(ctx, inputMetrics)
	require.NoError(t, err)
	require.JSONEq(t, marshalMetricsJSON(t, inputMetrics), marshalMetricsJSON(t, transformedMetrics))
}

func TestLoadTracesSubprocessorsWithInjectedFactory(t *testing.T) {
	ctx := context.Background()
	config := loadHotReloadTestConfig(t, "config-throughput.yaml")
	registry := cloneProcessorFactoryRegistry(defaultProcessorFactories())
	registry[customProcessorType] = newFakeCustomFactory(t)

	inputTraces := buildLoaderTestTraces()
	transformedTraces := ptrace.NewTraces()
	nextConsumer, _ := consumer.NewTraces(func(_ context.Context, td ptrace.Traces) error {
		td.CopyTo(transformedTraces)
		return nil
	})

	subprocessors, err := loadTracesSubprocessors(
		ctx,
		config,
		processortest.NewNopSettings(component.MustNewType("hotreloadprocessor")),
		nextConsumer,
		registry,
	)
	require.NoError(t, err)
	require.Len(t, subprocessors, 1)

	err = subprocessors[0].ConsumeTraces(ctx, inputTraces)
	require.NoError(t, err)
	require.JSONEq(t, marshalTracesJSON(t, inputTraces), marshalTracesJSON(t, transformedTraces))
}

func newFakeCustomFactory(t *testing.T) otelprocessor.Factory {
	return otelprocessor.NewFactory(
		customProcessorType,
		func() component.Config {
			return &customProcessorConfig{}
		},
		otelprocessor.WithLogs(
			func(
				_ context.Context,
				_ otelprocessor.Settings,
				cfg component.Config,
				nextConsumer consumer.Logs,
			) (otelprocessor.Logs, error) {
				assertFakeCustomConfig(t, cfg)
				return &passthroughLogsProcessor{next: nextConsumer}, nil
			},
			component.StabilityLevelDevelopment,
		),
		otelprocessor.WithMetrics(
			func(
				_ context.Context,
				_ otelprocessor.Settings,
				cfg component.Config,
				nextConsumer consumer.Metrics,
			) (otelprocessor.Metrics, error) {
				assertFakeCustomConfig(t, cfg)
				return &passthroughMetricsProcessor{next: nextConsumer}, nil
			},
			component.StabilityLevelDevelopment,
		),
		otelprocessor.WithTraces(
			func(
				_ context.Context,
				_ otelprocessor.Settings,
				cfg component.Config,
				nextConsumer consumer.Traces,
			) (otelprocessor.Traces, error) {
				assertFakeCustomConfig(t, cfg)
				return &passthroughTracesProcessor{next: nextConsumer}, nil
			},
			component.StabilityLevelDevelopment,
		),
	)
}

func assertFakeCustomConfig(t *testing.T, cfg component.Config) {
	t.Helper()

	customCfg, ok := cfg.(*customProcessorConfig)
	require.True(t, ok)
	require.Equal(
		t,
		[]string{"service.name", "library.name", "request.id"},
		customCfg.StandardAttributes,
	)
}

func loadHotReloadTestConfig(t *testing.T, file string) otelcol.Config {
	t.Helper()

	content, err := os.ReadFile(filepath.Join("testdata", file))
	require.NoError(t, err)

	var config otelcol.Config
	err = yaml.Unmarshal(content, &config)
	require.NoError(t, err)

	return config
}

func buildLoaderTestLogs() plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.Resource().Attributes().PutStr("service.name", "checkout")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().Attributes().PutStr("library.name", "checkout-logger")

	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("hotreload loader")
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return logs
}

func buildLoaderTestMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.Resource().Attributes().PutStr("service.name", "checkout")

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("requests_total")
	dataPoint := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dataPoint.SetIntValue(1)
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return metrics
}

func buildLoaderTestTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "checkout")

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	span := scopeSpans.Spans().AppendEmpty()
	span.SetName("hotreload-loader")
	now := time.Now()
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-1 * time.Second)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(now))

	return traces
}

func marshalMetricsJSON(t *testing.T, metrics pmetric.Metrics) string {
	t.Helper()

	json, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(metrics)
	require.NoError(t, err)
	return string(json)
}

func marshalLogsJSON(t *testing.T, logs plog.Logs) string {
	t.Helper()

	json, err := (&plog.JSONMarshaler{}).MarshalLogs(logs)
	require.NoError(t, err)
	return string(json)
}

func marshalTracesJSON(t *testing.T, traces ptrace.Traces) string {
	t.Helper()

	json, err := (&ptrace.JSONMarshaler{}).MarshalTraces(traces)
	require.NoError(t, err)
	return string(json)
}
