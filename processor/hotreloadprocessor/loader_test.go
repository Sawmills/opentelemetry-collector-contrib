// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor/processortest"
	"gopkg.in/yaml.v3"
)

func TestLoadSubprocessorsWithStandardAttributesProcessor(t *testing.T) {
	var cfg otelcol.Config
	require.NoError(t, yaml.Unmarshal([]byte(`
processors:
  batch/b:
    send_batch_size: 1
    timeout: 1s
service:
  pipelines:
    logs/test:
      processors:
        - batch/b
    metrics/test:
      processors:
        - batch/b
`), &cfg))

	settings := processortest.NewNopSettings(component.MustNewType("hotreloadprocessor"))

	logsProcessors, err := loadLogsSubprocessors(
		t.Context(),
		cfg,
		settings,
		consumertest.NewNop(),
		defaultProcessorFactories(),
	)
	require.NoError(t, err)
	require.Len(t, logsProcessors, 1)

	metricsProcessors, err := loadMetricsSubprocessors(
		t.Context(),
		cfg,
		settings,
		consumertest.NewNop(),
		defaultProcessorFactories(),
	)
	require.NoError(t, err)
	require.Len(t, metricsProcessors, 1)
}

func TestLoadSubprocessorsRejectsMultiplePipelinesPerSignal(t *testing.T) {
	var cfg otelcol.Config
	require.NoError(t, yaml.Unmarshal([]byte(`
processors:
  batch/b:
    send_batch_size: 1
    timeout: 1s
service:
  pipelines:
    logs/test-1:
      processors:
        - batch/b
    logs/test-2:
      processors:
        - batch/b
`), &cfg))

	settings := processortest.NewNopSettings(component.MustNewType("hotreloadprocessor"))

	_, err := loadLogsSubprocessors(
		t.Context(),
		cfg,
		settings,
		consumertest.NewNop(),
		defaultProcessorFactories(),
	)
	require.ErrorContains(t, err, "only one pipeline per signal is supported")
}
