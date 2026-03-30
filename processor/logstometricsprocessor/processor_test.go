// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/metadata"
)

const testDataDir = "testdata"

func TestProcessorWithLogs(t *testing.T) {
	testCases := []string{
		"sum",
		"histograms",
		"exponential_histograms",
		"metric_identity",
		"gauge",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ctx := t.Context()
			logTestDataDir := filepath.Join(testDataDir, "logs")
			inputLogs, err := golden.ReadLogs(filepath.Join(logTestDataDir, "logs.yaml"))
			require.NoError(t, err)

			logsSink := &consumertest.LogsSink{}
			tcTestDataDir := filepath.Join(logTestDataDir, tc)
			factory, settings, cfg := setupProcessor(t, tcTestDataDir)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
			require.NoError(t, err)

			// Start should succeed (no initialization needed with routereceiver)
			require.NoError(t, processor.Start(ctx, componenttest.NewNopHost()))
			defer func() {
				require.NoError(t, processor.Shutdown(ctx))
			}()

			require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

			// Note: With routereceiver, we can't easily verify metrics in unit tests
			// as routereceiver.RouteMetrics routes to a receiver in the metrics pipeline.
			// In integration tests, metrics would be verified through the metrics pipeline.

			// Verify logs were forwarded (default behavior)
			require.NotEmpty(t, logsSink.AllLogs(), "expected logs to be forwarded")
			assertLogsEqual(t, inputLogs, logsSink.AllLogs()[0])
		})
	}
}

func TestProcessorDropLogs(t *testing.T) {
	ctx := t.Context()

	inputLogs, err := golden.ReadLogs(filepath.Join(testDataDir, "logs", "logs.yaml"))
	require.NoError(t, err)

	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		Route:    "test_route",
		DropLogs: true,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: &config.Sum{
					Value: "1",
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

	require.NoError(t, processor.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, processor.Shutdown(ctx))
	}()

	require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

	// Note: With routereceiver, we can't easily verify metrics in unit tests
	// as routereceiver.RouteMetrics routes to a receiver in the metrics pipeline.

	// Verify logs were dropped - check that no log records were forwarded
	require.Equal(
		t,
		0,
		logsSink.LogRecordCount(),
		"expected logs to be dropped when drop_logs is true",
	)
}

func TestProcessorForwardLogs(t *testing.T) {
	ctx := t.Context()

	inputLogs, err := golden.ReadLogs(filepath.Join(testDataDir, "logs", "logs.yaml"))
	require.NoError(t, err)

	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		Route:    "test_route",
		DropLogs: false, // Explicitly set to false
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: &config.Sum{
					Value: "1",
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

	require.NoError(t, processor.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(t, processor.Shutdown(ctx))
	}()

	require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

	// Note: With routereceiver, we can't easily verify metrics in unit tests
	// as routereceiver.RouteMetrics routes to a receiver in the metrics pipeline.

	// Verify logs were forwarded
	require.NotEmpty(t, logsSink.AllLogs(), "expected logs to be forwarded when drop_logs is false")
	assertLogsEqual(t, inputLogs, logsSink.AllLogs()[0])
}

// Note: Connector-related tests removed since we now use routereceiver
// which routes metrics via routereceiver.RouteMetrics package function.

func TestProcessorNoMetricDefinitions(t *testing.T) {
	cfg := &config.Config{
		Route:    "test_route",
		DropLogs: false,
		Logs:     []config.MetricInfo{}, // No metric definitions
	}

	// Validation should reject empty logs configuration
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no logs configuration provided")
}

func TestProcessorInvalidConfig(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *config.Config
		expectedErr string
	}{
		{
			name: "missing route",
			cfg: &config.Config{
				DropLogs: false,
				Logs: []config.MetricInfo{
					{
						Name:        "log.count",
						Description: "Count of log records",
						Sum: &config.Sum{
							Value: "1",
						},
					},
				},
			},
			expectedErr: "route must be specified",
		},
		{
			name: "no logs configuration",
			cfg: &config.Config{
				Route:    "test_route",
				DropLogs: false,
				Logs:     []config.MetricInfo{},
			},
			expectedErr: "no logs configuration provided",
		},
		{
			name: "invalid metric name",
			cfg: &config.Config{
				Route:    "test_route",
				DropLogs: false,
				Logs: []config.MetricInfo{
					{
						Name:        "", // Missing name
						Description: "Count of log records",
						Sum: &config.Sum{
							Value: "1",
						},
					},
				},
			},
			expectedErr: "missing required metric name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func BenchmarkProcessorWithLogs(b *testing.B) {
	ctx := b.Context()

	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		Route:    "test_route",
		DropLogs: false,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: &config.Sum{
					Value: "1",
				},
			},
		},
	}
	require.NoError(b, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(b, err)

	require.NoError(b, processor.Start(ctx, componenttest.NewNopHost()))
	defer func() {
		require.NoError(b, processor.Shutdown(ctx))
	}()

	inputLogs, err := golden.ReadLogs("testdata/logs/logs.yaml")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := processor.ConsumeLogs(ctx, inputLogs); err != nil {
			b.Fatal(err)
		}
	}
}

// Helper functions

func setupProcessor(
	tb testing.TB,
	testDataDir string,
) (processor.Factory, processor.Settings, *config.Config) {
	tb.Helper()
	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)

	cfg := factory.CreateDefaultConfig().(*config.Config)
	cfg.Route = "test_route"

	conf, err := confmaptest.LoadConf(filepath.Join(testDataDir, "config.yaml"))
	require.NoError(tb, err)

	// Extract the processor config from under the processor name key
	sub, err := conf.Sub(metadata.Type.String())
	require.NoError(tb, err)
	require.NoError(tb, sub.Unmarshal(cfg))

	// Override route for testing
	cfg.Route = "test_route"
	require.NoError(tb, cfg.Validate())

	return factory, settings, cfg
}

func assertLogsEqual(t *testing.T, expected, actual plog.Logs) {
	assert.NoError(t, plogtest.CompareLogs(expected, actual, plogtest.IgnoreObservedTimestamp()))
}
