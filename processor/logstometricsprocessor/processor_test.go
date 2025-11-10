// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstometricsprocessor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/logstometricsprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			logTestDataDir := filepath.Join(testDataDir, "logs")
			inputLogs, err := golden.ReadLogs(filepath.Join(logTestDataDir, "logs.yaml"))
			require.NoError(t, err)

			metricsSink := &consumertest.MetricsSink{}
			logsSink := &consumertest.LogsSink{}
			tcTestDataDir := filepath.Join(logTestDataDir, tc)
			factory, settings, cfg := setupProcessor(t, tcTestDataDir, metricsSink)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
			require.NoError(t, err)

			expectedMetrics, err := golden.ReadMetrics(filepath.Join(tcTestDataDir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, processor.Start(ctx, createTestHost(t, cfg.MetricsConnector, metricsSink)))
			defer func() {
				require.NoError(t, processor.Shutdown(ctx))
			}()

			require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

			// Verify metrics were extracted
			require.Greater(t, len(metricsSink.AllMetrics()), 0, "expected at least one metrics batch")
			assertAggregatedMetrics(t, expectedMetrics, metricsSink.AllMetrics()[0])

			// Verify logs were forwarded (default behavior)
			require.Greater(t, len(logsSink.AllLogs()), 0, "expected logs to be forwarded")
			assertLogsEqual(t, inputLogs, logsSink.AllLogs()[0])
		})
	}
}

func TestProcessorDropLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inputLogs, err := golden.ReadLogs(filepath.Join(testDataDir, "logs", "logs.yaml"))
	require.NoError(t, err)

	metricsSink := &consumertest.MetricsSink{}
	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		MetricsConnector: component.MustNewID("test_connector"),
		DropLogs:       true,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: configoptional.Some(config.Sum{
					Value: "1",
				}),
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

		require.NoError(t, processor.Start(ctx, createTestHost(t, cfg.MetricsConnector, metricsSink)))
	defer func() {
		require.NoError(t, processor.Shutdown(ctx))
	}()

	require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

	// Verify metrics were extracted
	require.Greater(t, len(metricsSink.AllMetrics()), 0, "expected metrics to be extracted")

	// Verify logs were dropped - check that no log records were forwarded
	require.Equal(t, 0, logsSink.LogRecordCount(), "expected logs to be dropped when drop_logs is true")
}

func TestProcessorForwardLogs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inputLogs, err := golden.ReadLogs(filepath.Join(testDataDir, "logs", "logs.yaml"))
	require.NoError(t, err)

	metricsSink := &consumertest.MetricsSink{}
	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		MetricsConnector: component.MustNewID("test_connector"),
		DropLogs:       false, // Explicitly set to false
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: configoptional.Some(config.Sum{
					Value: "1",
				}),
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

		require.NoError(t, processor.Start(ctx, createTestHost(t, cfg.MetricsConnector, metricsSink)))
	defer func() {
		require.NoError(t, processor.Shutdown(ctx))
	}()

	require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

	// Verify metrics were extracted
	require.Greater(t, len(metricsSink.AllMetrics()), 0, "expected metrics to be extracted")

	// Verify logs were forwarded
	require.Greater(t, len(logsSink.AllLogs()), 0, "expected logs to be forwarded when drop_logs is false")
	assertLogsEqual(t, inputLogs, logsSink.AllLogs()[0])
}

func TestProcessorMetricsConnectorNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.Config{
		MetricsConnector: component.MustNewID("nonexistent"),
		DropLogs:       false,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: configoptional.Some(config.Sum{
					Value: "1",
				}),
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
	logsSink := &consumertest.LogsSink{}
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

	// Start should fail because connector is not found
	// Create a test host that implements connectorHost but doesn't have the connector
	host := &testHost{
		connectors: map[component.ID]component.Component{
			// Connector with different ID, so the one we're looking for won't be found
			component.MustNewID("other_connector"): &testMetricsConnector{
				MetricsRouterAndConsumer: connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
					pipeline.NewID(pipeline.SignalMetrics): &consumertest.MetricsSink{},
				}),
			},
		},
	}
	err = processor.Start(ctx, host)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestProcessorMetricsConnectorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inputLogs, err := golden.ReadLogs(filepath.Join(testDataDir, "logs", "logs.yaml"))
	require.NoError(t, err)

	// Create a metrics consumer that returns an error
	errMetricsConsumer := consumertest.NewErr(errors.New("consume error"))
	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		MetricsConnector: component.MustNewID("test_connector"),
		DropLogs:       false,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: configoptional.Some(config.Sum{
					Value: "1",
				}),
			},
		},
	}
	require.NoError(t, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(t, err)

	host := createTestHostWithErrorConsumer(t, cfg.MetricsConnector, errMetricsConsumer)
	require.NoError(t, processor.Start(ctx, host))
	defer func() {
		require.NoError(t, processor.Shutdown(ctx))
	}()

	// Processing should succeed even if metrics export fails
	require.NoError(t, processor.ConsumeLogs(ctx, inputLogs))

	// Logs should still be forwarded despite metrics export error
	require.Greater(t, len(logsSink.AllLogs()), 0, "expected logs to be forwarded even if metrics export fails")
}

func TestProcessorNoMetricDefinitions(t *testing.T) {
	cfg := &config.Config{
		MetricsConnector: component.MustNewID("test_connector"),
		DropLogs:       false,
		Logs:           []config.MetricInfo{}, // No metric definitions
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
			name: "missing metrics_connector",
			cfg: &config.Config{
				DropLogs: false,
				Logs: []config.MetricInfo{
					{
						Name:        "log.count",
						Description: "Count of log records",
						Sum: configoptional.Some(config.Sum{
							Value: "1",
						}),
					},
				},
			},
			expectedErr: "metrics_connector must be specified",
		},
		{
			name: "no logs configuration",
			cfg: &config.Config{
				MetricsConnector: component.MustNewID("test_connector"),
				DropLogs:       false,
				Logs:           []config.MetricInfo{},
			},
			expectedErr: "no logs configuration provided",
		},
		{
			name: "invalid metric name",
			cfg: &config.Config{
				MetricsConnector: component.MustNewID("test_connector"),
				DropLogs:       false,
				Logs: []config.MetricInfo{
					{
						Name:        "", // Missing name
						Description:  "Count of log records",
						Sum: configoptional.Some(config.Sum{
							Value: "1",
						}),
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsSink := &consumertest.MetricsSink{}
	logsSink := &consumertest.LogsSink{}

	cfg := &config.Config{
		MetricsConnector: component.MustNewID("test_connector"),
		DropLogs:       false,
		Logs: []config.MetricInfo{
			{
				Name:        "log.count",
				Description: "Count of log records",
				Sum: configoptional.Some(config.Sum{
					Value: "1",
				}),
			},
		},
	}
	require.NoError(b, cfg.Validate())

	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)
			processor, err := factory.CreateLogs(ctx, settings, cfg, logsSink)
	require.NoError(b, err)

	require.NoError(b, processor.Start(ctx, createTestHost(b, cfg.MetricsConnector, metricsSink)))
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

func setupProcessor(t testing.TB, testDataDir string, metricsSink *consumertest.MetricsSink) (processor.Factory, processor.Settings, *config.Config) {
	t.Helper()
	factory := NewFactory()
	settings := processortest.NewNopSettings(metadata.Type)

	cfg := factory.CreateDefaultConfig().(*config.Config)
	cfg.MetricsConnector = component.MustNewID("test_connector")

	conf, err := confmaptest.LoadConf(filepath.Join(testDataDir, "config.yaml"))
	require.NoError(t, err)
	
	// Extract the processor config from under the processor name key
	sub, err := conf.Sub(metadata.Type.String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	// Override metrics connector for testing
	cfg.MetricsConnector = component.MustNewID("test_connector")
	require.NoError(t, cfg.Validate())

	return factory, settings, cfg
}

func createTestHost(t testing.TB, connectorID component.ID, metricsSink *consumertest.MetricsSink) component.Host {
	// Create a metrics router with the sink
	router := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
		pipeline.NewID(pipeline.SignalMetrics): metricsSink,
	})
	
	// Create a test connector that embeds the router
	testConnector := &testMetricsConnector{
		MetricsRouterAndConsumer: router,
	}
	
	return &testHost{
		connectors: map[component.ID]component.Component{
			connectorID: testConnector,
		},
	}
}

func createTestHostWithErrorConsumer(t testing.TB, connectorID component.ID, errConsumer consumer.Metrics) component.Host {
	// Create a metrics router with the error consumer
	router := connector.NewMetricsRouter(map[pipeline.ID]consumer.Metrics{
		pipeline.NewID(pipeline.SignalMetrics): errConsumer,
	})
	
	// Create a test connector that embeds the router
	testConnector := &testMetricsConnector{
		MetricsRouterAndConsumer: router,
	}
	
	return &testHost{
		connectors: map[component.ID]component.Component{
			connectorID: testConnector,
		},
	}
}

// testMetricsConnector is a test connector that implements connector.MetricsRouterAndConsumer
// It embeds the router so it implements both connector.Metrics and connector.MetricsRouterAndConsumer
type testMetricsConnector struct {
	connector.MetricsRouterAndConsumer
}

func (t *testMetricsConnector) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (t *testMetricsConnector) Shutdown(ctx context.Context) error {
	return nil
}

func (t *testMetricsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (t *testMetricsConnector) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// This shouldn't be called directly, but implement it for completeness
	pipelineID := pipeline.NewID(pipeline.SignalMetrics)
	consumer, err := t.Consumer(pipelineID)
	if err != nil {
		return err
	}
	return consumer.ConsumeMetrics(ctx, md)
}

// testHost is a test implementation of component.Host
type testHost struct {
	connectors map[component.ID]component.Component
}

func (h *testHost) GetConnectors() map[component.ID]component.Component {
	return h.connectors
}

func (h *testHost) GetExtensions() map[component.ID]component.Component {
	return nil
}

func (h *testHost) GetFactory(kind component.Kind, componentType component.Type) component.Factory {
	return nil
}

func assertAggregatedMetrics(t *testing.T, expected, actual pmetric.Metrics) {
	opts := []pmetrictest.CompareMetricsOption{
		pmetrictest.IgnoreTimestamp(),
		pmetrictest.IgnoreStartTimestamp(),
		pmetrictest.IgnoreMetricDataPointsOrder(), // Ignore datapoint order as aggregation order may vary
		// Ignore collector instance info attributes that are added automatically
		pmetrictest.IgnoreResourceAttributeValue("logstometricsprocessor.service.instance.id"),
		pmetrictest.IgnoreResourceAttributeValue("logstometricsprocessor.service.name"),
		pmetrictest.IgnoreResourceAttributeValue("logstometricsprocessor.service.namespace"),
	}
	assert.NoError(t, pmetrictest.CompareMetrics(expected, actual, opts...))
}

func assertLogsEqual(t *testing.T, expected, actual plog.Logs) {
	assert.NoError(t, plogtest.CompareLogs(expected, actual, plogtest.IgnoreObservedTimestamp()))
}


