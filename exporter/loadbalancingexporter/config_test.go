// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	require.NotNil(t, cfg)
}

func TestConfigValidatePayloadCompression(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())

	cfg.QueueSettings.QueueConfig = configoptional.Some(exporterhelper.NewDefaultQueueConfig())
	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	require.NoError(t, cfg.Validate())
	require.Nil(t, cfg.QueueSettings.QueueConfig.Get().StorageID)

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionZstd
	require.NoError(t, cfg.Validate())

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompression("invalid")
	require.Error(t, cfg.Validate())
}

func TestConfigValidateCompressInMemory(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.QueueSettings.CompressInMemory = true
	require.ErrorContains(t, cfg.Validate(), "sending_queue.compress_in_memory requires sending_queue.enabled=true")

	cfg.QueueSettings.QueueConfig = configoptional.Some(exporterhelper.NewDefaultQueueConfig())
	require.ErrorContains(t, cfg.Validate(), "sending_queue.compress_in_memory requires sending_queue.payload_compression")

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionNone
	require.ErrorContains(t, cfg.Validate(), "sending_queue.compress_in_memory requires sending_queue.payload_compression")

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	require.NoError(t, cfg.Validate())

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionZstd
	require.NoError(t, cfg.Validate())

	queueCfg := exporterhelper.NewDefaultQueueConfig()
	queueCfg.WaitForResult = true
	cfg.QueueSettings.QueueConfig = configoptional.Some(queueCfg)
	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	require.ErrorContains(t, cfg.Validate(), "`wait_for_result` is not supported with a persistent queue configured with `storage`")
}

func TestConfigValidateLogBatcher(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, cfg.Validate())

	cfg.LogBatcher.Enabled = true
	cfg.LogBatcher.MaxRecords = 0
	require.ErrorContains(t, cfg.Validate(), "log_batcher.max_records")

	cfg.LogBatcher.MaxRecords = 10
	cfg.LogBatcher.MaxBytes = 0
	require.ErrorContains(t, cfg.Validate(), "log_batcher.max_bytes")

	cfg.LogBatcher.MaxBytes = 1024
	cfg.LogBatcher.FlushInterval = 0
	require.ErrorContains(t, cfg.Validate(), "log_batcher.flush_interval")

	cfg.LogBatcher.FlushInterval = time.Second
	require.NoError(t, cfg.Validate())
}

func TestLoadConfigWithQueueCompression(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"protocol": map[string]any{
			"otlp": map[string]any{
				"endpoint": "localhost:4317",
				"tls": map[string]any{
					"insecure": true,
				},
			},
		},
		"resolver": map[string]any{
			"static": map[string]any{
				"hostnames": []string{"localhost:4317"},
			},
		},
		"sending_queue": map[string]any{
			"enabled":             true,
			"queue_size":          1000,
			"num_consumers":       2,
			"payload_compression": "zstd",
			"compress_in_memory":  true,
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))
	require.True(t, cfg.QueueSettings.QueueConfig.HasValue())
	require.Equal(t, int64(1000), cfg.QueueSettings.QueueConfig.Get().QueueSize)
	require.Equal(t, 2, cfg.QueueSettings.QueueConfig.Get().NumConsumers)
	require.Equal(t, QueuePayloadCompressionZstd, cfg.QueueSettings.PayloadCompression)
	require.True(t, cfg.QueueSettings.CompressInMemory)
}

func TestLoadConfigWithLogBatcher(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	conf := confmap.NewFromStringMap(map[string]any{
		"protocol": map[string]any{
			"otlp": map[string]any{
				"endpoint": "localhost:4317",
				"tls": map[string]any{
					"insecure": true,
				},
			},
		},
		"resolver": map[string]any{
			"static": map[string]any{
				"hostnames": []string{"localhost:4317"},
			},
		},
		"log_batcher": map[string]any{
			"enabled":        true,
			"max_records":    1024,
			"max_bytes":      2097152,
			"flush_interval": "250ms",
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))
	require.True(t, cfg.LogBatcher.Enabled)
	require.Equal(t, 1024, cfg.LogBatcher.MaxRecords)
	require.Equal(t, 2097152, cfg.LogBatcher.MaxBytes)
	require.Equal(t, 250*time.Millisecond, cfg.LogBatcher.FlushInterval)
}
