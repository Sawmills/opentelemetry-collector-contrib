// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"

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

	cfg.QueueSettings.Enabled = true
	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	require.NoError(t, cfg.Validate())

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionZstd
	require.NoError(t, cfg.Validate())

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompression("invalid")
	require.Error(t, cfg.Validate())
}

func TestConfigValidateCompressInMemory(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.QueueSettings.CompressInMemory = true
	require.ErrorContains(t, cfg.Validate(), "sending_queue.compress_in_memory requires sending_queue.enabled=true")

	cfg.QueueSettings.Enabled = true
	require.ErrorContains(t, cfg.Validate(), "sending_queue.compress_in_memory requires sending_queue.payload_compression")

	cfg.QueueSettings.PayloadCompression = QueuePayloadCompressionSnappy
	require.NoError(t, cfg.Validate())
}
