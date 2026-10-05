// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package googlecloudpubsubreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id          component.ID
		expected    component.Config
		expectedErr error
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			expected: &Config{
				AckDeadlineSeconds: defaultAckDeadlineSeconds,
				AckBatchWait:       defaultAckBatchWait,
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "customname"),
			expected: &Config{
				ProjectID: "my-project",
				UserAgent: "opentelemetry-collector-contrib {{version}}",
				TimeoutSettings: exporterhelper.TimeoutConfig{
					Timeout: 20 * time.Second,
				},
				Subscription:       "projects/my-project/subscriptions/otlp-subscription",
				AckDeadlineSeconds: defaultAckDeadlineSeconds,
				AckBatchWait:       defaultAckBatchWait,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestAckConfig(t *testing.T) {
	// defaults come from the factory
	c := NewFactory().CreateDefaultConfig().(*Config)
	assert.Equal(t, defaultAckDeadlineSeconds, c.AckDeadlineSeconds)
	assert.Equal(t, defaultAckBatchWait, c.AckBatchWait)
	assert.Equal(t, defaultAckDeadlineSeconds, c.resolvedAckDeadlineSeconds())
	assert.Equal(t, defaultAckBatchWait, c.resolvedAckBatchWait())

	// zero values resolve to the defaults
	zero := &Config{}
	assert.Equal(t, defaultAckDeadlineSeconds, zero.resolvedAckDeadlineSeconds())
	assert.Equal(t, defaultAckBatchWait, zero.resolvedAckBatchWait())

	// explicit values are honored
	custom := &Config{AckDeadlineSeconds: 120, AckBatchWait: 2 * time.Second}
	assert.Equal(t, int32(120), custom.resolvedAckDeadlineSeconds())
	assert.Equal(t, 2*time.Second, custom.resolvedAckBatchWait())

	// validation of the ack fields
	valid := NewFactory().CreateDefaultConfig().(*Config)
	valid.Subscription = "projects/my-project/subscriptions/my-subscription"
	assert.NoError(t, valid.validate())

	tooLow := *valid
	tooLow.AckDeadlineSeconds = minAckDeadlineSeconds - 1
	assert.Error(t, tooLow.validate())

	tooHigh := *valid
	tooHigh.AckDeadlineSeconds = maxAckDeadlineSeconds + 1
	assert.Error(t, tooHigh.validate())

	negativeWait := *valid
	negativeWait.AckBatchWait = -1 * time.Second
	assert.Error(t, negativeWait.validate())
}

func TestConfigValidation(t *testing.T) {
	factory := NewFactory()
	c := factory.CreateDefaultConfig().(*Config)
	c.Subscription = "projects/000project/subscriptions/my-subscription"
	assert.Error(t, c.validate())
	c.Subscription = "projects/my-project/topics/my-topic"
	assert.Error(t, c.validate())
	c.Subscription = "projects/my-project/subscriptions/my-subscription"
	assert.NoError(t, c.validate())
	// Test for project IDs with a single colon (not at start, not at end)
	c.Subscription = "projects/s3ns:my-project/subscriptions/my-subscription"
	assert.NoError(t, c.validate())
	// Invalid: colon at the start
	c.Subscription = "projects/:invalid/subscriptions/my-subscription"
	assert.Error(t, c.validate())
	// Invalid: colon at the end
	c.Subscription = "projects/invalid:/subscriptions/my-subscription"
	assert.Error(t, c.validate())
	// Invalid: multiple colons
	c.Subscription = "projects/s3ns:invalid:invalid/subscriptions/my-subscription"
	assert.Error(t, c.validate())
}
