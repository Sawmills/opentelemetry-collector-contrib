// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	for _, configType := range []string{
		"cwmetrics", "cwlogs", "otlp_v1",
	} {
		t.Run(configType, func(t *testing.T) {
			fileName := configType + "_config.yaml"
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", fileName))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			err = xconfmap.Validate(cfg)
			assert.NoError(t, err)
			require.Equal(t, &Config{
				RecordType: configType,
				AccessKey:  "some_access_key",
				CommonAttributes: CommonAttributesConfig{
					OnInvalid: commonAttributesOnInvalidIgnore,
				},
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Transport: "tcp",
						Endpoint:  "0.0.0.0:4433",
					},
					TLS: configoptional.Some(configtls.ServerConfig{
						Config: configtls.Config{
							CertFile: "server.crt",
							KeyFile:  "server.key",
						},
					}),
				},
			}, cfg)
		})
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "invalid_config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	err = xconfmap.Validate(cfg)
	assert.ErrorIs(t, err, errRecordTypeEncodingSet)
}

func TestValidate(t *testing.T) {
	testCases := map[string]struct {
		mutate  func(*Config)
		wantErr string
	}{
		"Default": {},
		"CommonAttributesOnInvalidIgnore": {
			mutate: func(cfg *Config) {
				cfg.CommonAttributes.OnInvalid = commonAttributesOnInvalidIgnore
			},
		},
		"CommonAttributesOnInvalidError": {
			mutate: func(cfg *Config) {
				cfg.CommonAttributes.OnInvalid = commonAttributesOnInvalidError
			},
		},
		"CommonAttributesOnInvalidInvalid": {
			mutate: func(cfg *Config) {
				cfg.CommonAttributes.OnInvalid = "invalid"
			},
			wantErr: `common_attributes.on_invalid must be one of "ignore" or "error"`,
		},
	}
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			if testCase.mutate != nil {
				testCase.mutate(cfg)
			}
			err := cfg.Validate()
			if testCase.wantErr != "" {
				require.EqualError(t, err, testCase.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
