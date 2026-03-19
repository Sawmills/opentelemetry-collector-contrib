// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hotreloadprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/hotreloadprocessor"

import (
	"errors"
	"time"
)

// Config defines configuration for Resource processor.
type Config struct {
	// ConfigurationPrefix is the prefix to the s3 configuration file.
	ConfigurationPrefix string `mapstructure:"configuration_prefix"`
	// EncryptionKey is the key used to encrypt the configuration file.
	EncryptionKey string `mapstructure:"encryption_key"`
	// Region is the region of the S3 bucket where the configuration file is stored.
	Region string `mapstructure:"region"`
	// RefreshInterval is the interval at which the configuration is refreshed.
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`
	// ShutdownDelay is the delay before shutting down the old subprocessors.
	ShutdownDelay time.Duration `mapstructure:"shutdown_delay"`
	// WatchPath is the path to the file that is watched for changes.
	WatchPath *string `mapstructure:"watch_path"`
}

func (cfg *Config) Validate() error {
	if cfg.ConfigurationPrefix == "" {
		return errors.New("configuration_prefix must be specified")
	}

	if cfg.EncryptionKey == "" {
		return errors.New("encryption_key must be specified")
	}

	if cfg.Region == "" {
		return errors.New("region must be specified")
	}

	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = 60 * time.Second
	}

	if cfg.ShutdownDelay <= 0 {
		cfg.ShutdownDelay = 10 * time.Second
	}

	if cfg.RefreshInterval <= cfg.ShutdownDelay {
		return errors.New("refresh_interval must be greater than shutdown_delay")
	}

	return nil
}