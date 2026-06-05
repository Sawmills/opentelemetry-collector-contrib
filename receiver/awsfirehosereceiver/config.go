// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/zap"
)

var errRecordTypeEncodingSet = errors.New("record_type must not be set when encoding is set")

const (
	commonAttributesOnInvalidIgnore = "ignore"
	commonAttributesOnInvalidError  = "error"
)

type Config struct {
	// ServerConfig is used to set up the Firehose delivery
	// endpoint. The Firehose delivery stream expects an HTTPS
	// endpoint, so TLSs must be used to enable that.
	confighttp.ServerConfig `mapstructure:",squash"`
	// Encoding identifies the encoding of records received from
	// Firehose.
	//
	// Defaults to telemetry-specific encodings: "cwlog" for logs,
	// and "cwmetrics" for metrics. This default behavior is
	// deprecated as of v0.149.0, and in the future it will be
	// required to specify the encoding explicitly.
	Encoding string `mapstructure:"encoding"`
	// RecordType is an alias for Encoding for backwards compatibility.
	// It is an error to specify both encoding and record_type.
	//
	// Deprecated: [v0.121.0] use Encoding instead.
	RecordType string `mapstructure:"record_type"`
	// AccessKey is checked against the one received with each request.
	// This can be set when creating or updating the Firehose delivery
	// stream.
	AccessKey configopaque.String `mapstructure:"access_key"`
	// CommonAttributes controls how Firehose common attributes are parsed
	// and attached to resource attributes.
	CommonAttributes CommonAttributesConfig `mapstructure:"common_attributes"`
}

type CommonAttributesConfig struct {
	// MapKey stores Firehose common attributes as a nested resource attribute
	// map. If empty, common attributes are attached as flat resource attributes.
	MapKey string `mapstructure:"map_key"`
	// OnInvalid controls malformed X-Amz-Firehose-Common-Attributes handling.
	// Supported values are "ignore" and "error".
	OnInvalid string `mapstructure:"on_invalid"`
}

// Validate checks that the endpoint and record type exist and
// are valid.
func (c *Config) Validate() error {
	if c.NetAddr.Endpoint == "" {
		return errors.New("must specify endpoint")
	}
	if c.RecordType != "" && c.Encoding != "" {
		return errRecordTypeEncodingSet
	}
	if err := c.CommonAttributes.Validate(); err != nil {
		return err
	}
	return nil
}

func (c CommonAttributesConfig) Validate() error {
	switch c.OnInvalid {
	case "", commonAttributesOnInvalidIgnore, commonAttributesOnInvalidError:
		return nil
	default:
		return fmt.Errorf("common_attributes.on_invalid must be one of %q or %q", commonAttributesOnInvalidIgnore, commonAttributesOnInvalidError)
	}
}

func (c CommonAttributesConfig) onInvalid() string {
	if c.OnInvalid == "" {
		return commonAttributesOnInvalidIgnore
	}
	return c.OnInvalid
}

func handleDeprecatedConfig(cfg *Config, logger *zap.Logger) {
	if cfg.RecordType != "" {
		logger.Warn("record_type is deprecated, and will be removed in a future version. Use encoding instead.")
	}
}
