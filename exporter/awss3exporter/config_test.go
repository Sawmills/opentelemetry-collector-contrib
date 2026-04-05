// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/otelcol/otelcoltest"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/metadata"
)

func loadConfig(t *testing.T, fixture string) *Config {
	t.Helper()
	t.Setenv("S3_PREFIX", "123456")
	t.Setenv("S3_ACCESS_KEY_ID", "123456")
	t.Setenv("S3_SECRET_ACCESS_KEY", "123456")

	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", fixture), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	return cfg.Exporters[component.MustNewID("awss3")].(*Config)
}

func loadConfigFromYAML(t *testing.T, yaml string) (*Config, error) {
	t.Helper()

	dir := t.TempDir()
	fixture := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(fixture, []byte(yaml), 0o600))

	factories, err := otelcoltest.NopFactories()
	require.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory

	cfg, err := otelcoltest.LoadConfigAndValidate(fixture, factories)
	if err != nil {
		return nil, err
	}

	return cfg.Exporters[component.MustNewID("awss3")].(*Config), nil
}

func TestLoadConfig_LegacyFields(t *testing.T) {
	cfg := loadConfig(t, "legacy-config.yaml")

	assert.Equal(t, "minute", cfg.S3Uploader.S3Partition)
	assert.Equal(t, "123456", cfg.S3Uploader.S3Prefix)
	assert.Equal(t, "123456", cfg.S3Uploader.AccessKeyID)
	assert.Equal(t, "123456", cfg.S3Uploader.SecretAccessKey)
	assert.Equal(t, "AES256", cfg.S3Uploader.ServerSideEncryption)
	assert.Equal(t, "year=%Y/month=%m/day=%d/hour=%H/minute=%M", cfg.normalizedS3PartitionFormat())
}

func TestLoadConfig_LegacyDatadogTemplate(t *testing.T) {
	cfg := loadConfig(t, "legacy-datadog-json.yaml")

	assert.Equal(t, "{{.Prefix}}/{{.Date}}/{{.UUID}}.json.gz", cfg.S3Uploader.S3KeyTemplate)
	require.NotNil(t, cfg.Encoding)
	assert.Equal(t, "datadog_log_encoding/awss3/destination-json", cfg.Encoding.String())
}

func TestLoadConfig_LegacyParquetEncoding(t *testing.T) {
	cfg := loadConfig(t, "legacy-parquet.yaml")

	assert.Equal(t, "parquet", cfg.EncodingFileExtension)
	require.NotNil(t, cfg.Encoding)
	assert.Equal(t, "parquet_log_encoding/awss3/destination-parquet", cfg.Encoding.String())
}

func TestLoadConfig_LegacyPartitionValidation(t *testing.T) {
	_, err := loadConfigFromYAML(t, `receivers:
  nop:

exporters:
  awss3:
    s3uploader:
      region: us-east-1
      s3_bucket: sawmills-test
      s3_partition: fortnight

processors:
  nop:

service:
  pipelines:
    traces:
      receivers: [nop]
      processors: [nop]
      exporters: [awss3]
`)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid s3_partition")
}

func TestLoadConfig_LegacyMalformedTypes(t *testing.T) {
	_, err := loadConfigFromYAML(t, `receivers:
  nop:

exporters:
  awss3:
    s3uploader:
      region: us-east-1
      s3_bucket: sawmills-test
      s3_partition: minute
      s3_prefix: 123456

processors:
  nop:

service:
  pipelines:
    traces:
      receivers: [nop]
      processors: [nop]
      exporters: [awss3]
`)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3uploader.s3_prefix")
}

func TestLoadConfig_LegacyPartitionFormatPrecedence(t *testing.T) {
	cfg, err := loadConfigFromYAML(t, `receivers:
  nop:

exporters:
  awss3:
    s3uploader:
      region: us-east-1
      s3_bucket: sawmills-test
      s3_partition: minute
      s3_partition_format: "%Y/%m/%d/%H"

processors:
  nop:

service:
  pipelines:
    traces:
      receivers: [nop]
      processors: [nop]
      exporters: [awss3]
`)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "%Y/%m/%d/%H", cfg.normalizedS3PartitionFormat())
}

func TestLoadConfig_LegacyTemplateValidation(t *testing.T) {
	_, err := loadConfigFromYAML(t, `receivers:
  nop:

exporters:
  awss3:
    s3uploader:
      region: us-east-1
      s3_bucket: sawmills-test
      s3_key_template: "{{.Prefix"

processors:
  nop:

service:
  pipelines:
    traces:
      receivers: [nop]
      processors: [nop]
      exporters: [awss3]
`)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid s3_key_template")
}

func TestLoadConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[metadata.Type] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(filepath.Join("testdata", "default.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	encoding := component.MustNewIDWithName("foo", "bar")

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		QueueSettings:         queueCfg,
		TimeoutSettings:       timeoutCfg,
		Encoding:              &encoding,
		EncodingFileExtension: "baz",
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfig(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Some(func() exporterhelper.QueueBatchConfig {
		queue := exporterhelper.NewDefaultQueueConfig()
		queue.NumConsumers = 23
		queue.QueueSize = 42
		return queue
	}())

	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 8,
	}

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:              "us-east-1",
			S3Bucket:            "foo",
			S3Prefix:            "bar",
			S3PartitionFormat:   "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			S3PartitionTimezone: "Europe/London",
			Endpoint:            "http://endpoint.com",
			StorageClass:        "STANDARD",
			RetryMode:           DefaultRetryMode,
			RetryMaxAttempts:    DefaultRetryMaxAttempts,
			RetryMaxBackoff:     DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfigS3StorageClass(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_storage_class.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD_IA",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3ACL(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_acl.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			ACL:               "bucket-owner-read",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3ACLDefined(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_canned-acl.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			ACL:               "bucket-owner-full-control",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigForS3CompatibleSystems(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3-compatible-systems.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "alternative-s3-system.example.com",
			S3ForcePathStyle:  true,
			DisableSSL:        true,
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		errExpected error
	}{
		{
			// endpoint overrides region and bucket name.
			name: "valid with endpoint and region",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.Region = "foo"
				return c
			}(),
			errExpected: nil,
		},
		{
			// Endpoint will be built from bucket and region.
			// https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
			name: "valid with S3Bucket and region",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "foo"
				c.S3Uploader.S3Bucket = "bar"
				return c
			}(),
			errExpected: nil,
		},
		{
			name: "missing all",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = ""
				c.S3Uploader.S3Bucket = ""
				c.S3Uploader.Endpoint = ""
				return c
			}(),
			errExpected: multierr.Append(errors.New("region is required"),
				errors.New("bucket or endpoint is required")),
		},
		{
			name: "region only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Region = "foo"
				c.S3Uploader.S3Bucket = ""
				return c
			}(),
			errExpected: errors.New("bucket or endpoint is required"),
		},
		{
			name: "bucket only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.S3Bucket = "foo"
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: errors.New("region is required"),
		},
		{
			name: "endpoint only",
			config: func() *Config {
				c := createDefaultConfig().(*Config)
				c.S3Uploader.Endpoint = "http://example.com"
				c.S3Uploader.Region = ""
				return c
			}(),
			errExpected: errors.New("region is required"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.errExpected, err)
		})
	}
}

func TestMarshallerName(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "marshaler.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "sumo_ic",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "proto")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_proto",
	}, e,
	)
}

func TestCompressionName(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "compression.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "gzip",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "proto")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "none",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_proto",
	}, e,
	)

	e = cfg.Exporters[component.MustNewIDWithName("awss3", "zstd")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Compression:       "zstd",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestResourceAttrsToS3(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_resource-attrs-to-s3.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		MarshalerName: "otlp_json",
		ResourceAttrsToS3: ResourceAttrsToS3{
			S3Bucket: "com.awss3.bucket",
			S3Prefix: "com.awss3.prefix",
		},
	}, e,
	)
}

func TestRetry(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "retry.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)

	assert.Equal(t, &Config{
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD_IA",
			RetryMode:         "standard",
			RetryMaxAttempts:  5,
			RetryMaxBackoff:   30 * time.Second,
		},
		MarshalerName: "otlp_json",
	}, e,
	)
}

func TestConfigS3UniqueKeyFunc(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/33594
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_unique_key_func.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.Default(exporterhelper.NewDefaultQueueConfig())
	timeoutCfg := exporterhelper.NewDefaultTimeoutConfig()

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
			StorageClass:      "STANDARD",
			UniqueKeyFuncName: "uuidv7",
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3BasePrefix(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_base_prefix.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.None[exporterhelper.QueueBatchConfig]()
	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 5 * time.Second,
	}

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "bar",
			S3BasePrefix:      "base/prefix",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
	}, e,
	)
}

func TestConfigS3BasePrefixWithResourceAttrs(t *testing.T) {
	factories, err := otelcoltest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Exporters[factory.Type()] = factory
	cfg, err := otelcoltest.LoadConfigAndValidate(
		filepath.Join("testdata", "config-s3_base_prefix_with_resource_attrs.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	e := cfg.Exporters[component.MustNewID("awss3")].(*Config)
	queueCfg := configoptional.None[exporterhelper.QueueBatchConfig]()
	timeoutCfg := exporterhelper.TimeoutConfig{
		Timeout: 5 * time.Second,
	}

	assert.Equal(t, &Config{
		S3Uploader: S3UploaderConfig{
			Region:            "us-east-1",
			S3Bucket:          "foo",
			S3Prefix:          "default-metric",
			S3BasePrefix:      "environment/prod",
			S3PartitionFormat: "year=%Y/month=%m/day=%d/hour=%H/minute=%M",
			Endpoint:          "http://endpoint.com",
			StorageClass:      "STANDARD",
			RetryMode:         DefaultRetryMode,
			RetryMaxAttempts:  DefaultRetryMaxAttempts,
			RetryMaxBackoff:   DefaultRetryMaxBackoff,
		},
		QueueSettings:   queueCfg,
		TimeoutSettings: timeoutCfg,
		MarshalerName:   "otlp_json",
		ResourceAttrsToS3: ResourceAttrsToS3{
			S3Prefix: "com.awss3.prefix",
		},
	}, e,
	)
}
