// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"net/http"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	commonAttributesWarnSampleTick       = time.Minute
	commonAttributesWarnSampleFirst      = 1
	commonAttributesWarnSampleThereafter = 100
)

func nextRecordErrorStatus(error) int {
	return http.StatusBadRequest
}

func commonAttributesConfig(config *Config) CommonAttributesConfig {
	if config == nil {
		return CommonAttributesConfig{}
	}
	return config.CommonAttributes
}

func newCommonAttributesLogger(logger *zap.Logger) *zap.Logger {
	if logger == nil {
		return nil
	}
	return logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(
			core,
			commonAttributesWarnSampleTick,
			commonAttributesWarnSampleFirst,
			commonAttributesWarnSampleThereafter,
		)
	}))
}

func attachCommonAttributes(attrs pcommon.Map, commonAttributes map[string]string, cfg CommonAttributesConfig, logger *zap.Logger, warnOnNonMapTarget bool) bool {
	if len(commonAttributes) == 0 {
		return false
	}

	if cfg.MapKey == "" {
		attachMissingCommonAttributes(attrs, commonAttributes)
		return false
	}

	existing, found := attrs.Get(cfg.MapKey)
	if !found {
		attachMissingCommonAttributes(attrs.PutEmptyMap(cfg.MapKey), commonAttributes)
		return false
	}
	if existing.Type() != pcommon.ValueTypeMap {
		if logger != nil && warnOnNonMapTarget {
			logger.Warn("Firehose common attributes target exists and is not a map", zap.String("map_key", cfg.MapKey))
		}
		return true
	}
	attachMissingCommonAttributes(existing.Map(), commonAttributes)
	return false
}

func attachMissingCommonAttributes(attrs pcommon.Map, commonAttributes map[string]string) {
	for k, v := range commonAttributes {
		if _, found := attrs.Get(k); !found {
			attrs.PutStr(k, v)
		}
	}
}
