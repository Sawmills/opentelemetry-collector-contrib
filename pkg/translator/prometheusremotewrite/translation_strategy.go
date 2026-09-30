// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import (
	"fmt"
	"strings"

	"github.com/prometheus/otlptranslator"
)

// ValidateTranslationStrategy rejects naming modes the pinned translator cannot safely support.
func ValidateTranslationStrategy(strategy otlptranslator.TranslationStrategyOption) error {
	switch strategy {
	case "", otlptranslator.UnderscoreEscapingWithSuffixes, otlptranslator.UnderscoreEscapingWithoutSuffixes, otlptranslator.NoTranslation:
		return nil
	case otlptranslator.NoUTF8EscapingWithSuffixes:
		return fmt.Errorf("unsupported translation_strategy: %s (pinned translator can truncate metric names)", strategy)
	default:
		return fmt.Errorf("invalid translation_strategy: %s", strategy)
	}
}

// BuildLabelName preserves valid underscore-only names in UTF-8 mode.
func BuildLabelName(name string, namer otlptranslator.LabelNamer) (string, error) {
	if namer.UTF8Allowed && name != "" && strings.Trim(name, "_") == "" {
		return name, nil
	}
	return namer.Build(name)
}
