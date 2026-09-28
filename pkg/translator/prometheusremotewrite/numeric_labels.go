// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"

import (
	"math"
	"strconv"
	"strings"
)

func formatNumericLabel(value float64, settings Settings) string {
	if settings.UseGoFloatFormat {
		// The Go Prometheus client writes both signs of zero as 0.
		if value == 0 {
			return "0"
		}
		return strconv.FormatFloat(value, 'g', -1, 64)
	}
	formatted := strconv.FormatFloat(value, 'f', -1, 64)
	if settings.UseDecimalFloatFormat && !math.IsInf(value, 0) && !math.IsNaN(value) && !strings.Contains(formatted, ".") {
		return formatted + ".0"
	}
	return formatted
}
