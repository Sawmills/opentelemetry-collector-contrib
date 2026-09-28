// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite

import "strconv"

func formatNumericLabel(value float64, useGoFloatFormat bool) string {
	format := byte('f')
	if useGoFloatFormat {
		// The Go Prometheus client writes both signs of zero as 0.
		if value == 0 {
			return "0"
		}
		format = 'g'
	}
	return strconv.FormatFloat(value, format, -1, 64)
}
