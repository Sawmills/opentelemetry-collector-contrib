// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapters

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
)

type TagValueParser func(string) (any, bool)

func StatusFromSeverityNumber(severity plog.SeverityNumber) string {
	if severity == 0 {
		return ""
	}

	switch {
	case severity <= 4:
		return "trace"
	case severity <= 8:
		return "debug"
	case severity <= 12:
		return "info"
	case severity <= 16:
		return "warn"
	case severity <= 20:
		return "error"
	case severity <= 24:
		return "fatal"
	default:
		return "error"
	}
}

func TagsToMap(tags []string, trimTag bool, parseValue TagValueParser) map[string]any {
	tagMap := make(map[string]any, len(tags))
	for _, rawTag := range tags {
		tag := rawTag
		if trimTag {
			tag = strings.TrimSpace(tag)
		}

		parts := strings.SplitN(tag, ":", 2)
		if len(parts) != 2 {
			continue
		}

		value := parts[1]
		switch {
		case value == "true":
			tagMap[parts[0]] = true
		case value == "false":
			tagMap[parts[0]] = false
		case parseValue != nil:
			if parsedValue, ok := parseValue(value); ok {
				tagMap[parts[0]] = parsedValue
				continue
			}
			tagMap[parts[0]] = value
		default:
			tagMap[parts[0]] = value
		}
	}
	return tagMap
}
