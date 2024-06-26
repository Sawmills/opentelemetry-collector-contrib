// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package groupbytraceprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessorMetrics(t *testing.T) {
	expectedViewNames := []string{
		"processor_groupbytrace_processor_groupbytrace_conf_num_traces",
		"processor_groupbytrace_processor_groupbytrace_num_events_in_queue",
		"processor_groupbytrace_processor_groupbytrace_num_traces_in_memory",
		"processor_groupbytrace_processor_groupbytrace_traces_evicted",
		"processor_groupbytrace_processor_groupbytrace_spans_released",
		"processor_groupbytrace_processor_groupbytrace_traces_released",
		"processor_groupbytrace_processor_groupbytrace_incomplete_releases",
		"processor_groupbytrace_processor_groupbytrace_event_latency",
	}

	views := metricViews()
	for i, viewName := range expectedViewNames {
		assert.Equal(t, viewName, views[i].Name)
	}
}
