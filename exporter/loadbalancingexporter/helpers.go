// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// mergeTraces concatenates two ptrace.Traces into a single ptrace.Traces.
func mergeTraces(t1, t2 ptrace.Traces) ptrace.Traces {
	t2.ResourceSpans().MoveAndAppendTo(t1.ResourceSpans())
	return t1
}

// mergeLogs combines two plog.Logs into a single plog.Logs while reusing
// matching resource/scope structures so serialized size reflects the true
// merged OTLP payload.
func mergeLogs(l1, l2 plog.Logs) plog.Logs {
	for i := 0; i < l2.ResourceLogs().Len(); i++ {
		srcRL := l2.ResourceLogs().At(i)
		for j := 0; j < srcRL.ScopeLogs().Len(); j++ {
			srcSL := srcRL.ScopeLogs().At(j)
			for k := 0; k < srcSL.LogRecords().Len(); k++ {
				insertLogRecord(l1, srcRL, srcSL, srcSL.LogRecords().At(k))
			}
		}
	}
	return l1
}
