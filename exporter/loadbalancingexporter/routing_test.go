// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func findRoutingIDForEndpoint(t *testing.T, ring *hashRing, endpoint string) string {
	t.Helper()

	for i := range 4096 {
		routingID := fmt.Sprintf("routing-id-%d", i)
		if ring.endpointFor([]byte(routingID)) == endpoint {
			return routingID
		}
	}

	require.FailNow(t, "failed to find routing id for endpoint", "endpoint=%s", endpoint)
	return ""
}

func findTraceIDForEndpoint(t *testing.T, ring *hashRing, endpoint string) pcommon.TraceID {
	t.Helper()

	for i := range 4096 {
		var traceID pcommon.TraceID
		traceID[14] = byte(i >> 8)
		traceID[15] = byte(i)
		if ring.endpointFor(traceID[:]) == endpoint {
			return traceID
		}
	}

	require.FailNow(t, "failed to find trace id for endpoint", "endpoint=%s", endpoint)
	return pcommon.NewTraceIDEmpty()
}
