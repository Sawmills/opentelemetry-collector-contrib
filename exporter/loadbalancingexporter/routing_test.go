// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
