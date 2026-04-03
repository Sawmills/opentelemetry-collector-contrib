// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package xk8stest

import (
	"net/netip"
	"testing"

	networktypes "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
)

func TestHostEndpointFromIPAMConfigs(t *testing.T) {
	t.Run("prefers ipv4 gateway", func(t *testing.T) {
		endpoint, ok := hostEndpointFromIPAMConfigs([]networktypes.IPAMConfig{
			{Gateway: netip.MustParseAddr("fd00::1")},
			{Gateway: netip.MustParseAddr("172.18.0.1")},
		})
		require.True(t, ok)
		require.Equal(t, "172.18.0.1", endpoint)
	})

	t.Run("falls back to bracketed ipv6 gateway", func(t *testing.T) {
		endpoint, ok := hostEndpointFromIPAMConfigs([]networktypes.IPAMConfig{
			{Gateway: netip.MustParseAddr("fd00::1")},
		})
		require.True(t, ok)
		require.Equal(t, "[fd00::1]", endpoint)
	})
}
