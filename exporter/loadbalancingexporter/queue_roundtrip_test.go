// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestQueueSettingsRoundTripPreservesEnabled(t *testing.T) {
	cfg := &Config{}
	conf := confmap.NewFromStringMap(map[string]any{
		"sending_queue": map[string]any{
			"enabled":             true,
			"queue_size":          1000,
			"num_consumers":       2,
			"payload_compression": "zstd",
			"compress_in_memory":  true,
		},
	})

	require.NoError(t, conf.Unmarshal(cfg))

	roundTrip := confmap.New()
	require.NoError(t, roundTrip.Marshal(cfg))

	raw := map[string]any{}
	require.NoError(t, roundTrip.Unmarshal(&raw, confmap.WithIgnoreUnused()))

	sendingQueue, ok := raw["sending_queue"].(map[string]any)
	require.True(t, ok, "sending_queue should be preserved")
	require.Equal(t, true, sendingQueue["enabled"])
}
