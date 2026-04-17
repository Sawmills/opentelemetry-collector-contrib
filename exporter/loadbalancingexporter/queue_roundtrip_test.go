// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestQueueSettingsRoundTripPreservesEnabled(t *testing.T) {
	testCases := []struct {
		name          string
		sendingQueue  map[string]any
		assertionFunc func(t *testing.T, sendingQueue map[string]any)
	}{
		{
			name: "enabled true preserves queue fields",
			sendingQueue: map[string]any{
				"enabled":             true,
				"queue_size":          1000,
				"num_consumers":       2,
				"payload_compression": "zstd",
				"compress_in_memory":  true,
			},
			assertionFunc: func(t *testing.T, sendingQueue map[string]any) {
				require.True(t, sendingQueue["enabled"].(bool))
				require.EqualValues(t, 1000, sendingQueue["queue_size"])
				require.EqualValues(t, 2, sendingQueue["num_consumers"])
				require.EqualValues(t, QueuePayloadCompressionZstd, sendingQueue["payload_compression"])
				require.True(t, sendingQueue["compress_in_memory"].(bool))
			},
		},
		{
			name: "enabled false clears queue fields",
			sendingQueue: map[string]any{
				"enabled":             false,
				"queue_size":          1000,
				"num_consumers":       2,
				"payload_compression": "zstd",
				"compress_in_memory":  true,
			},
			assertionFunc: func(t *testing.T, sendingQueue map[string]any) {
				require.False(t, sendingQueue["enabled"].(bool))
				require.Empty(t, sendingQueue["payload_compression"])
				require.False(t, sendingQueue["compress_in_memory"].(bool))
				require.NotContains(t, sendingQueue, "queue_size")
				require.NotContains(t, sendingQueue, "num_consumers")
			},
		},
		{
			name: "missing enabled defaults to false",
			sendingQueue: map[string]any{
				"queue_size":          1000,
				"num_consumers":       2,
				"payload_compression": "zstd",
				"compress_in_memory":  true,
			},
			assertionFunc: func(t *testing.T, sendingQueue map[string]any) {
				require.False(t, sendingQueue["enabled"].(bool))
				require.Empty(t, sendingQueue["payload_compression"])
				require.False(t, sendingQueue["compress_in_memory"].(bool))
				require.NotContains(t, sendingQueue, "queue_size")
				require.NotContains(t, sendingQueue, "num_consumers")
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			conf := confmap.NewFromStringMap(map[string]any{
				"sending_queue": tt.sendingQueue,
			})

			require.NoError(t, conf.Unmarshal(cfg))

			roundTrip := confmap.New()
			require.NoError(t, roundTrip.Marshal(cfg))

			raw := map[string]any{}
			require.NoError(t, roundTrip.Unmarshal(&raw, confmap.WithIgnoreUnused()))

			sendingQueue, ok := raw["sending_queue"].(map[string]any)
			require.True(t, ok, "sending_queue should be preserved")
			tt.assertionFunc(t, sendingQueue)
		})
	}
}
