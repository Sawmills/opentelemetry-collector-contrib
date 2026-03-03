// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueuePayloadCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		compression QueuePayloadCompression
	}{
		{name: "none", compression: QueuePayloadCompressionNone},
		{name: "snappy", compression: QueuePayloadCompressionSnappy},
		{name: "zstd", compression: QueuePayloadCompressionZstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := newQueuePayloadCodec(tt.compression)
			original := []byte("hello compressed queue payload")

			encoded, err := codec.Encode(original)
			require.NoError(t, err)

			decoded, err := codec.Decode(encoded)
			require.NoError(t, err)
			require.Equal(t, original, decoded)
		})
	}
}

func TestQueuePayloadCodecDecodeRejectsInvalidPayload(t *testing.T) {
	codec := newQueuePayloadCodec(QueuePayloadCompressionSnappy)

	_, err := codec.Decode([]byte("bad"))
	require.Error(t, err)

	encoded, err := codec.Encode([]byte("hello"))
	require.NoError(t, err)

	encoded[0] = 'x'
	_, err = codec.Decode(encoded)
	require.Error(t, err)

	encoded, err = codec.Encode([]byte("hello"))
	require.NoError(t, err)

	encoded[3] = 0xFF
	_, err = codec.Decode(encoded)
	require.ErrorContains(t, err, "unsupported version")
}

func TestQueuePayloadCodecNoneDecodeLegacyRawPayload(t *testing.T) {
	codec := newQueuePayloadCodec(QueuePayloadCompressionNone)
	raw := []byte("legacy-raw-payload")

	decoded, err := codec.Decode(raw)
	require.NoError(t, err)
	require.Equal(t, raw, decoded)
}

func TestQueuePayloadCodecNoneDecodeCompressedPayload(t *testing.T) {
	snappyCodec := newQueuePayloadCodec(QueuePayloadCompressionSnappy)
	noneCodec := newQueuePayloadCodec(QueuePayloadCompressionNone)
	original := []byte("payload written before compression mode switched to none")

	encoded, err := snappyCodec.Encode(original)
	require.NoError(t, err)

	decoded, err := noneCodec.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}
