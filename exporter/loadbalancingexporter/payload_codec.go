// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper"
)

var (
	errInvalidCompressedPayload      = errors.New("invalid compressed queue payload")
	queuePayloadMagic                = [3]byte{'s', 'q', 'c'}
	queuePayloadVersion         byte = 1
)

const (
	queuePayloadCodecNone   byte = 0
	queuePayloadCodecSnappy byte = 1
	queuePayloadCodecZstd   byte = 2
)

type queuePayloadCodec struct {
	compression QueuePayloadCompression
	zstdOnce    sync.Once
	closeOnce   sync.Once
	zstdEnc     *zstd.Encoder
	zstdDec     *zstd.Decoder
	zstdErr     error
}

func newQueuePayloadCodec(compression QueuePayloadCompression) *queuePayloadCodec {
	return &queuePayloadCodec{compression: compression}
}

func (c *queuePayloadCodec) Encode(payload []byte) ([]byte, error) {
	codecID, err := codecIDForCompression(c.compression)
	if err != nil {
		return nil, err
	}

	compressed, err := c.compress(codecID, payload)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, len(compressed)+5)
	out = append(out, queuePayloadMagic[:]...)
	out = append(out, queuePayloadVersion, codecID)
	out = append(out, compressed...)
	return out, nil
}

func (c *queuePayloadCodec) Decode(payload []byte) ([]byte, error) {
	if len(payload) < 5 {
		if c.compression == QueuePayloadCompressionNone {
			return payload, nil
		}
		return nil, errInvalidCompressedPayload
	}
	if payload[0] != queuePayloadMagic[0] || payload[1] != queuePayloadMagic[1] || payload[2] != queuePayloadMagic[2] {
		if c.compression == QueuePayloadCompressionNone {
			return payload, nil
		}
		return nil, errInvalidCompressedPayload
	}
	if payload[3] != queuePayloadVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", errInvalidCompressedPayload, payload[3])
	}

	return c.decompress(payload[4], payload[5:])
}

func (c *queuePayloadCodec) compress(codecID byte, payload []byte) ([]byte, error) {
	switch codecID {
	case queuePayloadCodecNone:
		return payload, nil
	case queuePayloadCodecSnappy:
		return snappy.Encode(nil, payload), nil
	case queuePayloadCodecZstd:
		if err := c.initZstd(); err != nil {
			return nil, err
		}
		return c.zstdEnc.EncodeAll(payload, nil), nil
	default:
		return nil, fmt.Errorf("unsupported queue payload codec %d", codecID)
	}
}

func (c *queuePayloadCodec) decompress(codecID byte, payload []byte) ([]byte, error) {
	switch codecID {
	case queuePayloadCodecNone:
		return payload, nil
	case queuePayloadCodecSnappy:
		return snappy.Decode(nil, payload)
	case queuePayloadCodecZstd:
		if err := c.initZstd(); err != nil {
			return nil, err
		}
		return c.zstdDec.DecodeAll(payload, nil)
	default:
		return nil, fmt.Errorf("%w: unsupported codec %d", errInvalidCompressedPayload, codecID)
	}
}

func (c *queuePayloadCodec) initZstd() error {
	c.zstdOnce.Do(func() {
		c.zstdEnc, c.zstdErr = zstd.NewWriter(nil)
		if c.zstdErr != nil {
			return
		}
		c.zstdDec, c.zstdErr = zstd.NewReader(nil)
		if c.zstdErr != nil {
			if closeErr := c.zstdEnc.Close(); closeErr != nil {
				c.zstdErr = errors.Join(c.zstdErr, closeErr)
			}
			c.zstdEnc = nil
		}
	})
	return c.zstdErr
}

func (c *queuePayloadCodec) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.zstdDec != nil {
			c.zstdDec.Close()
		}
		if c.zstdEnc != nil {
			closeErr = c.zstdEnc.Close()
		}
	})
	return closeErr
}

func codecIDForCompression(compression QueuePayloadCompression) (byte, error) {
	switch compression {
	case QueuePayloadCompressionNone:
		return queuePayloadCodecNone, nil
	case QueuePayloadCompressionSnappy:
		return queuePayloadCodecSnappy, nil
	case QueuePayloadCompressionZstd:
		return queuePayloadCodecZstd, nil
	default:
		return 0, fmt.Errorf("unsupported queue payload compression %q", compression)
	}
}

type requestEncoding interface {
	Marshal(context.Context, xexporterhelper.Request) ([]byte, error)
	Unmarshal([]byte) (context.Context, xexporterhelper.Request, error)
}

type compressedRequestEncoding struct {
	next  requestEncoding
	codec *queuePayloadCodec
}

func newCompressedRequestEncoding(next requestEncoding, codec *queuePayloadCodec) requestEncoding {
	if codec == nil {
		return next
	}
	return compressedRequestEncoding{
		next:  next,
		codec: codec,
	}
}

func (e compressedRequestEncoding) Marshal(ctx context.Context, req xexporterhelper.Request) ([]byte, error) {
	payload, err := e.next.Marshal(ctx, req)
	if err != nil {
		return nil, err
	}
	return e.codec.Encode(payload)
}

func (e compressedRequestEncoding) Unmarshal(payload []byte) (context.Context, xexporterhelper.Request, error) {
	decoded, err := e.codec.Decode(payload)
	if err != nil {
		return nil, nil, err
	}
	return e.next.Unmarshal(decoded)
}
