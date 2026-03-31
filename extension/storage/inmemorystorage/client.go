// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemorystorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/inmemorystorage"

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/extension/xextension/storage"
)

type client struct {
	st     *sync.Map
	closed *atomic.Bool
}

func (c *client) Get(ctx context.Context, key string) ([]byte, error) {
	getOp := storage.GetOperation(key)
	err := c.Batch(ctx, getOp)
	return getOp.Value, err
}

func (c *client) Set(ctx context.Context, key string, value []byte) error {
	return c.Batch(ctx, storage.SetOperation(key, value))
}

func (c *client) Delete(ctx context.Context, key string) error {
	return c.Batch(ctx, storage.DeleteOperation(key))
}

func (c *client) Close(context.Context) error {
	c.closed.Store(true)
	return nil
}

func (c *client) Batch(_ context.Context, ops ...*storage.Operation) error {
	if c.IsClosed() {
		panic("client already closed")
	}
	for _, op := range ops {
		switch op.Type {
		case storage.Get:
			val, found := c.st.Load(op.Key)
			if !found {
				break
			}
			op.Value = val.([]byte)
		case storage.Set:
			c.st.Store(op.Key, op.Value)
		case storage.Delete:
			c.st.Delete(op.Key)
		default:
			return errors.New("wrong operation type")
		}
	}
	return nil
}

func (c *client) IsClosed() bool {
	return c.closed.Load()
}
