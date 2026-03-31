// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemorystorage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xstorage "go.opentelemetry.io/collector/extension/xextension/storage"
)

func TestClientBatchAfterCloseReturnsError(t *testing.T) {
	c := &client{
		st:     &sync.Map{},
		closed: &atomic.Bool{},
	}

	require.NoError(t, c.Close(t.Context()))
	err := c.Batch(t.Context(), xstorage.SetOperation("key", []byte("value")))
	require.Error(t, err)
	assert.EqualError(t, err, "client already closed")
}
