// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemorystorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/inmemorystorage"

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	xstorage "go.opentelemetry.io/collector/extension/xextension/storage"
)

type storage struct {
	st sync.Map
}

var _ xstorage.Extension = (*storage)(nil)

func (*storage) Start(context.Context, component.Host) error {
	return nil
}

func (s *storage) Shutdown(context.Context) error {
	return nil
}

func (s *storage) GetClient(context.Context, component.Kind, component.ID, string) (xstorage.Client, error) {
	return &client{st: &s.st, closed: &atomic.Bool{}}, nil
}
