// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemorystorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/inmemorystorage"

import (
	"context"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

// Storage is a component-local in-memory storage extension.
type Storage struct {
	st sync.Map
}

var _ storage.Extension = (*Storage)(nil)

func (*Storage) Start(context.Context, component.Host) error {
	return nil
}

func (s *Storage) Shutdown(context.Context) error {
	return nil
}

func (s *Storage) GetClient(context.Context, component.Kind, component.ID, string) (storage.Client, error) {
	return &client{st: &s.st, closed: &atomic.Bool{}}, nil
}
