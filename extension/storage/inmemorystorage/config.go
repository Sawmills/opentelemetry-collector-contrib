// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inmemorystorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/inmemorystorage"

import "go.opentelemetry.io/collector/component"

// Config defines the in-memory storage extension configuration.
type Config struct{}

var _ component.Config = (*Config)(nil)
