// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

var typ = component.MustNewType("parquet_log_encoding")

func TestComponentFactoryType(t *testing.T) {
	require.Equal(t, typ, NewFactory().Type())
}

func TestComponentConfigStruct(t *testing.T) {
	require.NoError(t, componenttest.CheckConfigStruct(NewFactory().CreateDefaultConfig()))
}

func TestComponentLifecycle(t *testing.T) {
	factory := NewFactory()

	cm, err := confmaptest.LoadConf("metadata.yaml")
	require.NoError(t, err)
	cfg := factory.CreateDefaultConfig()
	sub, err := cm.Sub("tests::config")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(&cfg))

	ext, err := factory.Create(t.Context(), extensiontest.NewNopSettings(typ), cfg)
	require.NoError(t, err)
	require.NoError(t, ext.Start(t.Context(), componenttest.NewNopHost()))
	require.NoError(t, ext.Shutdown(t.Context()))
}
