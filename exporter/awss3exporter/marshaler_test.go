// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func TestMarshaler(t *testing.T) {
	{
		m, err := newMarshaler("otlp_json", zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "json", m.format())
	}
	{
		m, err := newMarshaler("otlp_proto", zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "binpb", m.format())
	}
	{
		m, err := newMarshaler("sumo_ic", zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "json", m.format())
	}
	{
		m, err := newMarshaler("unknown", zap.NewNop())
		assert.Error(t, err)
		require.Nil(t, m)
	}
	{
		m, err := newMarshaler("body", zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "txt", m.format())
	}
}

type hostWithExtensions struct {
	encoding component.Component
}

func (h hostWithExtensions) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{
		component.MustNewID("foo"): h.encoding,
	}
}

type encodingExtension struct{}

func (encodingExtension) Start(context.Context, component.Host) error {
	panic("unsupported")
}

func (encodingExtension) Shutdown(context.Context) error {
	panic("unsupported")
}

func TestMarshalerFromEncoding(t *testing.T) {
	id := component.MustNewID("foo")

	{
		host := hostWithExtensions{
			encoding: encodingExtension{},
		}
		m, err := newMarshalerFromEncoding(&id, "myext", host, zap.NewNop())
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "myext", m.format())
	}
	{
		m, err := newMarshalerFromEncoding(&id, "", componenttest.NewNopHost(), zap.NewNop())
		assert.EqualError(t, err, `unknown encoding "foo"`)
		require.Nil(t, m)
	}
}

type encodingExtensionWithFlushMetadata struct {
	buf         []byte
	reason      string
	completedAt time.Time
}

func (encodingExtensionWithFlushMetadata) Start(context.Context, component.Host) error {
	panic("unsupported")
}

func (encodingExtensionWithFlushMetadata) Shutdown(context.Context) error {
	panic("unsupported")
}

func (e encodingExtensionWithFlushMetadata) MarshalLogs(plog.Logs) ([]byte, error) {
	return e.buf, nil
}

func (e encodingExtensionWithFlushMetadata) MarshalLogsWithFlushMetadata(
	plog.Logs,
) ([]byte, string, time.Time, error) {
	return e.buf, e.reason, e.completedAt, nil
}

func TestMarshalLogsWithFlushMetadata_FromEncodingExtension(t *testing.T) {
	id := component.MustNewID("foo")
	completedAt := time.Now()
	host := hostWithExtensions{
		encoding: encodingExtensionWithFlushMetadata{
			buf:         []byte("payload"),
			reason:      "manual",
			completedAt: completedAt,
		},
	}

	m, err := newMarshalerFromEncoding(&id, "parquet", host, zap.NewNop())
	require.NoError(t, err)

	s3m, ok := m.(*s3Marshaler)
	require.True(t, ok)

	buf, meta, err := s3m.MarshalLogsWithFlushMetadata(plog.NewLogs())
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), buf)
	assert.Equal(t, "manual", meta.reason)
	assert.Equal(t, completedAt, meta.flushCompletedAt)
}
