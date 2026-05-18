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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"
)

func TestMarshaler(t *testing.T) {
	{
		m, err := awss3marshaler.NewMarshaler("otlp_json")
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "json", m.Format())
	}
	{
		m, err := awss3marshaler.NewMarshaler("otlp_proto")
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "binpb", m.Format())
	}
	{
		m, err := awss3marshaler.NewMarshaler("sumo_ic")
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "json", m.Format())
	}
	{
		m, err := awss3marshaler.NewMarshaler("unknown")
		assert.Error(t, err)
		require.Nil(t, m)
	}
	{
		m, err := awss3marshaler.NewMarshaler("body")
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "txt", m.Format())
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
		m, err := newMarshalerFromEncoding(&id, "myext", host)
		assert.NoError(t, err)
		require.NotNil(t, m)
		assert.Equal(t, "myext", m.Format())
	}
	{
		m, err := newMarshalerFromEncoding(&id, "", componenttest.NewNopHost())
		assert.EqualError(t, err, `unknown encoding "foo"`)
		require.Nil(t, m)
	}
}

func TestMarshalerFromEncodingUnsupportedSignals(t *testing.T) {
	id := component.MustNewID("foo")
	m, err := newMarshalerFromEncoding(&id, "myext", hostWithExtensions{
		encoding: encodingExtension{},
	})
	require.NoError(t, err)

	_, err = m.MarshalLogs(plog.NewLogs())
	require.EqualError(t, err, "configured encoding does not support logs marshaling")

	_, err = m.MarshalTraces(ptrace.NewTraces())
	require.EqualError(t, err, "configured encoding does not support traces marshaling")

	_, err = m.MarshalMetrics(pmetric.NewMetrics())
	require.EqualError(t, err, "configured encoding does not support metrics marshaling")
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

	m, err := newMarshalerFromEncoding(&id, "parquet", host)
	require.NoError(t, err)

	s3m, ok := m.(marshalerWithFlushMetadata)
	require.True(t, ok)

	buf, meta, err := s3m.MarshalLogsWithFlushMetadata(plog.NewLogs())
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), buf)
	assert.Equal(t, "manual", meta.reason)
	assert.Equal(t, completedAt, meta.flushCompletedAt)
}
