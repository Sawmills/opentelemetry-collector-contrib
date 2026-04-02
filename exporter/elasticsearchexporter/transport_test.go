// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestOpenSearchCompatibleTransport_AddsProductHeader(t *testing.T) {
	t.Run("adds missing header", func(t *testing.T) {
		transport := &OpenSearchCompatibleTransport{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader("ok")),
				}, nil
			}),
		}

		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)

		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
	})

	t.Run("preserves existing header", func(t *testing.T) {
		transport := &OpenSearchCompatibleTransport{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header: http.Header{
						"X-Elastic-Product": []string{"OpenSearch"},
					},
					Body: io.NopCloser(strings.NewReader("ok")),
				}, nil
			}),
		}

		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)

		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, "OpenSearch", resp.Header.Get("X-Elastic-Product"))
	})

	t.Run("propagates underlying error", func(t *testing.T) {
		wantErr := errors.New("boom")
		transport := &OpenSearchCompatibleTransport{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, wantErr
			}),
		}

		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)

		resp, err := transport.RoundTrip(req)
		require.ErrorIs(t, err, wantErr)
		assert.Nil(t, resp)
	})

	t.Run("initializes nil response headers", func(t *testing.T) {
		transport := &OpenSearchCompatibleTransport{
			Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
				}, nil
			}),
		}

		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)

		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
	})

	t.Run("defaults nil transport", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		defer server.Close()

		transport := &OpenSearchCompatibleTransport{}
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)

		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, "Elasticsearch", resp.Header.Get("X-Elastic-Product"))
	})
}
