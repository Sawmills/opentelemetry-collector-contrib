// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import "net/http"

// openSearchCompatibleTransport spoofs the header Elasticsearch clients expect.
type openSearchCompatibleTransport struct {
	Transport http.RoundTripper
}

func (t *openSearchCompatibleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	if resp.Header.Get("X-Elastic-Product") == "" {
		resp.Header.Set("X-Elastic-Product", "Elasticsearch")
	}
	return resp, nil
}
