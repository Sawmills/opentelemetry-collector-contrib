// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import "net/http"

// OpenSearchCompatibleTransport spoofs the header Elasticsearch clients expect.
type OpenSearchCompatibleTransport struct {
	Transport http.RoundTripper
}

func (t *OpenSearchCompatibleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.Transport.RoundTrip(req)
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
