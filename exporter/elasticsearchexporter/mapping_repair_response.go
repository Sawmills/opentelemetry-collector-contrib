// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

// The dependency decoder treats a missing item status as indexed. Require a
// complete, positively stated outcome before any opt-in repair acceptance metric.
type mappingResponseClient struct {
	next  elastictransport.Interface
	count int
}

func (c *mappingResponseClient) Perform(request *http.Request) (*http.Response, error) {
	response, err := c.next.Perform(request)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		return nil, errors.New("bulk request transport failed")
	}
	if response == nil {
		return nil, errors.New("bulk request returned no response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.Body != nil {
			response.Body.Close()
		}
		// Request-level errors can echo document values. Preserve the status
		// for ordinary retry policy without returning the upstream body.
		body := []byte(`{"error":"bulk request failed"}`)
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		return response, nil
	}
	if response.Body == nil {
		return nil, consumererror.NewPermanent(errors.New("bulk item response has no body"))
	}
	const maxResponseBytes = 16 << 20
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	response.Body.Close()
	if readErr != nil || len(body) > maxResponseBytes || !c.valid(body) {
		return nil, consumererror.NewPermanent(errors.New("bulk item response is incomplete or uncorrelated"))
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}

func (c *mappingResponseClient) valid(body []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if !uniqueResponseValue(decoder, 0) {
		return false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	var result struct {
		Items []map[string]struct {
			Status *int            `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	if json.Unmarshal(body, &result) != nil || len(result.Items) != c.count {
		return false
	}
	for _, item := range result.Items {
		if len(item) != 1 {
			return false
		}
		for action, outcome := range item {
			if (action != "create" && action != "index") || outcome.Status == nil {
				return false
			}
			status := *outcome.Status
			if status == 200 || status == 201 {
				if len(outcome.Error) > 0 && string(outcome.Error) != "null" {
					return false
				}
			} else if status < 400 || status > 599 {
				return false
			}
		}
	}
	return true
}

// Different JSON decoders can select different duplicate members. Reject that
// ambiguity before both validation and the dependency's acceptance accounting.
func uniqueResponseValue(decoder *json.Decoder, depth int) bool {
	if depth > 128 {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]struct{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return false
			}
			name, ok := key.(string)
			if !ok {
				return false
			}
			if _, duplicate := seen[name]; duplicate {
				return false
			}
			seen[name] = struct{}{}
			if !uniqueResponseValue(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim('}')
	case json.Delim('['):
		for decoder.More() {
			if !uniqueResponseValue(decoder, depth+1) {
				return false
			}
		}
		end, err := decoder.Token()
		return err == nil && end == json.Delim(']')
	default:
		_, delimiter := token.(json.Delim)
		return !delimiter
	}
}
