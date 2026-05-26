// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunk

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func TestConsumeMetrics(t *testing.T) {
	tests := []struct {
		name             string
		httpResponseCode int
		responseBody     string
		retryAfter       int
		wantErr          bool
		wantPermanentErr bool
		wantThrottleErr  bool
	}{
		{
			name:             "response_forbidden",
			httpResponseCode: http.StatusForbidden,
			responseBody:     `{"text":"Invalid token","code":4}`,
			wantPermanentErr: true,
		},
		{
			name:             "response_bad_request",
			httpResponseCode: http.StatusBadRequest,
			responseBody:     `{"text":"Invalid data format","code":6}`,
			wantPermanentErr: true,
		},
		{
			name:             "response_throttle",
			httpResponseCode: http.StatusTooManyRequests,
			wantThrottleErr:  true,
		},
		{
			name:             "response_throttle_with_header",
			retryAfter:       123,
			httpResponseCode: http.StatusServiceUnavailable,
			responseBody:     `{"text":"Server busy","code":9}`,
			wantThrottleErr:  true,
		},
		{
			name:             "large_batch",
			httpResponseCode: http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				Request: &http.Request{
					URL: &url.URL{Scheme: "http", Host: "splunk.com", Path: "/endpoint"},
				},
				StatusCode: tt.httpResponseCode,
			}
			if tt.responseBody != "" {
				resp.Body = io.NopCloser(strings.NewReader(tt.responseBody))
			}
			if tt.retryAfter != 0 {
				resp.Header = map[string][]string{
					HeaderRetryAfter: {strconv.Itoa(tt.retryAfter)},
				}
			}

			err := HandleHTTPCode(resp)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			if tt.wantPermanentErr {
				assert.Error(t, err)
				assert.True(t, consumererror.IsPermanent(err))
				if tt.responseBody != "" {
					assert.ErrorContains(t, err, "response body: "+tt.responseBody)
				}
				return
			}

			if tt.wantThrottleErr {
				expected := fmt.Errorf("HTTP \"/endpoint\" %d %q", tt.httpResponseCode, http.StatusText(tt.httpResponseCode))
				expected = exporterhelper.NewThrottleRetry(expected, time.Duration(tt.retryAfter)*time.Second)
				assert.EqualError(t, err, expected.Error())
				assert.NotContains(t, err.Error(), "response body:")
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestHandleHTTPCodeLimitsResponseBody(t *testing.T) {
	responseBody := strings.Repeat("a", maxHTTPErrorResponseBodyLen+1)
	resp := &http.Response{
		Request: &http.Request{
			URL: &url.URL{Scheme: "http", Host: "splunk.com", Path: "/endpoint"},
		},
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader(responseBody)),
	}

	err := HandleHTTPCode(resp)
	assert.EqualError(t, err, fmt.Sprintf(
		"Permanent error: HTTP \"/endpoint\" 400 \"Bad Request\": response body: %s",
		strings.Repeat("a", maxHTTPErrorResponseBodyLen),
	))
}

func TestHandleHTTPCodeOmitsResponseBodyForInternalServerError(t *testing.T) {
	resp := &http.Response{
		Request: &http.Request{
			URL: &url.URL{Scheme: "http", Host: "splunk.com", Path: "/endpoint"},
		},
		StatusCode: http.StatusInternalServerError,
		Body:       io.NopCloser(strings.NewReader(`{"text":"Internal server error","code":8}`)),
	}

	err := HandleHTTPCode(resp)
	assert.EqualError(t, err, `HTTP "/endpoint" 500 "Internal Server Error"`)
}
