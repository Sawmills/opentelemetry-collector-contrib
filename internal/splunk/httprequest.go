// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package splunk // import "github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	HeaderRetryAfter            = "Retry-After"
	maxHTTPErrorResponseBodyLen = 8 * 1024
)

// HandleHTTPCode handles an http response and returns the right type of error in case of a failure.
func HandleHTTPCode(resp *http.Response) error {
	// Splunk accepts all 2XX codes.
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}

	err := fmt.Errorf(
		"HTTP %q %d %q",
		resp.Request.URL.Path,
		resp.StatusCode,
		http.StatusText(resp.StatusCode),
	)
	if shouldReadHTTPErrorResponseBody(resp.StatusCode) {
		body := readHTTPErrorResponseBody(resp)
		if body != "" {
			err = fmt.Errorf("%w: response body: %s", err, body)
		}
	}

	switch resp.StatusCode {
	// Check for responses that may include "Retry-After" header.
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		// Fallback to 0 if the Retry-After header is not present. This will trigger the
		// default backoff policy by our caller (retry handler).
		retryAfter := 0
		if val := resp.Header.Get(HeaderRetryAfter); val != "" {
			if seconds, err2 := strconv.Atoi(val); err2 == nil {
				retryAfter = seconds
			}
		}
		// Indicate to our caller to pause for the specified number of seconds.
		err = exporterhelper.NewThrottleRetry(err, time.Duration(retryAfter)*time.Second)
	// Check for permanent errors.
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		err = consumererror.NewPermanent(err)
	}

	return err
}

func shouldReadHTTPErrorResponseBody(statusCode int) bool {
	switch statusCode {
	case http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden:
		return true
	default:
		return false
	}
}

func readHTTPErrorResponseBody(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPErrorResponseBodyLen))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}
