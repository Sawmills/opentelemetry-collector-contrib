// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"go.opentelemetry.io/collector/extension/extensionmiddleware"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

// Match the pinned OTLP/HTTP exporter's response read limit. An oversized or
// unparseable acknowledgement cannot authorize reclaiming source records.
const centralQueueMaxHTTPAckBytes = 64 * 1024

func (m centralQueueAckMiddleware) GetHTTPRoundTripper(context.Context) (extensionmiddleware.WrapHTTPRoundTripperFunc, error) {
	if m.signal != signalKindLogs {
		return nil, errors.New("HTTP central queue acknowledgement requires logs")
	}
	return func(_ context.Context, next http.RoundTripper) (http.RoundTripper, error) {
		return centralQueueHTTPAckTransport{next: next}, nil
	}, nil
}

type centralQueueHTTPAckTransport struct {
	next http.RoundTripper
}

func (t centralQueueHTTPAckTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	acknowledgement, ok := req.Context().Value(centralQueueAckContextKey{}).(*centralQueueOTLPAck)
	if !ok {
		return nil, errors.New("central queue HTTP response has no acknowledgement owner")
	}
	response, err := t.next.RoundTrip(req)
	if err != nil || response.StatusCode < 200 || response.StatusCode > 299 {
		return response, err
	}
	if response.Body == nil {
		acknowledgement.localErr = errors.New("missing central queue HTTP acknowledgement body")
		return response, nil
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, centralQueueMaxHTTPAckBytes+1))
	closeErr := response.Body.Close()
	// The standard exporter still reads and validates its original response.
	response.Body = io.NopCloser(bytes.NewReader(body))
	if readErr != nil || closeErr != nil {
		acknowledgement.localErr = errors.Join(readErr, closeErr)
		return response, nil
	}
	if len(body) > centralQueueMaxHTTPAckBytes {
		acknowledgement.localErr = errors.New("central queue HTTP acknowledgement exceeds response limit")
		return response, nil
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		acknowledgement.localErr = err
		return response, nil
	}
	exportResponse := plogotlp.NewExportResponse()
	switch contentType {
	case "application/x-protobuf":
		err = exportResponse.UnmarshalProto(body)
	case "application/json":
		// pdata does not require the entire response to be a JSON object.
		if !json.Valid(body) || !bytes.HasPrefix(bytes.TrimSpace(body), []byte("{")) {
			err = errors.New("invalid central queue JSON acknowledgement")
		} else {
			err = exportResponse.UnmarshalJSON(body)
		}
	default:
		err = errors.New("unsupported central queue HTTP acknowledgement content type")
	}
	if err != nil {
		acknowledgement.localErr = err
		return response, nil
	}
	acknowledgement.rejected = exportResponse.PartialSuccess().RejectedLogRecords()
	acknowledgement.observed = true
	return response, nil
}

func (t centralQueueHTTPAckTransport) CloseIdleConnections() {
	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
