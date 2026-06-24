// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFailedDocsInputSampler(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	sampler := &failedDocsInputSampler{
		interval: time.Minute,
		now:      func() time.Time { return now },
		lastLog:  make(map[string]time.Time),
	}

	key := failedDocInputSampleKey{index: "logs-a", errorType: "mapper_parsing_exception", status: 400}
	assert.True(t, sampler.allow(key))
	assert.False(t, sampler.allow(key))

	assert.True(t, sampler.allow(failedDocInputSampleKey{index: "logs-b", errorType: "mapper_parsing_exception", status: 400}))

	now = now.Add(time.Minute)
	assert.True(t, sampler.allow(key))
}

func TestParseFailedDocErrorReason(t *testing.T) {
	tests := []struct {
		name             string
		reason           string
		wantField        string
		wantExpectedType string
	}{
		{
			name:             "field and type",
			reason:           `failed to parse field [http.response.status_code] of type [long] in document with id 'abc'`,
			wantField:        "http.response.status_code",
			wantExpectedType: "long",
		},
		{
			name:             "object mapping",
			reason:           `object mapping for [host] tried to parse field [host] as object, but found a concrete value`,
			wantField:        "host",
			wantExpectedType: "object",
		},
		{
			name:      "field only",
			reason:    `failed to parse field [message]`,
			wantField: "message",
		},
		{
			name:   "unknown",
			reason: `some other indexing failure`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotField, gotExpectedType := parseFailedDocErrorReason(tt.reason)
			assert.Equal(t, tt.wantField, gotField)
			assert.Equal(t, tt.wantExpectedType, gotExpectedType)
		})
	}
}

func TestFailedDocInputHashUsesDocumentLine(t *testing.T) {
	actionAndDoc := "{\"create\":{\"_index\":\"foo\"}}\n{\"foo\":\"bar\"}\n"
	otherActionSameDoc := "{\"create\":{\"_index\":\"bar\"}}\n{\"foo\":\"bar\"}\n"
	otherDoc := "{\"create\":{\"_index\":\"foo\"}}\n{\"foo\":\"baz\"}\n"

	assert.Equal(t, failedDocInputHash(actionAndDoc), failedDocInputHash(otherActionSameDoc))
	assert.NotEqual(t, failedDocInputHash(actionAndDoc), failedDocInputHash(otherDoc))
}
