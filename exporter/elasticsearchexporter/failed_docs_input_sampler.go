// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

const maxFailedDocInputSampleKeys = 1024

var (
	failedDocParseFieldTypeRE = regexp.MustCompile(`failed to parse field \[([^\]]+)\] of type \[([^\]]+)\]`)
	failedDocParseFieldRE     = regexp.MustCompile(`failed to parse field \[([^\]]+)\]`)
	failedDocObjectMappingRE  = regexp.MustCompile(`object mapping for \[([^\]]+)\].* as object`)
)

type failedDocInputSampleKey struct {
	index        string
	errorType    string
	errorField   string
	expectedType string
	status       int
}

func (k failedDocInputSampleKey) String() string {
	return strings.Join([]string{
		k.index,
		k.errorType,
		k.errorField,
		k.expectedType,
		fmt.Sprint(k.status),
	}, "\x00")
}

type failedDocsInputSampler struct {
	interval time.Duration
	now      func() time.Time

	mu      sync.Mutex
	lastLog map[string]time.Time
}

func newFailedDocsInputSampler(config *Config) *failedDocsInputSampler {
	if !config.LogFailedDocsInput {
		return nil
	}
	return &failedDocsInputSampler{
		interval: config.LogFailedDocsInputRateLimit,
		now:      time.Now,
		lastLog:  make(map[string]time.Time),
	}
}

func (s *failedDocsInputSampler) allow(key failedDocInputSampleKey) bool {
	if s == nil || s.interval <= 0 {
		return true
	}

	now := s.now()
	keyString := key.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	last, ok := s.lastLog[keyString]
	if ok && now.Sub(last) < s.interval {
		return false
	}
	s.lastLog[keyString] = now
	if len(s.lastLog) > maxFailedDocInputSampleKeys {
		s.pruneLocked(now)
	}
	return true
}

func (s *failedDocsInputSampler) pruneLocked(now time.Time) {
	maxAge := s.interval * 10
	for key, last := range s.lastLog {
		if now.Sub(last) > maxAge {
			delete(s.lastLog, key)
		}
	}
	if len(s.lastLog) <= maxFailedDocInputSampleKeys {
		return
	}
	for key := range s.lastLog {
		delete(s.lastLog, key)
		if len(s.lastLog) <= maxFailedDocInputSampleKeys {
			return
		}
	}
}

func parseFailedDocErrorReason(reason string) (field, expectedType string) {
	if matches := failedDocParseFieldTypeRE.FindStringSubmatch(reason); matches != nil {
		return matches[1], matches[2]
	}
	if matches := failedDocObjectMappingRE.FindStringSubmatch(reason); matches != nil {
		return matches[1], "object"
	}
	if matches := failedDocParseFieldRE.FindStringSubmatch(reason); matches != nil {
		return matches[1], ""
	}
	return "", ""
}

func failedDocInputHash(input string) string {
	doc := failedDocDocumentLine(input)
	sum := sha256.Sum256([]byte(doc))
	return hex.EncodeToString(sum[:])
}

func failedDocDocumentLine(input string) string {
	firstNewline := strings.IndexByte(input, '\n')
	if firstNewline < 0 || firstNewline == len(input)-1 {
		return strings.TrimSpace(input)
	}
	return strings.TrimSpace(input[firstNewline+1:])
}
