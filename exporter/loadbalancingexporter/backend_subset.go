// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
)

type backendSubsetSelector struct {
	seed         string
	maxEndpoints int
}

type backendSubsetRank struct {
	endpoint string
	score    [sha256.Size]byte
}

func newBackendSubsetSelector(cfg BackendSubsetConfig) (*backendSubsetSelector, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	var seed string
	if cfg.Seed != nil {
		seed = strings.TrimSpace(*cfg.Seed)
	} else {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolve backend_subset seed from hostname: %w", err)
		}
		seed = strings.TrimSpace(hostname)
	}
	if seed == "" {
		return nil, errors.New("backend_subset seed is empty")
	}

	return &backendSubsetSelector{
		seed:         seed,
		maxEndpoints: cfg.MaxEndpoints,
	}, nil
}

func (s backendSubsetSelector) selectEndpoints(endpoints []string) []string {
	normalized := normalizeEndpoints(endpoints)
	if len(normalized) <= s.maxEndpoints {
		return normalized
	}
	if s.maxEndpoints <= 0 {
		return nil
	}

	ranked := make([]backendSubsetRank, 0, len(normalized))
	for _, endpoint := range normalized {
		ranked = append(ranked, backendSubsetRank{
			endpoint: endpoint,
			score:    s.score(endpoint),
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if comparison := bytes.Compare(ranked[i].score[:], ranked[j].score[:]); comparison != 0 {
			return comparison > 0
		}
		return ranked[i].endpoint < ranked[j].endpoint
	})

	selected := make([]string, 0, s.maxEndpoints)
	for _, rank := range ranked[:s.maxEndpoints] {
		selected = append(selected, rank.endpoint)
	}
	sort.Strings(selected)
	return selected
}

func (s backendSubsetSelector) score(endpoint string) [sha256.Size]byte {
	return sha256.Sum256([]byte(s.seed + "\x00" + backendSubsetHost(endpoint)))
}

func backendSubsetHost(endpoint string) string {
	normalized := endpointWithPort(endpoint)
	host, _, err := net.SplitHostPort(normalized)
	if err != nil {
		return normalized
	}
	return strings.Trim(host, "[]")
}
