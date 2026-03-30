// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"encoding/binary"
	"hash/crc32"
	"sort"
)

const (
	maxPositions     uint32 = 36000 // 360 degrees with two decimal places
	defaultWeight    int    = 100   // the number of points in the ring for each entry. For better results, it should be greater than 100.
	linearProbeLimit int    = 10    // The number of times to probe ahead in the hash ring if there is a collision while constructing the hash ring
)

// position represents a specific angle in the ring.
// Each entry in the ring is positioned at an angle in a hypothetical circle, meaning that it ranges from 0 to 360.
type position uint32

// ringItem connects a specific angle in the ring with a specific endpoint.
type ringItem struct {
	pos      position
	endpoint string
}

// hashRing is a consistent hash ring following Karger et al.
type hashRing struct {
	// ringItems holds all the positions, used for the lookup the position for the closest next ring item
	items []ringItem
}

// newHashRing builds a new immutable consistent hash ring based on the given endpoints.
func newHashRing(endpoints []string) *hashRing {
	items := positionsForEndpoints(endpoints, defaultWeight)
	return &hashRing{
		items: items,
	}
}

// endpointFor calculates which backend is responsible for the given traceID
func (h *hashRing) endpointFor(identifier []byte) string {
	if h == nil {
		// perhaps the ring itself couldn't get initialized yet?
		return ""
	}
	hasher := crc32.NewIEEE()
	hasher.Write(identifier)
	hash := hasher.Sum32()
	pos := hash % maxPositions

	return h.findEndpoint(position(pos))
}

// candidateEndpointsFor returns distinct backend candidates in ring order for the given identifier.
func (h *hashRing) candidateEndpointsFor(identifier []byte, limit int) []string {
	if h == nil || len(h.items) == 0 || limit <= 0 {
		return nil
	}

	hasher := crc32.NewIEEE()
	hasher.Write(identifier)
	hash := hasher.Sum32()
	pos := position(hash % maxPositions)

	idx := h.findEndpointIndex(pos)
	if idx < 0 {
		return nil
	}

	seen := make(map[string]struct{}, limit)
	candidates := make([]string, 0, limit)
	for offset := 0; offset < len(h.items) && len(candidates) < limit; offset++ {
		item := h.items[(idx+offset)%len(h.items)]
		if _, ok := seen[item.endpoint]; ok {
			continue
		}
		seen[item.endpoint] = struct{}{}
		candidates = append(candidates, item.endpoint)
	}

	return candidates
}

// findEndpoint returns the "next" endpoint starting from the given position, or an empty string in case no endpoints are available
func (h *hashRing) findEndpoint(pos position) string {
	ringSize := len(h.items)
	if ringSize == 0 {
		return ""
	}
	return h.items[h.findEndpointIndex(pos)].endpoint
}

func (h *hashRing) findEndpointIndex(pos position) int {
	if len(h.items) == 0 {
		return -1
	}
	idx := sort.Search(len(h.items), func(i int) bool {
		return h.items[i].pos >= pos
	})
	if idx == len(h.items) {
		return 0
	}
	return idx
}

// positionFor calculates all the positions in the ring based. The numPoints indicates how many positions to calculate.
// The slice length of the result matches the numPoints.
func positionsFor(endpoint string, numPoints int) []position {
	res := make([]position, 0, numPoints)
	buf := make([]byte, 4)
	for i := range numPoints {
		h := crc32.NewIEEE()
		binary.LittleEndian.PutUint32(buf, uint32(i))
		h.Write([]byte(endpoint))
		h.Write(buf)
		hash := h.Sum32()
		pos := hash % maxPositions
		res = append(res, position(pos))
	}

	return res
}

// positionsForEndpoints calculates all the positions for all the given endpoints
func positionsForEndpoints(endpoints []string, weight int) []ringItem {
	var items []ringItem
	positions := map[position]bool{} // tracking the used positions
	for _, endpoint := range endpoints {
		// for this initial implementation, we don't allow endpoints to have custom weights
		for _, pos := range positionsFor(endpoint, weight) {
			// if this position is occupied already, look ahead in the array for a free position
			actualPos := pos
			positionsProbed := 0
			for positions[actualPos] && positionsProbed < linearProbeLimit {
				actualPos = (actualPos + 1) % position(maxPositions)
				positionsProbed++
			}
			if positionsProbed >= linearProbeLimit {
				continue // Not able to find a free spot; skip this item
			}

			positions[actualPos] = true

			item := ringItem{
				pos:      actualPos,
				endpoint: endpoint,
			}
			items = append(items, item)
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].pos < items[j].pos
	})

	return items
}

func (h *hashRing) equal(candidate *hashRing) bool {
	if candidate == nil {
		return false
	}

	if len(h.items) != len(candidate.items) {
		return false
	}
	for i := range candidate.items {
		if h.items[i].endpoint != candidate.items[i].endpoint {
			return false
		}
		if h.items[i].pos != candidate.items[i].pos {
			return false
		}
	}
	return true
}
