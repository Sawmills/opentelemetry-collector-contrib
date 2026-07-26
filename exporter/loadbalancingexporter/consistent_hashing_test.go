// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewHashRing(t *testing.T) {
	// prepare
	endpoints := []string{"endpoint-1", "endpoint-2"}

	// test
	ring := newHashRing(endpoints)

	// verify
	assert.Len(t, ring.items, 2*defaultWeight)
	assert.Equal(t, []string{"endpoint-1:4317", "endpoint-2:4317"}, ring.configuredEndpoints)
}

func TestEndpointFor(t *testing.T) {
	// prepare
	endpoints := []string{"endpoint-1", "endpoint-2"}
	ring := newHashRing(endpoints)

	for _, tt := range []struct {
		id       []byte
		expected string
	}{
		// check that we are indeed alternating endpoints for different inputs
		{[]byte{1, 2, 0, 0}, "endpoint-2"},
		{[]byte{128, 128, 0, 0}, "endpoint-1"},
		{[]byte("ad-service-7"), "endpoint-2"},
		{[]byte("get-recommendations-1"), "endpoint-1"},
	} {
		t.Run(fmt.Sprintf("Endpoint for id %s", string(tt.id)), func(t *testing.T) {
			// test
			endpoint := ring.endpointFor(tt.id)

			// verify
			assert.Equal(t, tt.expected, endpoint)
		})
	}
}

func TestPositionsFor(t *testing.T) {
	// prepare
	endpoint := "host1"

	// test
	positions := positionsFor(endpoint, 10)

	// verify
	assert.Len(t, positions, 10)
}

func TestPositionsForMatchesLegacyCRCConstruction(t *testing.T) {
	for _, endpoint := range []string{"endpoint-1", "10.141.1.2:10417", "[::1]:4317"} {
		expected := make([]position, 0, defaultWeight)
		buf := make([]byte, 4)
		for i := range defaultWeight {
			hasher := crc32.NewIEEE()
			binary.LittleEndian.PutUint32(buf, uint32(i))
			_, _ = hasher.Write([]byte(endpoint))
			_, _ = hasher.Write(buf)
			expected = append(expected, position(hasher.Sum32()%maxPositions))
		}
		assert.Equal(t, expected, positionsFor(endpoint, defaultWeight))
	}
}

func BenchmarkNewHashRing(b *testing.B) {
	for _, endpointCount := range []int{86, 365, 720} {
		b.Run(fmt.Sprintf("endpoints_%d", endpointCount), func(b *testing.B) {
			endpoints := make([]string, endpointCount)
			for i := range endpointCount {
				endpoints[i] = fmt.Sprintf("10.141.%d.%d:10417", i/256, i%256)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				consistentHashBenchmarkRing = newHashRing(endpoints)
			}
		})
	}
}

func BenchmarkHashRingEndpointFor(b *testing.B) {
	ring := newHashRing([]string{"endpoint-1", "endpoint-2"})
	identifier := []byte("bigid-routing-key")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		consistentHashBenchmarkEndpoint = ring.endpointFor(identifier)
	}
}

var (
	consistentHashBenchmarkRing     *hashRing
	consistentHashBenchmarkEndpoint string
)

func TestBinarySearch(t *testing.T) {
	// prepare
	items := []ringItem{
		{pos: 14},
		{pos: 25},
		{pos: 33},
		{pos: 47},
		{pos: 56},
		{pos: 121},
		{pos: 134},
		{pos: 158},
		{pos: 240},
		{pos: 270},
		{pos: 350},
	}
	ringSize := len(items)
	left, right := items[:ringSize/2], items[ringSize/2:]

	for _, tt := range []struct {
		requested position
		expected  position
	}{
		{position(85), position(121)},
		{position(14), position(14)},
		{position(351), position(14)},
		{position(270), position(270)},
		{position(271), position(350)},
	} {
		t.Run(fmt.Sprintf("Angle %d Requested", uint32(tt.requested)), func(t *testing.T) {
			// test
			found := bsearch(tt.requested, left, right)

			// verify
			assert.Equal(t, tt.expected, found.pos)
		})
	}
}

func TestPositionsForEndpoints(t *testing.T) {
	for _, tt := range []struct {
		name      string
		endpoints []string
		expected  []ringItem
	}{
		{
			"Single Endpoint",
			[]string{"endpoint-1"},
			[]ringItem{
				// this was first calculated by running the algorithm and taking its output
				{pos: 0x21ca, endpoint: "endpoint-1"},
				{pos: 0x29d3, endpoint: "endpoint-1"},
				{pos: 0x3984, endpoint: "endpoint-1"},
				{pos: 0x5eaf, endpoint: "endpoint-1"},
				{pos: 0x8bc1, endpoint: "endpoint-1"},
			},
		},
		{
			"Duplicate Endpoint",
			[]string{"endpoint-1", "endpoint-1"},
			[]ringItem{
				// We expect to not have duplicate items.
				// When a clash occurs, the next free positions should be taken. In this case, there will always be
				// exactly one clash because of duplicate endpoints. So, the pos will always be i and i+1.
				{pos: 0x21ca, endpoint: "endpoint-1"},
				{pos: 0x21cb, endpoint: "endpoint-1"},
				{pos: 0x29d3, endpoint: "endpoint-1"},
				{pos: 0x29d4, endpoint: "endpoint-1"},
				{pos: 0x3984, endpoint: "endpoint-1"},
				{pos: 0x3985, endpoint: "endpoint-1"},
				{pos: 0x5eaf, endpoint: "endpoint-1"},
				{pos: 0x5eb0, endpoint: "endpoint-1"},
				{pos: 0x8bc1, endpoint: "endpoint-1"},
				{pos: 0x8bc2, endpoint: "endpoint-1"},
			},
		},
		{
			"Multiple Endpoints",
			[]string{"endpoint-A", "endpoint-B"},
			[]ringItem{
				// we expect to have 5 positions for each endpoint
				{pos: 0xdde, endpoint: "endpoint-B"},
				{pos: 0x162e, endpoint: "endpoint-A"},
				{pos: 0x21f5, endpoint: "endpoint-B"},
				{pos: 0x34e5, endpoint: "endpoint-A"},
				{pos: 0x61fb, endpoint: "endpoint-B"},
				{pos: 0x6910, endpoint: "endpoint-B"},
				{pos: 0x76a0, endpoint: "endpoint-A"},
				{pos: 0x7e2b, endpoint: "endpoint-A"},
				{pos: 0x7f7c, endpoint: "endpoint-A"},
				{pos: 0x85ac, endpoint: "endpoint-B"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// test
			items := positionsForEndpoints(tt.endpoints, 5)

			// verify
			assert.Equal(t, tt.expected, items)
		})
	}
}

func TestEqual(t *testing.T) {
	original := &hashRing{
		items: []ringItem{
			{pos: position(123), endpoint: "endpoint-1"},
		},
		endpoints: []string{"endpoint-1:4317"},
	}

	for _, tt := range []struct {
		name      string
		candidate *hashRing
		outcome   bool
	}{
		{
			"empty",
			&hashRing{items: []ringItem{}},
			false,
		},
		{
			"null",
			nil,
			false,
		},
		{
			"equal",
			&hashRing{
				items: []ringItem{
					{pos: position(123), endpoint: "endpoint-1"},
				},
				endpoints: []string{"endpoint-1:4317"},
			},
			true,
		},
		{
			"different length",
			&hashRing{
				items: []ringItem{
					{pos: position(123), endpoint: "endpoint-1"},
					{pos: position(124), endpoint: "endpoint-2"},
				},
				endpoints: []string{"endpoint-1:4317", "endpoint-2:4317"},
			},
			false,
		},
		{
			"different position",
			&hashRing{
				items: []ringItem{
					{pos: position(124), endpoint: "endpoint-1"},
				},
				endpoints: []string{"endpoint-1:4317"},
			},
			false,
		},
		{
			"different endpoint",
			&hashRing{
				items: []ringItem{
					{pos: position(123), endpoint: "endpoint-2"},
				},
				endpoints: []string{"endpoint-2:4317"},
			},
			false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.outcome, original.equal(tt.candidate))
		})
	}
}

func TestHashRingConfiguredEndpointsDoNotChangeRoutableEndpoints(t *testing.T) {
	items := []ringItem{{pos: 1, endpoint: "endpoint-1"}}
	ring := &hashRing{
		items:               items,
		endpoints:           hashRingEndpoints(items),
		configuredEndpoints: normalizeEndpoints([]string{"endpoint-1", "endpoint-without-position"}),
	}

	assert.Equal(t, []string{"endpoint-1:4317"}, ring.endpoints)
	assert.True(t, ring.hasNormalizedEndpoints([]string{"endpoint-1:4317", "endpoint-without-position:4317"}))
	assert.True(t, ring.hasNormalizedEndpoints([]string{
		"endpoint-without-position",
		"endpoint-1:4317",
		"endpoint-1",
	}))
}
