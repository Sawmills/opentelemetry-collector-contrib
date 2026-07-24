// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBackendSubsetSelector(t *testing.T) {
	selector, err := newBackendSubsetSelector(BackendSubsetConfig{})
	require.NoError(t, err)
	require.Nil(t, selector)

	explicitSeed := " gateway-1 "
	selector, err = newBackendSubsetSelector(BackendSubsetConfig{
		Enabled:      true,
		MaxEndpoints: 32,
		Seed:         &explicitSeed,
	})
	require.NoError(t, err)
	require.Equal(t, "gateway-1", selector.seed)

	hostname, err := os.Hostname()
	require.NoError(t, err)
	selector, err = newBackendSubsetSelector(BackendSubsetConfig{
		Enabled:      true,
		MaxEndpoints: 32,
	})
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(hostname), selector.seed)
}

func TestBackendSubsetOrderAndPortInvariant(t *testing.T) {
	selector := backendSubsetSelector{seed: "gateway-1", maxEndpoints: 3}
	endpoints := []string{
		"10.0.0.1:10417",
		"10.0.0.2:10417",
		"10.0.0.3:10417",
		"10.0.0.4:10417",
		"10.0.0.5:10417",
	}

	selected := selector.selectEndpoints(endpoints)
	reversed := slices.Clone(endpoints)
	slices.Reverse(reversed)
	require.Equal(t, selected, selector.selectEndpoints(reversed))

	otherPort := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		otherPort = append(otherPort, strings.Replace(endpoint, ":10417", ":10418", 1))
	}
	require.Equal(
		t,
		backendSubsetHosts(selected),
		backendSubsetHosts(selector.selectEndpoints(otherPort)),
	)
}

func TestBackendSubsetReturnsAllWhenWithinLimit(t *testing.T) {
	selector := backendSubsetSelector{seed: "gateway-1", maxEndpoints: 4}

	require.Equal(
		t,
		[]string{"10.0.0.1:4317", "10.0.0.2:4317"},
		selector.selectEndpoints([]string{"10.0.0.2", "10.0.0.1:4317", "10.0.0.1"}),
	)
}

func TestBackendSubsetMembershipChurn(t *testing.T) {
	selector := backendSubsetSelector{seed: "gateway-1", maxEndpoints: 5}
	endpoints := backendSubsetTestEndpoints(20, 10417)
	selected := selector.selectEndpoints(endpoints)

	withAdded := append(slices.Clone(endpoints), "10.0.1.1:10417")
	require.LessOrEqual(t, symmetricDifferenceSize(selected, selector.selectEndpoints(withAdded)), 2)

	var unselected string
	for _, endpoint := range endpoints {
		if !slices.Contains(selected, endpoint) {
			unselected = endpoint
			break
		}
	}
	require.NotEmpty(t, unselected)
	withoutUnselected := slices.DeleteFunc(slices.Clone(endpoints), func(endpoint string) bool {
		return endpoint == unselected
	})
	require.Equal(t, selected, selector.selectEndpoints(withoutUnselected))

	removedSelected := selected[0]
	withoutSelected := slices.DeleteFunc(slices.Clone(endpoints), func(endpoint string) bool {
		return endpoint == removedSelected
	})
	replacement := selector.selectEndpoints(withoutSelected)
	require.Len(t, replacement, selector.maxEndpoints)
	require.NotContains(t, replacement, removedSelected)
	require.Equal(t, 2, symmetricDifferenceSize(selected, replacement))
}

func TestBackendSubsetDifferentSeeds(t *testing.T) {
	endpoints := backendSubsetTestEndpoints(100, 10417)
	subsets := make(map[string]struct{})
	for i := range 100 {
		selector := backendSubsetSelector{
			seed:         fmt.Sprintf("gateway-%d", i),
			maxEndpoints: 10,
		}
		subsets[strings.Join(selector.selectEndpoints(endpoints), ",")] = struct{}{}
	}

	require.Greater(t, len(subsets), 90)
}

func TestBackendSubsetDistribution(t *testing.T) {
	const (
		gateways     = 400
		workers      = 400
		maxEndpoints = 32
	)
	endpoints := backendSubsetTestEndpoints(workers, 10417)
	counts := make(map[string]int, workers)
	for i := range gateways {
		selector := backendSubsetSelector{
			seed:         fmt.Sprintf("gateway-%d", i),
			maxEndpoints: maxEndpoints,
		}
		for _, endpoint := range selector.selectEndpoints(endpoints) {
			counts[endpoint]++
		}
	}

	mean := float64(gateways*maxEndpoints) / workers
	var sumSquaredDeviation float64
	maxCount := 0
	for _, endpoint := range endpoints {
		count := counts[endpoint]
		maxCount = max(maxCount, count)
		deviation := float64(count) - mean
		sumSquaredDeviation += deviation * deviation
	}
	coefficientOfVariation := math.Sqrt(sumSquaredDeviation/workers) / mean

	require.Less(t, coefficientOfVariation, 0.22)
	require.Less(t, float64(maxCount)/mean, 1.60)
}

func backendSubsetTestEndpoints(count, port int) []string {
	endpoints := make([]string, 0, count)
	for i := range count {
		endpoints = append(endpoints, fmt.Sprintf("10.%d.%d.%d:%d", i/(255*255), (i/255)%255, i%255, port))
	}
	return endpoints
}

func backendSubsetHosts(endpoints []string) []string {
	hosts := make([]string, 0, len(endpoints))
	for _, endpoint := range endpoints {
		hosts = append(hosts, backendSubsetHost(endpoint))
	}
	slices.Sort(hosts)
	return hosts
}

func symmetricDifferenceSize(left, right []string) int {
	difference := 0
	for _, value := range left {
		if !slices.Contains(right, value) {
			difference++
		}
	}
	for _, value := range right {
		if !slices.Contains(left, value) {
			difference++
		}
	}
	return difference
}
