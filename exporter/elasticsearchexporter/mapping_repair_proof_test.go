// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
)

// The observation budget is process-wide. Exercise each public contract in a
// fresh process rather than resetting production state between tests.
func isolatedRepairProofTest(t *testing.T) bool {
	t.Helper()
	if os.Getenv("ES_REPAIR_PROOF_TEST") == t.Name() {
		return false
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), binary, "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(os.Environ(), "ES_REPAIR_PROOF_TEST="+t.Name())
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	return true
}

func TestMappingRepairProvesActualOutgoingEnvelope(t *testing.T) {
	if isolatedRepairProofTest(t) {
		return
	}
	result := mixedMappingRepair(t)

	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"equal": 1, "unequal": 0, "unverified": 0}, result.proofCounts)
	require.Equal(t, int64(1), result.repairCounts["succeeded"])
	for _, point := range result.proofPoints {
		require.Equal(t, 1, point.Attributes.Len())
		require.Empty(t, point.Exemplars)
	}
}

func TestMappingRepairProofBudgetSharedAcrossExporters(t *testing.T) {
	if isolatedRepairProofTest(t) {
		return
	}
	var equal int64
	for range 25 {
		result := mixedMappingRepair(t)
		require.NoError(t, result.err)
		require.Equal(t, int64(1), result.repairCounts["succeeded"])
		equal += result.proofCounts["equal"]
	}
	require.Equal(t, int64(20), equal)
}

func TestMappingRepairProofBudgetSurvivesBatches(t *testing.T) {
	if isolatedRepairProofTest(t) {
		return
	}
	var responses []rejectionHTTPResponse
	for range 25 {
		responses = append(responses,
			rejectionHTTPResponse{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
			rejectionHTTPResponse{body: `{"items":[{"create":{"status":201}}]}`})
	}
	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair,
		func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
			for range 25 {
				if err := exp.ConsumeLogs(ctx, logs); err != nil {
					return err
				}
			}
			return nil
		})
	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"equal": 20, "unequal": 0, "unverified": 0}, result.proofCounts)
	require.Equal(t, int64(25), result.repairCounts["succeeded"])
}

func TestMappingRepairOversizedProofDoesNotChangeDeliveryOrExposeValues(t *testing.T) {
	if isolatedRepairProofTest(t) {
		return
	}
	logs := oneRejectionLog()
	const field = "SYNTHETIC_PRIVATE_FIELD"
	const value = "SYNTHETIC_PRIVATE_VALUE"
	attributes := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes()
	attributes.PutStr(field, strings.Repeat(value, 50000))
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [SYNTHETIC_PRIVATE_FIELD] of type [object]"}}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}
	result := runRejectionResponseFixture(t, logs, responses, enableMappingRepair)

	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"equal": 0, "unequal": 0, "unverified": 1}, result.proofCounts)
	require.Equal(t, int64(1), result.repairCounts["succeeded"])
	require.Zero(t, result.repairCounts["terminal_unrepaired"])
	for _, point := range result.proofPoints {
		require.Equal(t, 1, point.Attributes.Len())
		require.Empty(t, point.Exemplars)
	}
	output := fmt.Sprint(result.proofPoints, result.logEntries, result.err)
	require.NotContains(t, output, field)
	require.NotContains(t, output, value)
}
