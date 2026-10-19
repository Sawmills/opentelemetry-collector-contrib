// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadatatest"
)

func TestMappingRepairRetriesOnlyConfirmedRejectedItem(t *testing.T) {
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}

	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, func(config *Config) {
		config.RepairMappingFailures = true
		config.Mapping.Mode = "ecs"
		config.Mapping.AllowedModes = []string{"ecs"}
	})

	require.NoError(t, result.err)
	require.Equal(t, int64(1), result.failed, "existing failure counter keeps its original meaning")
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 1, "succeeded": 1, "terminal_unrepaired": 0, "unconfirmed": 0}, result.repairCounts)
}

func enableMappingRepair(config *Config) {
	config.RepairMappingFailures = true
	config.Mapping.Mode = "ecs"
	config.Mapping.AllowedModes = []string{"ecs"}
}

func mixedMappingRepair(t *testing.T) rejectionFixtureResult {
	t.Helper()
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr("accepted")
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},{"create":{"status":201}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}
	return runRejectionResponseFixture(t, logs, responses, enableMappingRepair)
}

func TestMappingRepairKeepsAcceptedMember(t *testing.T) {
	result := mixedMappingRepair(t)

	require.NoError(t, result.err)
	require.NotContains(t, result.requests[1], `"message":"accepted"`, "accepted member must never be submitted again")
	require.Equal(t, strings.Split(result.requests[0], "\n")[0], strings.Split(result.requests[1], "\n")[0], "bulk action and routing must stay unchanged")
}

func TestMappingRepairKeepsFullOriginalValue(t *testing.T) {
	want := `{"@timestamp":"1970-01-01T00:00:00.000000000Z","message":"synthetic","sawmills_original_error_json":"{\"key\":\"error\",\"value_type\":\"object\",\"value_json\":\"{\\\"code\\\":\\\"synthetic\\\"}\"}"}`

	result := mixedMappingRepair(t)

	require.NoError(t, result.err)
	require.JSONEq(t, want, strings.Split(result.requests[1], "\n")[1])
}

func TestMappingRepairMakesOnlyOneAttempt(t *testing.T) {
	rejected := rejectionHTTPResponse{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{rejected, rejected}, enableMappingRepair)

	require.NoError(t, result.err)
	require.Equal(t, int64(2), result.failed)
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 1, "succeeded": 0, "terminal_unrepaired": 1, "unconfirmed": 0}, result.repairCounts)
}

func TestMappingRepairFailedRequestIsPermanentAndDoesNotExposePayload(t *testing.T) {
	secret := "SYNTHETIC-PRIVATE-PAYLOAD"
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{status: 400, body: `{"error":"SYNTHETIC-PRIVATE-PAYLOAD"}`},
	}

	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair)

	require.True(t, consumererror.IsPermanent(result.err), "repair errors must not cause whole-batch replay")
	require.NotContains(t, fmt.Sprint(result.logEntries, result.err), secret)
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 1, "succeeded": 0, "terminal_unrepaired": 1, "unconfirmed": 1}, result.repairCounts)
}

func TestMappingRepairIsOffByDefault(t *testing.T) {
	response := rejectionHTTPResponse{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{response}, nil)

	require.NoError(t, result.err)
	require.Equal(t, int64(1), result.failed)
	require.Empty(t, result.repairCounts)
}

func mappingRepairConfig() *Config {
	config := NewFactory().CreateDefaultConfig().(*Config)
	config.Endpoints = []string{"http://localhost:9200"}
	enableMappingRepair(config)
	return config
}

func TestMappingRepairRefusesPayloadLogging(t *testing.T) {
	config := mappingRepairConfig()
	config.LogFailedDocsInput = true

	err := config.Validate()

	require.ErrorContains(t, err, "payload logging disabled")
}

func TestMappingRepairRequiresPreservedReasons(t *testing.T) {
	config := mappingRepairConfig()
	config.PreserveErrorReason = false

	err := config.Validate()

	require.ErrorContains(t, err, "preserve_error_reason")
}

func TestMappingRepairRefusesAutomatic400Retries(t *testing.T) {
	config := mappingRepairConfig()
	config.Retry.RetryOnStatus = []int{400}

	err := config.Validate()

	require.ErrorContains(t, err, "400")
}

func TestMappingRepairRefusesMetricsExporter(t *testing.T) {
	config := mappingRepairConfig()
	settings := metadatatest.NewSettings(componenttest.NewTelemetry())

	_, err := NewFactory().CreateMetrics(t.Context(), settings, config)

	require.ErrorContains(t, err, "only supported for logs")
}

func TestMappingRepairPublishesPositiveZeroCoverageForAcceptedBatch(t *testing.T) {
	response := rejectionHTTPResponse{body: `{"items":[{"create":{"status":201}}]}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{response}, enableMappingRepair)

	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"original_400": 0, "attempted": 0, "succeeded": 0, "terminal_unrepaired": 0, "unconfirmed": 0}, result.repairCounts)
}

func TestMappingRepairKeepsUnknownRejectionUnrepaired(t *testing.T) {
	response := rejectionHTTPResponse{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"untrusted prefix failed to parse field [error] of type [text]"}}}]}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{response}, enableMappingRepair)

	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 0, "succeeded": 0, "terminal_unrepaired": 1, "unconfirmed": 0}, result.repairCounts)
}

func TestMappingRepairKeepsExistingBackupUntouched(t *testing.T) {
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().PutStr("sawmills_original_error_json", "existing")
	response := rejectionHTTPResponse{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}

	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{response}, enableMappingRepair)

	require.NoError(t, result.err)
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 0, "succeeded": 0, "terminal_unrepaired": 1, "unconfirmed": 0}, result.repairCounts)
	require.Contains(t, result.requests[0], `"sawmills_original_error_json":"existing"`)
}

func routedMappingRepair(t *testing.T, docID, pipeline string) rejectionFixtureResult {
	t.Helper()
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().PutStr("elasticsearch.document_id", docID)
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}
	return runRejectionResponseFixture(t, logs, responses, func(config *Config) {
		enableMappingRepair(config)
		config.LogsDynamicID.Enabled = true
		config.Pipeline = pipeline
	})
}

func TestMappingRepairPreservesDocumentID(t *testing.T) {
	result := routedMappingRepair(t, "synthetic-id", "synthetic-ingest")

	require.NoError(t, result.err)
	require.Contains(t, result.requests[0], `"_id":"synthetic-id"`)
	require.Equal(t, strings.Split(result.requests[0], "\n")[0], strings.Split(result.requests[1], "\n")[0])
}

func TestMappingRepairPreservesIngestPipeline(t *testing.T) {
	result := routedMappingRepair(t, "synthetic-id", "synthetic-ingest")

	require.NoError(t, result.err)
	require.Contains(t, result.requestQueries[0], "pipeline=synthetic-ingest")
	require.Equal(t, result.requestQueries[0], result.requestQueries[1])
}

func TestMappingRepairFailureStillDrainsOriginal429(t *testing.T) {
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr("retry-429")
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},{"create":{"status":429}}]}`},
		{status: 400, body: `{"error":"repair-unconfirmed"}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}

	result := runRejectionResponseFixture(t, logs, responses, func(config *Config) {
		enableMappingRepair(config)
		config.Retry.Enabled = true
	})

	require.True(t, consumererror.IsPermanent(result.err))
	require.Contains(t, result.requests[2], `"message":"retry-429"`)
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
}

func TestMappingRepairDoesNotRepeatHTTP429(t *testing.T) {
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{status: 429, body: `{"error":"repair-throttled"}`},
	}

	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, func(config *Config) {
		enableMappingRepair(config)
		config.Retry.Enabled = true
		config.Retry.MaxRetries = 1
	})

	require.True(t, consumererror.IsPermanent(result.err))
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
}

func TestMappingRepairRequiresPositiveRetryItemStatus(t *testing.T) {
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{body: `{"items":[{"create":{}}]}`},
	}

	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair)

	require.True(t, consumererror.IsPermanent(result.err))
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
	require.Zero(t, result.repairCounts["succeeded"])
}

func TestMappingRepairRequiresPositiveOriginalItemStatus(t *testing.T) {
	response := rejectionHTTPResponse{body: `{"items":[{"create":{}}]}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{response}, enableMappingRepair)

	require.True(t, consumererror.IsPermanent(result.err))
	require.Equal(t, int64(0), result.repairCounts["terminal_unrepaired"])
	require.Equal(t, int64(1), result.repairCounts["unconfirmed"])
}

func TestMappingRepairDoesNotLogOriginalRejectedValue(t *testing.T) {
	secret := "SYNTHETIC-ORIGINAL-VALUE"
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]. Preview of field's value: 'SYNTHETIC-ORIGINAL-VALUE'"}}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}

	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair)

	require.NoError(t, result.err)
	require.NotContains(t, fmt.Sprint(result.logEntries), secret)
}

func TestMappingRepairDoesNotExposeOriginalRequestErrorBody(t *testing.T) {
	secret := "SYNTHETIC-REQUEST-VALUE"
	response := rejectionHTTPResponse{status: 400, body: `{"error":"SYNTHETIC-REQUEST-VALUE"}`}

	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{response}, enableMappingRepair)

	require.Error(t, result.err)
	require.NotContains(t, fmt.Sprint(result.logEntries, result.err), secret)
}

func TestMappingRepairRefusesDuplicateResponseItems(t *testing.T) {
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{body: `{"items":[{"create":{}}],"items":[{"create":{"status":201}}]}`},
	}
	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair)
	require.True(t, consumererror.IsPermanent(result.err))
	require.Zero(t, result.repairCounts["succeeded"])
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
	require.Equal(t, int64(1), result.repairCounts["unconfirmed"])
}

func TestMappingRepairKeepsPermanentErrorWhenOrdinaryRetryFails(t *testing.T) {
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr("retry-429")
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},{"create":{"status":429}}]}`},
		{status: 400, body: `{"error":"repair-unconfirmed"}`},
		{status: 503, body: `{"error":"original-unconfirmed"}`},
	}
	result := runRejectionResponseFixture(t, logs, responses, func(config *Config) {
		enableMappingRepair(config)
		config.Retry.Enabled = true
		config.Retry.RetryOnStatus = []int{429}
	})
	require.True(t, consumererror.IsPermanent(result.err), "an uncertain repair must prevent batch replay even after ordinary retries fail")
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
	require.Equal(t, int64(2), result.repairCounts["unconfirmed"])
}

func TestMappingRepairCancellationKeepsPermanentError(t *testing.T) {
	cancelReady := make(chan context.CancelFunc, 1)
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{before: func() { (<-cancelReady)() }, status: 503, body: `{"error":"repair-unconfirmed"}`},
	}
	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, enableMappingRepair, func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cancelReady <- cancel
		return exp.ConsumeLogs(ctx, logs)
	})
	require.True(t, consumererror.IsPermanent(result.err), "ConsumeLogs must preserve permanent repair errors after cancellation")
	require.ErrorIs(t, result.err, context.Canceled)
	require.Equal(t, int64(1), result.repairCounts["terminal_unrepaired"])
	require.Equal(t, int64(1), result.repairCounts["unconfirmed"])
}

func TestMappingRepairCancellationKeepsEarlierPermanentError(t *testing.T) {
	cancelReady := make(chan context.CancelFunc, 1)
	logs := oneRejectionLog()
	logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr("later-record")
	responses := []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
		{status: 400, body: `{"error":"repair-unconfirmed"}`},
		{before: func() { (<-cancelReady)() }, status: 503, body: `{"error":"later-unconfirmed"}`},
	}
	result := runRejectionResponseFixtureWithFactory(t, logs, responses, func(config *Config) {
		enableMappingRepair(config)
		config.QueueBatchConfig = configoptional.Some(exporterhelper.QueueBatchConfig{Batch: configoptional.Some(exporterhelper.BatchConfig{Sizer: exporterhelper.RequestSizerTypeBytes, MaxSize: 1})})
	}, func(_ context.Context, set exporter.Settings, cfg *Config) (exporter.Logs, error) {
		e, err := newExporter(cfg, set, cfg.LogsIndex)
		if err != nil {
			return nil, err
		}
		return directRepairFixtureExporter{e}, nil
	}, func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cancelReady <- cancel
		return exp.ConsumeLogs(ctx, logs)
	})
	require.True(t, consumererror.IsPermanent(result.err), "later cancellation must retain the earlier repair's no-replay marker")
	require.ErrorIs(t, result.err, context.Canceled)
}

// Exercise the same logs callback without the outer asynchronous queue so the
// size-flush and cancellation behavior is observed at its owning module seam.
type directRepairFixtureExporter struct{ *elasticsearchExporter }

func (e directRepairFixtureExporter) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	return e.pushLogsData(ctx, logs)
}

func (directRepairFixtureExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func TestMappingRepairKeepsOrdinaryRetriesAfterLargeValidResponse(t *testing.T) {
	logs := oneRejectionLog()
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	records.AppendEmpty().Body().SetStr("ordinary-retry")
	records.AppendEmpty().Body().SetStr("already-accepted")
	response := `{"padding":"` + strings.Repeat("x", 17<<20) + `","items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},{"create":{"status":429}},{"create":{"status":201}}]}`
	responses := []rejectionHTTPResponse{
		{body: response},
		{body: `{"items":[{"create":{"status":201}}]}`},
		{body: `{"items":[{"create":{"status":201}}]}`},
	}
	result := runRejectionResponseFixture(t, logs, responses, func(config *Config) {
		enableMappingRepair(config)
		config.Retry.Enabled = true
	})
	require.NoError(t, result.err, "valid response size must not discard recoverable members")
	require.NotContains(t, result.requests[1], "ordinary-retry")
	require.Contains(t, result.requests[2], "ordinary-retry")
	require.NotContains(t, result.requests[1]+result.requests[2], "already-accepted")
	require.Equal(t, map[string]int64{"original_400": 1, "attempted": 1, "succeeded": 1, "terminal_unrepaired": 0, "unconfirmed": 0}, result.repairCounts)
}
