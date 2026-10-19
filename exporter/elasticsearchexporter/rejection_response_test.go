// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadatatest"
)

type rejectionHTTPResponse struct {
	before func()
	status int
	body   string
}

type rejectionFixtureResult struct {
	points            []metricdata.DataPoint[int64]
	requests          []string
	requestQueries    []string
	err               error
	failed            int64
	existingExemplars int
	repairCounts      map[string]int64
	logEntries        []observer.LoggedEntry
}

// This fixture crosses the factory/ConsumeLogs seam and observes real OTel
// counters and real HTTP requests. It never calls the private classifier.
func runRejectionResponseFixture(t *testing.T, logs plog.Logs, responses []rejectionHTTPResponse, configure func(*Config), calls ...func(exporter.Logs, context.Context, plog.Logs) error) rejectionFixtureResult {
	return runRejectionResponseFixtureWithFactory(t, logs, responses, configure, nil, calls...)
}

func runRejectionResponseFixtureWithFactory(t *testing.T, logs plog.Logs, responses []rejectionHTTPResponse, configure func(*Config), makeLogs func(context.Context, exporter.Settings, *Config) (exporter.Logs, error), calls ...func(exporter.Logs, context.Context, plog.Logs) error) rejectionFixtureResult {
	t.Helper()
	var mu sync.Mutex
	var requests []string
	var requestQueries []string
	server := newESTestServerBulkHandlerFunc(t, func(w http.ResponseWriter, r *http.Request) {
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			defer gz.Close()
			reader = gz
		}
		data, err := io.ReadAll(reader)
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		mu.Lock()
		position := len(requests)
		requests = append(requests, string(data))
		requestQueries = append(requestQueries, r.URL.RawQuery)
		mu.Unlock()
		if position >= len(responses) {
			t.Error("unexpected extra bulk request")
			w.WriteHeader(500)
			return
		}
		response := responses[position]
		if response.before != nil {
			response.before()
		}
		w.Header().Set("Content-Type", "application/json")
		status := response.status
		if status == 0 {
			status = 200
		}
		w.WriteHeader(status)
		_, err = io.WriteString(w, response.body)
		if err != nil {
			t.Error(err)
		}
	})
	telemetry := componenttest.NewTelemetry(componenttest.WithMetricOptions(sdkmetric.WithExemplarFilter(exemplar.TraceBasedFilter)))
	settings := metadatatest.NewSettings(telemetry)
	logCore, logObserver := observer.New(zap.DebugLevel)
	settings.Logger = zap.New(logCore)
	settings.ID = component.NewIDWithName(metadata.Type, "fixture")
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	config.Endpoints = []string{server.URL}
	config.LogsIndex = "logs-fixture"
	config.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	config.Retry.Enabled = false
	if configure != nil {
		configure(config)
	}
	var exp exporter.Logs
	var err error
	if makeLogs == nil {
		exp, err = factory.CreateLogs(t.Context(), settings, config)
	} else {
		exp, err = makeLogs(t.Context(), settings, config)
	}
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.WithoutCancel(t.Context()))) })
	ctx := client.NewContext(t.Context(), client.Info{Metadata: client.NewMetadata(map[string][]string{"X-Elastic-Mapping-Mode": {"ecs"}})})
	if !exp.Capabilities().MutatesData {
		logs.MarkReadOnly()
	}
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled}))
	var consumeErr error
	if len(calls) == 0 {
		consumeErr = exp.ConsumeLogs(ctx, logs)
	} else {
		consumeErr = calls[0](exp, ctx, logs)
	}
	result := rejectionFixtureResult{err: consumeErr}
	mu.Lock()
	result.requests = append([]string(nil), requests...)
	result.requestQueries = append([]string(nil), requestQueries...)
	mu.Unlock()
	require.Len(t, result.requests, len(responses))
	if collected, err := telemetry.GetMetric("otelcol.elasticsearch.docs.failed_by_field"); err == nil {
		sum, ok := collected.Data.(metricdata.Sum[int64])
		require.True(t, ok)
		result.points = sum.DataPoints
	} else {
		require.ErrorContains(t, err, "not found")
	}
	collected, err := telemetry.GetMetric("otelcol.elasticsearch.docs.failed")
	require.NoError(t, err)
	sum, ok := collected.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	for _, point := range sum.DataPoints {
		result.failed += point.Value
		result.existingExemplars += len(point.Exemplars)
	}
	result.repairCounts = make(map[string]int64)
	for _, name := range []string{"original_400", "attempted", "succeeded", "terminal_unrepaired", "unconfirmed"} {
		if collected, err := telemetry.GetMetric("otelcol.elasticsearch.mapping_repair." + name); err == nil {
			for _, point := range collected.Data.(metricdata.Sum[int64]).DataPoints {
				result.repairCounts[name] += point.Value
			}
		}
	}
	result.logEntries = logObserver.All()
	return result
}

func oneRejectionLog() plog.Logs {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetStr("synthetic")
	record.Attributes().PutEmptyMap("error").PutStr("code", "synthetic")
	return logs
}

func assertRejectionTuple(t *testing.T, point metricdata.DataPoint[int64], status, field, expected, kind, source string, count int64) {
	t.Helper()
	require.Equal(t, count, point.Value)
	require.Equal(t, 6, point.Attributes.Len())
	for key, want := range map[string]string{"exporter": "elasticsearch/fixture", "item_status_class": status, "field_class": field, "expected_type": expected, "value_type": kind, "source": source} {
		actual, found := point.Attributes.Value(attribute.Key(key))
		require.True(t, found)
		require.Equal(t, want, actual.AsString())
	}
}

func TestRejectionResponsePrivacyAndCoverage(t *testing.T) {
	for _, test := range []struct {
		name, response                                            string
		status                                                    int
		strip                                                     bool
		wantStatus, wantField, wantExpected, wantKind, wantSource string
	}{
		{name: "known_header", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "text", wantKind: "map", wantSource: "log"},
		{name: "elasticsearch_location", response: `{"items":[{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"[1:42] failed to parse field [error] of type [text] in document with id 'SYNTHETIC-ID'. Preview of field's value: 'SYNTHETIC-SECRET'"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "text", wantKind: "map", wantSource: "log"},
		{name: "object_mapping", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"object mapping for [error] tried to parse field [error] as object, but found a concrete value"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "object", wantKind: "map", wantSource: "log"},
		{name: "located_object_mapping", response: `{"items":[{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"[2:7] object mapping for [error] tried to parse field [error] as object, but found a concrete value"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "object", wantKind: "map", wantSource: "log"},
		{name: "malformed_location", response: `{"items":[{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"[x:42] failed to parse field [error] of type [text]"}}}]}`, wantStatus: "400"},
		{name: "mismatched_object_path", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"object mapping for [error] tried to parse field [message] as object, but found a concrete value"}}}]}`, wantStatus: "400"},
		{name: "unrecognized_prefix", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"synthetic-secret failed to parse field [error] of type [text]"}}}]}`, wantStatus: "400"},
		{name: "nested_path", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error.private] of type [text]"}}}]}`, wantStatus: "400"},
		{name: "malformed_suffix", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]not-a-header"}}}]}`, wantStatus: "400"},
		{name: "documentary_suffix", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text] in documentary garbage"}}}]}`, wantStatus: "400"},
		{name: "arbitrary_period_suffix", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]. unsupported format"}}}]}`, wantStatus: "400"},
		{name: "supported_period_preview", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]. Preview of field's value: 'SYNTHETIC-SECRET'"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "text", wantKind: "map", wantSource: "log"},
		{name: "unknown_type", response: `{"items":[{"create":{"status":400,"error":{"type":"private-error-type","reason":"failed to parse field [error] of type [text]"}}}]}`, wantStatus: "400"},
		{name: "stripped_reason", strip: true, response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`, wantStatus: "400"},
		{name: "conflict", response: `{"items":[{"create":{"status":409,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`, wantStatus: "409"},
		{name: "opensearch_header", response: `{"items":[{"create":{"status":400,"error":{"type":"document_parsing_exception","reason":"failed to parse field [error] of type [text] in document with id 'SYNTHETIC-ID'. Preview of field's value: 'SYNTHETIC-SECRET'"}}}]}`, wantStatus: "400", wantField: "error", wantExpected: "text", wantKind: "map", wantSource: "log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{{body: test.response}}, func(config *Config) { config.PreserveErrorReason = !test.strip })
			require.NoError(t, result.err)
			require.Equal(t, int64(1), result.failed)
			require.Len(t, result.points, 1)
			field, expected, kind, source := test.wantField, test.wantExpected, test.wantKind, test.wantSource
			if field == "" {
				field, expected, kind, source = "unknown", "unknown", "unknown", "unknown"
			}
			assertRejectionTuple(t, result.points[0], test.wantStatus, field, expected, kind, source, 1)
		})
	}
}

func TestRejectionResponseEmptyKeyMapHasUnknownProvenance(t *testing.T) {
	for _, source := range []string{"resource", "scope", "log"} {
		t.Run(source, func(t *testing.T) {
			logs := oneRejectionLog()
			rl := logs.ResourceLogs().At(0)
			sl := rl.ScopeLogs().At(0)
			attrs := sl.LogRecords().At(0).Attributes()
			if source == "resource" {
				attrs = rl.Resource().Attributes()
			} else if source == "scope" {
				attrs = sl.Scope().Attributes()
			}
			attrs.PutEmptyMap("").PutEmptyMap("").PutStr("error", "synthetic flattened contributor")
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, nil)
			require.NoError(t, result.err)
			require.Len(t, result.points, 1)
			assertRejectionTuple(t, result.points[0], "400", "error", "text", "unknown", "unknown", 1)
		})
	}
}

func TestRejectionResponseSharedShapesStayIsolated(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutDouble("duration", 0.25)
	first := rl.ScopeLogs().AppendEmpty()
	first.Scope().Attributes().PutEmptyMap("error").PutStr("code", "synthetic")
	first.LogRecords().AppendEmpty().Body().SetStr("synthetic")
	collision := first.LogRecords().AppendEmpty()
	collision.Body().SetStr("synthetic")
	collision.Attributes().PutStr("error", "synthetic")
	first.LogRecords().AppendEmpty().Body().SetStr("synthetic")
	second := rl.ScopeLogs().AppendEmpty()
	second.Scope().Attributes().PutStr("error", "synthetic")
	second.LogRecords().AppendEmpty().Body().SetStr("synthetic")
	durationCollision := second.LogRecords().AppendEmpty()
	durationCollision.Body().SetStr("synthetic")
	durationCollision.Attributes().PutEmptyMap("duration").PutStr("value", "synthetic")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("synthetic")
	errorItem := `{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}`
	durationItem := `{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [duration] of type [float]"}}}`
	response := `{"items":[` + strings.Repeat(errorItem+",", 4) + durationItem + "," + durationItem + `]}`
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: response}}, nil)
	require.NoError(t, result.err)
	require.Equal(t, int64(6), result.failed)
	want := map[string]int64{
		"error:map:scope":          2,
		"error:unknown:mixed":      1,
		"error:string:scope":       1,
		"duration:unknown:mixed":   1,
		"duration:double:resource": 1,
	}
	require.Len(t, result.points, len(want))
	for _, point := range result.points {
		field, _ := point.Attributes.Value(attribute.Key("field_class"))
		kind, _ := point.Attributes.Value(attribute.Key("value_type"))
		source, _ := point.Attributes.Value(attribute.Key("source"))
		key := field.AsString() + ":" + kind.AsString() + ":" + source.AsString()
		require.Contains(t, want, key)
		expected := "text"
		if field.AsString() == "duration" {
			expected = "number"
		}
		assertRejectionTuple(t, point, "400", field.AsString(), expected, kind.AsString(), source.AsString(), want[key])
		delete(want, key)
	}
	require.Empty(t, want)
}

func TestRejectionResponseDoesNotFabricateUnconfirmedItems(t *testing.T) {
	for _, test := range []struct {
		name, response  string
		status, records int
		wantError       bool
		wantFailed      int64
	}{
		{name: "accepted", response: `{"items":[{"create":{"status":201}}]}`, records: 1},
		{name: "request_failure", response: `{"error":"synthetic unavailable"}`, status: 503, records: 1, wantError: true, wantFailed: 1},
		{name: "truncated_after_success", response: `{"items":[{"create":{"status":201}},`, records: 2, wantError: true, wantFailed: 1},
		{name: "truncated_after_rejection", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},`, records: 2, wantError: true, wantFailed: 2},
		{name: "too_few_items", response: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`, records: 2, wantFailed: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := oneRejectionLog()
			if test.records == 2 {
				logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty().Body().SetStr("synthetic second")
			}
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{status: test.status, body: test.response}}, nil)
			if test.wantError {
				require.Error(t, result.err)
			} else {
				require.NoError(t, result.err)
			}
			require.Empty(t, result.points)
			require.Equal(t, test.wantFailed, result.failed)
		})
	}
}

func TestRejectionResponseBodyAliasProvenanceIsAmbiguous(t *testing.T) {
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Body().SetEmptyMap().PutStr("exception.message", "synthetic")
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, nil)
	require.NoError(t, result.err)
	require.Len(t, result.points, 1)
	// Sawmills preprocessing can copy a body alias into attributes. The later
	// encoder seam cannot distinguish that copy from an original log attribute.
	assertRejectionTuple(t, result.points[0], "400", "error", "text", "unknown", "mixed", 1)
	const want = "{\"create\":{\"_index\":\"logs-fixture\",\"require_data_stream\":true}}\n" +
		"{\"@timestamp\":\"1970-01-01T00:00:00.000000000Z\",\"error\":{\"message\":\"synthetic\"},\"message\":\"{\\\"exception.message\\\":\\\"synthetic\\\"}\"}\n"
	require.Equal(t, want, result.requests[0])
}

func TestRejectionResponseRetriesHaveUnknownProvenance(t *testing.T) {
	const initial = `{"items":[{"create":{"status":201}},{"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"synthetic retry"}}}]}`
	for _, test := range []struct {
		name, final, status, field, expected string
		wantCount                            int64
	}{
		{name: "retry_succeeds", final: `{"items":[{"create":{"status":201}}]}`},
		{name: "retry_terminal_mapping_failure", final: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`, status: "400", field: "error", expected: "text", wantCount: 1},
		{name: "retry_exhausted", final: `{"items":[{"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"synthetic retry"}}}]}`, status: "429", field: "unknown", expected: "unknown", wantCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := oneRejectionLog()
			scope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
			scope.Scope().Attributes().PutEmptyMap("error").PutStr("code", "retained")
			scope.LogRecords().AppendEmpty().Body().SetStr("synthetic retained")
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: initial}, {body: test.final}}, func(config *Config) {
				config.Retry.Enabled = true
				config.Retry.MaxRetries = 1
				config.Retry.InitialInterval = time.Millisecond
				config.Retry.MaxInterval = time.Millisecond
			})
			require.NoError(t, result.err)
			require.Equal(t, test.wantCount, result.failed)
			if test.wantCount == 0 {
				require.Empty(t, result.points)
			} else {
				require.Len(t, result.points, 1)
				assertRejectionTuple(t, result.points[0], test.status, test.field, test.expected, "unknown", "unknown", 1)
			}
			const retained = "{\"create\":{\"_index\":\"logs-fixture\",\"require_data_stream\":true}}\n" +
				"{\"@timestamp\":\"1970-01-01T00:00:00.000000000Z\",\"error\":{\"code\":\"retained\"},\"message\":\"synthetic retained\"}\n"
			require.Equal(t, retained, result.requests[1], "only the retrying record is sent again")
		})
	}
}

func TestRejectionResponseWholeRequestRetryDoesNotMultiplyItems(t *testing.T) {
	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{{status: 503, body: `{"error":"synthetic retry"}`}, {body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, func(config *Config) {
		config.Retry.Enabled = true
		config.Retry.MaxRetries = 1
		config.Retry.RetryOnStatus = []int{503}
		config.Retry.InitialInterval = time.Millisecond
		config.Retry.MaxInterval = time.Millisecond
	})
	require.NoError(t, result.err)
	require.Equal(t, int64(1), result.failed)
	require.Len(t, result.points, 1)
	assertRejectionTuple(t, result.points[0], "400", "error", "text", "map", "log", 1)
	require.Equal(t, result.requests[0], result.requests[1])
}

func TestRejectionResponseSummaryCapRetainsFailureCount(t *testing.T) {
	logs := oneRejectionLog()
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for range 8192 {
		records.At(0).CopyTo(records.AppendEmpty())
	}
	response := `{"items":[` + strings.Repeat(`{"create":{"status":201}},`, 8192) + `{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: response}}, nil)
	require.NoError(t, result.err)
	require.Equal(t, int64(1), result.failed)
	require.Len(t, result.points, 1)
	assertRejectionTuple(t, result.points[0], "400", "error", "text", "unknown", "unknown", 1)
	require.Equal(t, 8193, strings.Count(result.requests[0], `"create":`))
}

func TestRejectionResponseSeriesCapRetainsFailureCount(t *testing.T) {
	logs := plog.NewLogs()
	var items []string
	for _, field := range []string{"timestamp", "duration", "error", "errorsData", "change"} {
		for source := range 3 {
			for kind := range 8 {
				rl := logs.ResourceLogs().AppendEmpty()
				sl := rl.ScopeLogs().AppendEmpty()
				lr := sl.LogRecords().AppendEmpty()
				lr.Body().SetStr("synthetic")
				attrs := []pcommon.Map{rl.Resource().Attributes(), sl.Scope().Attributes(), lr.Attributes()}[source]
				switch kind {
				case 0:
					attrs.PutEmptyMap(field).PutStr("code", "synthetic")
				case 1:
					attrs.PutEmptySlice(field).AppendEmpty().SetStr("synthetic")
				case 2:
					attrs.PutStr(field, "synthetic")
				case 3:
					attrs.PutInt(field, 42)
				case 4:
					attrs.PutDouble(field, 0.125)
				case 5:
					attrs.PutBool(field, true)
				case 6:
					attrs.PutEmptyBytes(field).FromRaw([]byte{1, 2})
				case 7:
					attrs.PutEmpty(field)
				}
				items = append(items, fmt.Sprintf(`{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [%s] of type [text]"}}}`, field))
			}
		}
	}
	for _, status := range []int{409, 429, 418, 500, 302} {
		logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("synthetic")
		items = append(items, fmt.Sprintf(`{"create":{"status":%d,"error":{"type":"synthetic","reason":"synthetic"}}}`, status))
	}
	extra := oneRejectionLog()
	extra.ResourceLogs().At(0).ScopeLogs().At(0).Scope().Attributes().PutStr("elastic.mapping.mode", "none")
	extra.MarkReadOnly()
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: `{"items":[` + strings.Join(items, ",") + `]}`}, {body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, nil, func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
		return errors.Join(exp.ConsumeLogs(ctx, logs), exp.ConsumeLogs(ctx, extra))
	})
	require.NoError(t, result.err)
	require.Equal(t, int64(126), result.failed)
	require.Len(t, result.points, 70, "64 specific tuples plus six status-preserving UNKNOWN buckets, shared across modes")
	var total, unknown int64
	wantUnknown := map[string]int64{"400": 57, "409": 1, "429": 1, "other_4xx": 1, "5xx": 1, "other": 1}
	for _, point := range result.points {
		total += point.Value
		field, _ := point.Attributes.Value("field_class")
		if field.AsString() == "unknown" {
			unknown += point.Value
			status, _ := point.Attributes.Value("item_status_class")
			want, exists := wantUnknown[status.AsString()]
			require.True(t, exists)
			assertRejectionTuple(t, point, status.AsString(), "unknown", "unknown", "unknown", "unknown", want)
			delete(wantUnknown, status.AsString())
		}
	}
	require.Equal(t, int64(126), total)
	require.Equal(t, int64(62), unknown)
	require.Empty(t, wantUnknown)
}

func TestRejectionResponseBodyConversionIsDerived(t *testing.T) {
	for _, test := range []struct {
		name     string
		body     func(pcommon.Value)
		wantKind string
	}{
		{name: "map", body: func(v pcommon.Value) { v.SetEmptyMap().PutStr("synthetic-key", "SYNTHETIC-SECRET") }, wantKind: "derived"},
		{name: "slice", body: func(v pcommon.Value) { v.SetEmptySlice().AppendEmpty().SetStr("SYNTHETIC-SECRET") }, wantKind: "derived"},
		{name: "integer", body: func(v pcommon.Value) { v.SetInt(42) }, wantKind: "derived"},
		{name: "bytes", body: func(v pcommon.Value) { v.SetEmptyBytes().FromRaw([]byte{1, 2}) }, wantKind: "derived"},
		{name: "text", body: func(v pcommon.Value) { v.SetStr("SYNTHETIC-SECRET") }, wantKind: "string"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := plog.NewLogs()
			record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			test.body(record.Body())
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [message] of type [text]"}}}]}`}}, nil)
			require.NoError(t, result.err)
			require.Len(t, result.points, 1)
			assertRejectionTuple(t, result.points[0], "400", "message", "text", test.wantKind, "body", 1)
		})
	}
}

func TestRejectionResponseAutomaticFlushAndSessionReuse(t *testing.T) {
	const response = `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`
	for _, test := range []struct {
		name      string
		automatic bool
	}{{name: "automatic", automatic: true}, {name: "successive_sessions"}} {
		t.Run(test.name, func(t *testing.T) {
			logs := oneRejectionLog()
			if test.automatic {
				records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
				records.At(0).CopyTo(records.AppendEmpty())
			}
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: response}, {body: response}}, func(config *Config) {
				if test.automatic {
					queue := exporterhelper.NewDefaultQueueConfig()
					queue.NumConsumers = 1
					queue.WaitForResult = true
					queue.Batch = configoptional.Some(exporterhelper.BatchConfig{Sizer: exporterhelper.RequestSizerTypeBytes, MinSize: 1, MaxSize: 100, FlushTimeout: time.Millisecond})
					config.QueueBatchConfig = configoptional.Some(queue)
				}
			}, func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
				err := exp.ConsumeLogs(ctx, logs)
				if !test.automatic {
					err = errors.Join(err, exp.ConsumeLogs(ctx, logs))
				}
				return err
			})
			require.NoError(t, result.err)
			require.Equal(t, int64(2), result.failed)
			require.Len(t, result.points, 1)
			assertRejectionTuple(t, result.points[0], "400", "error", "text", "map", "log", 2)
			require.Equal(t, result.requests[0], result.requests[1])
		})
	}
}

func TestRejectionResponseConcurrentSessionsStayIsolated(t *testing.T) {
	const count = 24
	responses := make([]rejectionHTTPResponse, count)
	for i := range responses {
		responses[i].body = `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`
	}
	result := runRejectionResponseFixture(t, oneRejectionLog(), responses, nil, func(exp exporter.Logs, ctx context.Context, logs plog.Logs) error {
		var wg sync.WaitGroup
		errorsCh := make(chan error, count)
		start := make(chan struct{})
		for range count {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; errorsCh <- exp.ConsumeLogs(ctx, logs) }()
		}
		close(start)
		wg.Wait()
		close(errorsCh)
		var result error
		for err := range errorsCh {
			result = errors.Join(result, err)
		}
		return result
	})
	require.NoError(t, result.err)
	require.Equal(t, int64(count), result.failed)
	require.LessOrEqual(t, len(result.points), 2)
	var total int64
	for _, point := range result.points {
		total += point.Value
		source, _ := point.Attributes.Value("source")
		if source.AsString() == "unknown" {
			assertRejectionTuple(t, point, "400", "error", "text", "unknown", "unknown", point.Value)
		} else {
			assertRejectionTuple(t, point, "400", "error", "text", "map", "log", point.Value)
		}
	}
	require.Equal(t, int64(count), total)
}

func TestRejectionResponseDuplicateAttributeKindsAreAmbiguous(t *testing.T) {
	var decoder plog.JSONUnmarshaler
	logs, err := decoder.UnmarshalLogs([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"synthetic"},"attributes":[{"key":"error","value":{"stringValue":"first"}},{"key":"error","value":{"kvlistValue":{"values":[{"key":"code","value":{"stringValue":"second"}}]}}}]}]}]}]}`))
	require.NoError(t, err)
	require.Equal(t, 2, logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Len(), "fixture retains both OTLP attributes")
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, nil)
	require.NoError(t, result.err)
	require.Len(t, result.points, 1)
	assertRejectionTuple(t, result.points[0], "400", "error", "text", "unknown", "mixed", 1)
}

func TestRejectionResponseUnknownEvidence(t *testing.T) {
	for _, test := range []struct{ name, reason, mode, wantField, wantExpected string }{
		{name: "missing_reason", wantField: "unknown", wantExpected: "unknown"},
		{name: "oversized_reason", reason: "failed to parse field [error] of type [text] in document " + strings.Repeat("SYNTHETIC-SECRET", 160), wantField: "unknown", wantExpected: "unknown"},
		{name: "unmodeled_mapping", mode: "none", reason: "failed to parse field [error] of type [text]", wantField: "error", wantExpected: "text"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := oneRejectionLog()
			if test.mode != "" {
				logs.ResourceLogs().At(0).ScopeLogs().At(0).Scope().Attributes().PutStr("elastic.mapping.mode", test.mode)
			}
			response := fmt.Sprintf(`{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":%q}}}]}`, test.reason)
			result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{{body: response}}, nil)
			require.NoError(t, result.err)
			require.Len(t, result.points, 1)
			assertRejectionTuple(t, result.points[0], "400", test.wantField, test.wantExpected, "unknown", "unknown", 1)
		})
	}
}

func TestRejectionResponseMixedTerminalAndRetryPositions(t *testing.T) {
	logs := oneRejectionLog()
	scope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	scope.Scope().Attributes().PutEmptyMap("error").PutStr("code", "retained")
	scope.LogRecords().AppendEmpty().Body().SetStr("synthetic retained")
	result := runRejectionResponseFixture(t, logs, []rejectionHTTPResponse{
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}},{"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"synthetic retry"}}}]}`},
		{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`},
	}, func(config *Config) {
		config.Retry.Enabled = true
		config.Retry.MaxRetries = 1
		config.Retry.InitialInterval = time.Millisecond
		config.Retry.MaxInterval = time.Millisecond
	})
	require.NoError(t, result.err)
	require.Equal(t, int64(2), result.failed)
	require.Len(t, result.points, 2)
	var known, unknown int64
	for _, point := range result.points {
		source, _ := point.Attributes.Value("source")
		if source.AsString() == "log" {
			known += point.Value
			assertRejectionTuple(t, point, "400", "error", "text", "map", "log", 1)
		} else {
			unknown += point.Value
			assertRejectionTuple(t, point, "400", "error", "text", "unknown", "unknown", 1)
		}
	}
	require.Equal(t, int64(1), known)
	require.Equal(t, int64(1), unknown)
}

func TestRejectionResponseDoesNotEmitRequestIdentifiers(t *testing.T) {
	result := runRejectionResponseFixture(t, oneRejectionLog(), []rejectionHTTPResponse{{body: `{"items":[{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`}}, nil)
	require.NoError(t, result.err)
	require.Len(t, result.points, 1)
	assertRejectionTuple(t, result.points[0], "400", "error", "text", "map", "log", 1)
	require.Greater(t, result.existingExemplars, 0, "control: sampled request context reaches existing counter exemplars")
	require.Empty(t, result.points[0].Exemplars, "diagnostics must not emit request trace/span IDs through exemplars")
}
