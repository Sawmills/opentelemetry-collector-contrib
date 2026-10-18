// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"compress/gzip"
	"context"
 "fmt"
 "strings"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadatatest"
)

func TestRejectedItemClassificationUsesItsOwnSource(t *testing.T) {
	testRejectedErrorAttribution(t, "scope", "map", "scope", `{"code":"rejected"}`)
}

func TestRejectedItemClassificationRecognizesECSExceptionAlias(t *testing.T) {
	testRejectedErrorAttribution(t, "alias", "derived", "log_alias", `{"message":"rejected"}`)
}

func TestRejectedItemClassificationRefusesDottedCollision(t *testing.T) {
 testRejectedErrorAttribution(t, "dotted", "unknown", "mixed", `{"code":"rejected","message":"dotted"}`)
}

func TestRejectedItemClassificationResourceSource(t *testing.T) {
 testRejectedErrorAttribution(t, "resource", "map", "resource", `{"code":"rejected"}`)
}

func TestRejectedItemClassificationLogSource(t *testing.T) {
 testRejectedErrorAttribution(t, "log", "map", "log", `{"code":"rejected"}`)
}

func TestRejectedItemClassificationRefusesAliasCollision(t *testing.T) {
 testRejectedErrorAttribution(t, "alias_collision", "unknown", "mixed", `{"code":"rejected","message":"rejected"}`)
}

func TestRejectedItemClassificationBoundsAttributeInspection(t *testing.T) {
 testRejectedErrorAttribution(t, "bound", "unknown", "unknown", `{"code":"rejected"}`)
}

func testRejectedErrorAttribution(t *testing.T, variant string, wantKind, wantSource, wantError string) {
	t.Helper()
	var requests atomic.Int64
	var mu sync.Mutex
	var submitted string
	server := newESTestServerBulkHandlerFunc(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer reader.Close()
			body = reader
		}
		data, err := io.ReadAll(body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		submitted = string(data)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, err = io.WriteString(w, `{"errors":true,"items":[{"create":{"_index":"logs-fixture","status":201}},{"create":{"_index":"logs-fixture","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [error] of type [text]"}}}]}`)
		if err != nil {
			t.Error(err)
		}
	})

	telemetry := componenttest.NewTelemetry()
	settings := metadatatest.NewSettings(telemetry)
	settings.ID = component.NewIDWithName(metadata.Type, "diagnostic")
	factory := NewFactory()
	config := factory.CreateDefaultConfig().(*Config)
	config.Endpoints = []string{server.URL}
	config.LogsIndex = "logs-fixture"
	config.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	config.Retry.Enabled = false
	exp, err := factory.CreateLogs(t.Context(), settings, config)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		require.NoError(t, exp.Shutdown(context.WithoutCancel(t.Context())))
	})

	logs := plog.NewLogs()
	acceptedScope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	accepted := acceptedScope.LogRecords().AppendEmpty()
	accepted.Body().SetStr("accepted synthetic")
	accepted.Attributes().PutEmptyMap("error").PutStr("code", "accepted")
	rejectedScope := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	rejected := rejectedScope.LogRecords().AppendEmpty()
	rejected.Body().SetStr("rejected synthetic")
	if variant == "alias" || variant == "alias_collision" {
  if variant == "alias_collision" {
   rejectedScope.Scope().Attributes().PutEmptyMap("error").PutStr("code", "rejected")
  }
		rejected.Attributes().PutStr("exception.message", "rejected")
	} else if variant == "resource" {
  logs.ResourceLogs().At(1).Resource().Attributes().PutEmptyMap("error").PutStr("code", "rejected")
 } else if variant == "log" {
  rejected.Attributes().PutEmptyMap("error").PutStr("code", "rejected")
 } else {
  rejectedScope.Scope().Attributes().PutEmptyMap("error").PutStr("code", "rejected")
  if variant == "dotted" {
   rejected.Attributes().PutStr("error.message", "dotted")
  }
	}
	if variant == "bound" {
  for i := range 128 {
   rejectedScope.Scope().Attributes().PutStr(fmt.Sprintf("pad_%03d", i), "synthetic")
  }
 }
 logs.MarkReadOnly()
	ctx := client.NewContext(t.Context(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{"X-Elastic-Mapping-Mode": {"ecs"}}),
	})
	require.NoError(t, exp.ConsumeLogs(ctx, logs))
	require.Equal(t, int64(1), requests.Load())

	mu.Lock()
	actual := submitted
	mu.Unlock()
	var padding strings.Builder
 if variant == "bound" {
  for i := range 128 {
   fmt.Fprintf(&padding, ",\"pad_%03d\":\"synthetic\"", i)
  }
 }
 want := "{\"create\":{\"_index\":\"logs-fixture\",\"require_data_stream\":true}}\n" +
		"{\"@timestamp\":\"1970-01-01T00:00:00.000000000Z\",\"error\":{\"code\":\"accepted\"},\"message\":\"accepted synthetic\"}\n" +
		"{\"create\":{\"_index\":\"logs-fixture\",\"require_data_stream\":true}}\n" +
		"{\"@timestamp\":\"1970-01-01T00:00:00.000000000Z\",\"error\":" + wantError + ",\"message\":\"rejected synthetic\"" + padding.String() + "}\n"
	require.Equal(t, want, actual, "diagnostics must preserve the submitted NDJSON")

	metric, err := telemetry.GetMetric("otelcol.elasticsearch.docs.failed_by_field")
	require.NoError(t, err)
	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	point := sum.DataPoints[0]
	require.Equal(t, int64(1), point.Value)
	require.ElementsMatch(t, []attribute.KeyValue{
		attribute.String("exporter", "elasticsearch/diagnostic"),
		attribute.String("item_status_class", "400"),
		attribute.String("field_class", "error"),
		attribute.String("expected_type", "text"),
		attribute.String("value_type", wantKind),
		attribute.String("source", wantSource),
	}, point.Attributes.ToSlice())
}
