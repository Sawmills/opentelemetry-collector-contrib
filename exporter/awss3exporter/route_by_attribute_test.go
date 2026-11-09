// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"
)

const routeAttr = "sawmills.service"

type routedRecord struct {
	resourceService string
	recordService   string
	body            string
}

// newRoutedLogs builds one ResourceLogs per distinct resourceService (in order
// of first appearance), each with a single scope holding its records.
func newRoutedLogs(records ...routedRecord) plog.Logs {
	ld := plog.NewLogs()
	byResource := map[string]plog.LogRecordSlice{}
	for _, r := range records {
		lrs, ok := byResource[r.resourceService]
		if !ok {
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("host.name", "host-1")
			if r.resourceService != "" {
				rl.Resource().Attributes().PutStr(routeAttr, r.resourceService)
			}
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName("scope-1")
			lrs = sl.LogRecords()
			byResource[r.resourceService] = lrs
		}
		lr := lrs.AppendEmpty()
		lr.Body().SetStr(r.body)
		if r.recordService != "" {
			lr.Attributes().PutStr(routeAttr, r.recordService)
		}
	}
	return ld
}

func bodies(ld plog.Logs) []string {
	var out []string
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		sls := ld.ResourceLogs().At(i).ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				out = append(out, lrs.At(k).Body().Str())
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestRouteFolder(t *testing.T) {
	tests := []struct {
		value  string
		folder string
		ok     bool
	}{
		{value: "payments", folder: "payments", ok: true},
		{value: "Payments-API_v2.1", folder: "Payments-API_v2.1", ok: true},
		{value: "team payments", folder: "team payments", ok: true},
		{value: "a/b", folder: "a_b", ok: true},
		{value: "/leading/and/trailing/", folder: "_leading_and_trailing_", ok: true},
		{value: "", ok: false},
		{value: ".", ok: false},
		{value: "..", ok: false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.value), func(t *testing.T) {
			folder, ok := routeFolder(tt.value)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.folder, folder)
		})
	}
}

func TestSplitLogsByRoute(t *testing.T) {
	ld := newRoutedLogs(
		routedRecord{resourceService: "", recordService: "payments", body: "p1"},
		routedRecord{resourceService: "", recordService: "orders", body: "o1"},
		routedRecord{resourceService: "", recordService: "payments", body: "p2"},
		routedRecord{resourceService: "", body: "none1"},
		routedRecord{resourceService: "auth", body: "a1"},                          // resource fallback
		routedRecord{resourceService: "auth", recordService: "orders", body: "o2"}, // record wins over resource
		routedRecord{resourceService: "", recordService: "..", body: "unsafe1"},
	)

	groups := splitLogsByRoute(ld, routeAttr)

	got := map[string][]string{}
	for _, g := range groups {
		got[g.folder] = bodies(g.logs)
	}
	assert.Equal(t, map[string][]string{
		"payments": {"p1", "p2"},
		"orders":   {"o1", "o2"},
		"auth":     {"a1"},
		"":         {"none1", "unsafe1"},
	}, got)

	for _, g := range groups {
		for i := 0; i < g.logs.ResourceLogs().Len(); i++ {
			rl := g.logs.ResourceLogs().At(i)
			host, ok := rl.Resource().Attributes().Get("host.name")
			require.True(t, ok, "resource attributes must be preserved")
			assert.Equal(t, "host-1", host.Str())
			assert.Equal(t, "scope-1", rl.ScopeLogs().At(0).Scope().Name(), "scope must be preserved")
		}
	}
}

func TestRoutePrefix(t *testing.T) {
	assert.Equal(t, "logs/payments", routePrefix("logs", "payments"))
	assert.Equal(t, "logs/payments", routePrefix("logs/", "payments"))
	assert.Equal(t, "logs/destination_id=abc/payments", routePrefix("logs/destination_id=abc/", "payments"))
	assert.Equal(t, "payments", routePrefix("", "payments"))
}

func TestRouteHiveFolder(t *testing.T) {
	assert.Equal(t, "sawmills_service=payments", hiveFolder("sawmills.service", "payments"))
	assert.Equal(t, "sawmills_service=__default__", hiveFolder("sawmills.service", ""))
	assert.Equal(t, "service=a_b", hiveFolder("service", "a_b"))
}

type routeUpload struct {
	prefix string
	data   string
}

type recordingUploader struct {
	mu      sync.Mutex
	uploads []routeUpload
}

func (r *recordingUploader) Upload(_ context.Context, buf []byte, opts *upload.UploadOptions) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prefix := "<default>"
	if opts != nil && opts.OverridePrefix != "" {
		prefix = opts.OverridePrefix
	}
	r.uploads = append(r.uploads, routeUpload{prefix: prefix, data: string(buf)})
	return int64(len(buf)), nil
}

func (r *recordingUploader) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.uploads)
}

// bufferingMarshaler mimics a buffering encoding extension (e.g. ndjson):
// MarshalLogs only buffers, FlushLogs returns and clears the buffer.
type bufferingMarshaler struct {
	mu  sync.Mutex
	buf []string
}

func (m *bufferingMarshaler) MarshalLogs(ld plog.Logs) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buf = append(m.buf, bodies(ld)...)
	return nil, nil
}

func (m *bufferingMarshaler) FlushLogs() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.buf) == 0 {
		return nil, nil
	}
	out := strings.Join(m.buf, ",")
	m.buf = nil
	return []byte(out), nil
}

func (*bufferingMarshaler) MarshalTraces(ptrace.Traces) ([]byte, error)    { return nil, nil }
func (*bufferingMarshaler) MarshalMetrics(pmetric.Metrics) ([]byte, error) { return nil, nil }
func (*bufferingMarshaler) Format() string                                 { return "txt" }
func (*bufferingMarshaler) Compressed() bool                               { return false }

const (
	paymentsPrefix = "logs/sawmills_service=payments"
	ordersPrefix   = "logs/sawmills_service=orders"
	defaultPrefix  = "logs/sawmills_service=__default__"
)

func otlpJSON(t *testing.T) func() (marshaler, error) {
	t.Helper()
	return func() (marshaler, error) { return awss3marshaler.NewMarshaler("otlp_json") }
}

func batchingOtlpJSON(t *testing.T, maxFileSizeBytes int) func() (marshaler, error) {
	t.Helper()
	return func() (marshaler, error) {
		return awss3marshaler.NewMarshalerWithConfig("otlp_json", maxFileSizeBytes, zap.NewNop())
	}
}

func bufferingExtension() (marshaler, error) { return &bufferingMarshaler{}, nil }

func newRoutingExporter(t *testing.T, newSink func() (marshaler, error), up upload.Manager, configure ...func(*Config)) *s3Exporter {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.S3Uploader.S3Prefix = "logs"
	cfg.RouteByAttribute.Attribute = routeAttr
	for _, c := range configure {
		c(cfg)
	}
	exp := &s3Exporter{config: cfg, uploader: up, logger: zap.NewNop()}
	exp.routes = newLogRoutes(cfg.RouteByAttribute, newSink)
	return exp
}

func uploadsByPrefix(up *recordingUploader) map[string]string {
	up.mu.Lock()
	defer up.mu.Unlock()
	out := map[string]string{}
	for _, u := range up.uploads {
		out[u.prefix] += u.data
	}
	return out
}

func routedTestLogs(batch string) plog.Logs {
	return newRoutedLogs(
		routedRecord{recordService: "payments", body: "PAY-1" + batch},
		routedRecord{recordService: "orders", body: "ORD-1" + batch},
		routedRecord{recordService: "payments", body: "PAY-2" + batch},
		routedRecord{body: "NONE-1" + batch},
	)
}

func assertRouted(t *testing.T, up *recordingUploader, batches ...string) {
	t.Helper()
	got := uploadsByPrefix(up)
	require.ElementsMatch(t, []string{paymentsPrefix, ordersPrefix, defaultPrefix}, keys(got))
	for _, b := range batches {
		assert.Contains(t, got[paymentsPrefix], "PAY-1"+b)
		assert.Contains(t, got[paymentsPrefix], "PAY-2"+b)
		assert.Contains(t, got[ordersPrefix], "ORD-1"+b)
		assert.Contains(t, got[defaultPrefix], "NONE-1"+b)
	}
	assert.NotContains(t, got[paymentsPrefix], "ORD-")
	assert.NotContains(t, got[paymentsPrefix], "NONE-")
	assert.NotContains(t, got[ordersPrefix], "PAY-")
	assert.NotContains(t, got[ordersPrefix], "NONE-")
	assert.NotContains(t, got[defaultPrefix], "PAY-")
	assert.NotContains(t, got[defaultPrefix], "ORD-")
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRouteByAttributeNonBufferingMarshaler(t *testing.T) {
	up := &recordingUploader{}
	exp := newRoutingExporter(t, otlpJSON(t), up)

	require.NoError(t, exp.ConsumeLogs(t.Context(), routedTestLogs("a")))
	assert.Equal(t, 3, up.count(), "one upload per folder in the batch")
	assertRouted(t, up, "a")
}

func TestRouteByAttributeBuffersPerFolderUntilFlush(t *testing.T) {
	for name, newSink := range map[string]func() (marshaler, error){
		"batching otlp_json": batchingOtlpJSON(t, defaultMaxFileSizeBytes),
		"encoding extension": bufferingExtension,
	} {
		t.Run(name, func(t *testing.T) {
			up := &recordingUploader{}
			exp := newRoutingExporter(t, newSink, up)

			require.NoError(t, exp.ConsumeLogs(t.Context(), routedTestLogs("a")))
			require.NoError(t, exp.ConsumeLogs(t.Context(), routedTestLogs("b")))
			assert.Zero(t, up.count(), "batches stay buffered per folder")

			require.NoError(t, exp.flushMarshaler(t.Context(), "timer"))
			assert.Equal(t, 3, up.count(), "one object per folder per flush, regardless of batch count")
			assertRouted(t, up, "a", "b")

			require.NoError(t, exp.flushMarshaler(t.Context(), "shutdown"))
			assert.Equal(t, 3, up.count(), "empty folders are not uploaded")
		})
	}
}

func TestRouteByAttributeFlushesFolderOnSize(t *testing.T) {
	up := &recordingUploader{}
	exp := newRoutingExporter(t, batchingOtlpJSON(t, 1), up)

	require.NoError(t, exp.ConsumeLogs(t.Context(), routedTestLogs("a")))
	assert.Equal(t, 3, up.count())
	assertRouted(t, up, "a")
}

func TestRouteByAttributeNestsUnderPrefixWithSuffix(t *testing.T) {
	up := &recordingUploader{}
	exp := newRoutingExporter(t, otlpJSON(t), up, func(cfg *Config) {
		cfg.S3Uploader.S3Prefix = "logs/destination_id=abc/"
	})

	require.NoError(t, exp.ConsumeLogs(t.Context(), newRoutedLogs(routedRecord{recordService: "payments", body: "PAY-1"})))
	require.Len(t, up.uploads, 1)
	assert.Equal(t, "logs/destination_id=abc/sawmills_service=payments", up.uploads[0].prefix)
}

func TestRouteByAttributeDisabledKeepsDefaultBehavior(t *testing.T) {
	m, err := awss3marshaler.NewMarshaler("otlp_json")
	require.NoError(t, err)
	up := &recordingUploader{}
	cfg := createDefaultConfig().(*Config)
	cfg.S3Uploader.S3Prefix = "logs"
	exp := &s3Exporter{config: cfg, uploader: up, logger: zap.NewNop(), marshaler: m}

	require.NoError(t, exp.ConsumeLogs(t.Context(), routedTestLogs("a")))
	require.Len(t, up.uploads, 1, "without routing a batch is a single upload")
	assert.Equal(t, "<default>", up.uploads[0].prefix)
}

func TestRouteByAttributeConcurrentBatchesDoNotMix(t *testing.T) {
	up := &recordingUploader{}
	exp := newRoutingExporter(t, bufferingExtension, up)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := fmt.Sprintf("svc-%d", i%4)
			assert.NoError(t, exp.ConsumeLogs(t.Context(),
				newRoutedLogs(routedRecord{recordService: svc, body: fmt.Sprintf("%s-msg-%d", svc, i)})))
		}(i)
	}
	wg.Wait()
	require.NoError(t, exp.flushMarshaler(t.Context(), "timer"))

	require.Equal(t, 4, up.count())
	total := 0
	for _, u := range up.uploads {
		svc := strings.TrimPrefix(u.prefix, "logs/sawmills_service=")
		for b := range strings.SplitSeq(u.data, ",") {
			assert.True(t, strings.HasPrefix(b, svc+"-msg-"), "upload to %s contains %q", u.prefix, b)
			total++
		}
	}
	assert.Equal(t, 20, total)
}

func TestRouteByAttributeSoftCap(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(context.WithoutCancel(t.Context()))) })
	up := &recordingUploader{}
	exp := newRoutingExporter(t, otlpJSON(t), up, func(cfg *Config) {
		cfg.RouteByAttribute.MaxFolders = 2
	})
	exp.telemetry = newExporterTelemetry(tel.NewTelemetrySettings(), exp.logger)

	require.NoError(t, exp.ConsumeLogs(t.Context(), newRoutedLogs(
		routedRecord{recordService: "a", body: "A-1"},
		routedRecord{recordService: "b", body: "B-1"},
		routedRecord{recordService: "c", body: "C-1"},
		routedRecord{recordService: "c", body: "C-2"},
		routedRecord{body: "NONE-1"},
	)))
	require.NoError(t, exp.ConsumeLogs(t.Context(), newRoutedLogs(routedRecord{recordService: "a", body: "A-2"})))

	got := uploadsByPrefix(up)
	require.ElementsMatch(t, []string{"logs/sawmills_service=a", "logs/sawmills_service=b", defaultPrefix}, keys(got))
	assert.Contains(t, got["logs/sawmills_service=a"], "A-2", "existing folders keep routing after the cap is reached")
	assert.Contains(t, got[defaultPrefix], "C-1")
	assert.Contains(t, got[defaultPrefix], "C-2")
	assert.Contains(t, got[defaultPrefix], "NONE-1")

	assert.Equal(t, int64(2), requireSumPoint(t, tel, "otelcol_exporter_awss3_route_overflow_records_total").Value)
	folders, err := tel.GetMetric("otelcol_exporter_awss3_route_folders")
	require.NoError(t, err)
	points := folders.Data.(metricdata.Gauge[int64]).DataPoints
	require.Len(t, points, 1)
	assert.Equal(t, int64(2), points[0].Value)
}

// routingEncodingExtension is an encoding extension that can create
// independent encoders, one per folder.
type routingEncodingExtension struct{ encodingExtension }

func (routingEncodingExtension) NewLogEncoder() (plog.Marshaler, error) {
	return &bufferingMarshaler{}, nil
}

func TestRouteByAttributeStartWithEncodingExtension(t *testing.T) {
	newExp := func() *s3Exporter {
		cfg := createDefaultConfig().(*Config)
		cfg.S3Uploader.Region = "us-east-1"
		cfg.S3Uploader.S3Bucket = "bucket"
		cfg.S3Uploader.S3Partition = ""
		cfg.RouteByAttribute.Attribute = routeAttr
		id := component.MustNewID("foo")
		cfg.Encoding = &id
		return newS3Exporter(cfg, "logs", exportertest.NewNopSettings(component.MustNewType("awss3")))
	}

	exp := newExp()
	err := exp.start(t.Context(), hostWithExtensions{encoding: encodingExtension{}})
	assert.ErrorContains(t, err, "route_by_attribute")

	exp = newExp()
	require.NoError(t, exp.start(t.Context(), hostWithExtensions{encoding: routingEncodingExtension{}}))
	require.NotNil(t, exp.routes)
	first, err := exp.routes.newSink()
	require.NoError(t, err)
	second, err := exp.routes.newSink()
	require.NoError(t, err)
	assert.NotSame(t, first, second, "each folder gets its own encoder")
}

func TestRouteByAttributeConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, confmap.NewFromStringMap(map[string]any{
		"s3uploader":         map[string]any{"region": "us-east-1", "s3_bucket": "b", "s3_prefix": "logs"},
		"route_by_attribute": map[string]any{"attribute": routeAttr},
	}).Unmarshal(cfg))
	assert.Equal(t, routeAttr, cfg.RouteByAttribute.Attribute)
	assert.Equal(t, 200, newLogRoutes(cfg.RouteByAttribute, nil).maxFolders, "max_folders defaults to 200")
	require.NoError(t, cfg.Validate())

	cfg.RouteByAttribute.MaxFolders = -1
	assert.ErrorContains(t, cfg.Validate(), "route_by_attribute.max_folders")
	cfg.RouteByAttribute.MaxFolders = 0

	cfg.ResourceAttrsToS3.S3Prefix = "other"
	assert.ErrorContains(t, cfg.Validate(), "route_by_attribute")

	// Archive recovery retains a single archive; a routed batch uploads several.
	cfg.ResourceAttrsToS3.S3Prefix = ""
	cfg.ArchiveRecovery = true
	assert.ErrorContains(t, cfg.Validate(), "route_by_attribute cannot be combined with archive_recovery")
}

func TestRouteValueNestedAttribute(t *testing.T) {
	newMap := func(fill func(pcommon.Map)) pcommon.Map {
		m := pcommon.NewMap()
		fill(m)
		return m
	}
	tests := []struct {
		name  string
		attrs pcommon.Map
		want  string
		found bool
	}{
		{"flat key", newMap(func(m pcommon.Map) { m.PutStr("sawmills.service", "flat") }), "flat", true},
		{"nested map (BHN shape)", newMap(func(m pcommon.Map) {
			s := m.PutEmptyMap("sawmills")
			s.PutStr("service", "tango")
			s.PutEmptySlice("sampled_by").AppendEmpty().SetStr("x")
		}), "tango", true},
		{"flat key wins over nested", newMap(func(m pcommon.Map) {
			m.PutStr("sawmills.service", "flat")
			m.PutEmptyMap("sawmills").PutStr("service", "nested")
		}), "flat", true},
		{"partially dotted nesting", newMap(func(m pcommon.Map) {
			m.PutEmptyMap("a").PutStr("b.c", "v")
		}), "", false},
		{"missing", newMap(func(m pcommon.Map) { m.PutEmptyMap("sawmills").PutStr("other", "x") }), "", false},
		{"non-map parent", newMap(func(m pcommon.Map) { m.PutStr("sawmills", "x") }), "", false},
		{"non-string value", newMap(func(m pcommon.Map) { m.PutEmptyMap("sawmills").PutInt("service", 7) }), "7", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := lookupAttribute(tt.attrs, routeAttr)
			assert.Equal(t, tt.found, found)
			assert.Equal(t, tt.want, got)
		})
	}

	ab := pcommon.NewMap()
	ab.PutEmptyMap("a").PutStr("b.c", "v")
	got, found := lookupAttribute(ab, "a.b.c")
	assert.True(t, found, "a key segment may itself contain dots")
	assert.Equal(t, "v", got)
}

func TestSplitLogsByRouteNestedAttribute(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutEmptyMap("sawmills").PutStr("service", "from-resource")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	lr := lrs.AppendEmpty()
	lr.Body().SetStr("r1")
	lr.Attributes().PutEmptyMap("sawmills").PutStr("service", "tango")
	lrs.AppendEmpty().Body().SetStr("r2")

	got := map[string][]string{}
	for _, g := range splitLogsByRoute(ld, routeAttr) {
		got[g.folder] = bodies(g.logs)
	}
	assert.Equal(t, map[string][]string{"tango": {"r1"}, "from-resource": {"r2"}}, got)
}
