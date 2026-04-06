// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
)

var (
	s3PrefixKey    = "_sourceHost"
	s3BucketKey    = "_sourceBucket"
	overridePrefix = "host"
	overrideBucket = "my-bucket"
	testLogs       = fmt.Sprintf(`{"resourceLogs":[{"resource":{"attributes":[{"key":"_sourceCategory","value":{"stringValue":"logfile"}},{"key":"%s","value":{"stringValue":"%s"}},{"key":"%s","value":{"stringValue":"%s"}}]},"scopeLogs":[{"scope":{},"logRecords":[{"observedTimeUnixNano":"1654257420681895000","body":{"stringValue":"2022-06-03 13:57:00.62739 +0200 CEST m=+14.018296742 log entry14"},"attributes":[{"key":"log.file.path_resolved","value":{"stringValue":"logwriter/data.log"}}]}]}],"schemaUrl":"https://opentelemetry.io/schemas/1.6.1"}]}`, s3PrefixKey, overridePrefix, s3BucketKey, overrideBucket) //nolint:gocritic //sprintfQuotedString for JSON
)

type testWriter struct {
	t            *testing.T
	expectedOpts *upload.UploadOptions
}

func (testWriter *testWriter) Upload(_ context.Context, buf []byte, uploadOpts *upload.UploadOptions) error {
	assert.JSONEq(testWriter.t, testLogs, string(buf))
	assert.Equal(testWriter.t, testWriter.expectedOpts, uploadOpts)
	return nil
}

func getTestLogs(tb testing.TB) plog.Logs {
	logsMarshaler := plog.JSONUnmarshaler{}
	logs, err := logsMarshaler.UnmarshalLogs([]byte(testLogs))
	assert.NoError(tb, err, "Can't unmarshal testing the logs data -> %s", err)
	return logs
}

func init() {
	_ = os.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func getLogExporter(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	exporter := &s3Exporter{
		config:    createDefaultConfig().(*Config),
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverridePrefix: ""}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLog(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporter(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

func getLogExporterWithResourceAttrs(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	config := createDefaultConfig().(*Config)
	config.ResourceAttrsToS3.S3Prefix = s3PrefixKey
	exporter := &s3Exporter{
		config:    config,
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverridePrefix: overridePrefix}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLogWithResourceAttrs(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporterWithResourceAttrs(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

func getLogExporterWithBucketAndPrefixAttrs(t *testing.T) *s3Exporter {
	marshaler, _ := newMarshaler("otlp_json", zap.NewNop())
	config := createDefaultConfig().(*Config)
	config.ResourceAttrsToS3.S3Bucket = s3BucketKey
	config.ResourceAttrsToS3.S3Prefix = s3PrefixKey
	exporter := &s3Exporter{
		config:    config,
		uploader:  &testWriter{t: t, expectedOpts: &upload.UploadOptions{OverrideBucket: overrideBucket, OverridePrefix: overridePrefix}},
		logger:    zap.NewNop(),
		marshaler: marshaler,
	}
	return exporter
}

func TestLogWithBucketAndPrefixAttrs(t *testing.T) {
	logs := getTestLogs(t)
	exporter := getLogExporterWithBucketAndPrefixAttrs(t)
	assert.NoError(t, exporter.ConsumeLogs(t.Context(), logs))
}

type flushMetadataMarshaler struct {
	buf       []byte
	flushMeta flushMetadata
}

func (*flushMetadataMarshaler) MarshalTraces(ptrace.Traces) ([]byte, error) { return nil, nil }

func (m *flushMetadataMarshaler) MarshalLogs(plog.Logs) ([]byte, error) {
	return m.buf, nil
}

func (m *flushMetadataMarshaler) MarshalLogsWithFlushMetadata(
	plog.Logs,
) ([]byte, flushMetadata, error) {
	return m.buf, m.flushMeta, nil
}

func (*flushMetadataMarshaler) MarshalMetrics(pmetric.Metrics) ([]byte, error) { return nil, nil }

func (*flushMetadataMarshaler) format() string { return "parquet" }

func (*flushMetadataMarshaler) compressed() bool { return false }

type uploaderStub struct {
	err         error
	uploadCalls int
	delay       time.Duration
}

func (u *uploaderStub) Upload(context.Context, []byte, *upload.UploadOptions) error {
	u.uploadCalls++
	if u.delay > 0 {
		time.Sleep(u.delay)
	}
	return u.err
}

func TestExporterRecordsEquivalentFlushAndUploadTelemetry(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	tel := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, tel.Shutdown(context.WithoutCancel(t.Context())))
	})

	settings := exportertest.NewNopSettings(component.MustNewType("awss3"))
	settings.TelemetrySettings = tel.NewTelemetrySettings()

	exporter := newS3Exporter(createDefaultConfig().(*Config), "logs", settings)
	const (
		expectedReason = "manual"
		handoffGap     = 50 * time.Millisecond
		uploadDelay    = 200 * time.Millisecond
	)
	exporter.marshaler = &flushMetadataMarshaler{
		buf: []byte("payload"),
		flushMeta: flushMetadata{
			reason:           expectedReason,
			flushCompletedAt: time.Now().Add(-handoffGap),
		},
	}
	uploader := &uploaderStub{delay: uploadDelay}
	exporter.uploader = uploader

	require.NoError(t, exporter.ConsumeLogs(t.Context(), getTestLogs(t)))
	require.Equal(t, 1, uploader.uploadCalls)

	assertMetricDataPointCount(t, tel, "otelcol_exporter_awss3_flush_start_total")
	assertMetricDataPointCount(t, tel, "otelcol_exporter_awss3_flush_complete_total")
	assertMetricDataPointCount(t, tel, "otelcol_exporter_awss3_upload_start_total")
	assertMetricDataPointCount(t, tel, "otelcol_exporter_awss3_upload_complete_total")
	assertMetricDataPointCount(t, tel, "otelcol_exporter_awss3_flush_duration")
	uploadDurationPoint := requireHistogramPoint(t, tel, "otelcol_exporter_awss3_upload_duration")
	flushToUploadPoint := requireHistogramPoint(t, tel, "otelcol_exporter_awss3_flush_to_upload_duration")
	flushCompletePoint := requireSumPoint(t, tel, "otelcol_exporter_awss3_flush_complete_total")
	uploadCompletePoint := requireSumPoint(t, tel, "otelcol_exporter_awss3_upload_complete_total")

	assert.Equal(t, int64(1), flushCompletePoint.Value)
	assert.Equal(t, int64(1), uploadCompletePoint.Value)
	assertMetricAttribute(t, flushCompletePoint.Attributes, "reason", expectedReason)
	assertMetricAttribute(t, flushCompletePoint.Attributes, "signal", "logs")
	assertMetricAttribute(t, flushCompletePoint.Attributes, "outcome", "success")
	assertMetricAttribute(t, uploadCompletePoint.Attributes, "reason", expectedReason)
	assertMetricAttribute(t, uploadCompletePoint.Attributes, "signal", "logs")
	assertMetricAttribute(t, uploadCompletePoint.Attributes, "outcome", "success")
	assertMetricAttribute(t, flushToUploadPoint.Attributes, "reason", expectedReason)
	assertMetricAttribute(t, flushToUploadPoint.Attributes, "signal", "logs")
	assertMetricAttribute(t, flushToUploadPoint.Attributes, "outcome", "success")

	assert.GreaterOrEqual(t, uploadDurationPoint.Sum, uploadDelay.Milliseconds())
	assert.GreaterOrEqual(t, flushToUploadPoint.Sum, handoffGap.Milliseconds())
	assert.Less(t, flushToUploadPoint.Sum, uploadDurationPoint.Sum)
}

func assertMetricDataPointCount(t *testing.T, tel *componenttest.Telemetry, name string) {
	t.Helper()

	metric, err := tel.GetMetric(name)
	require.NoError(t, err)

	switch data := metric.Data.(type) {
	case metricdata.Sum[int64]:
		require.Len(t, data.DataPoints, 1)
	case metricdata.Histogram[int64]:
		require.Len(t, data.DataPoints, 1)
	default:
		t.Fatalf("unexpected metric type %T for %s", metric.Data, name)
	}
}

func requireHistogramPoint(t *testing.T, tel *componenttest.Telemetry, name string) metricdata.HistogramDataPoint[int64] {
	t.Helper()

	metric, err := tel.GetMetric(name)
	require.NoError(t, err)

	histogram, ok := metric.Data.(metricdata.Histogram[int64])
	require.True(t, ok, "metric %s is not a histogram", name)
	require.Len(t, histogram.DataPoints, 1)
	return histogram.DataPoints[0]
}

func requireSumPoint(t *testing.T, tel *componenttest.Telemetry, name string) metricdata.DataPoint[int64] {
	t.Helper()

	metric, err := tel.GetMetric(name)
	require.NoError(t, err)

	sum, ok := metric.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %s is not a sum", name)
	require.Len(t, sum.DataPoints, 1)
	return sum.DataPoints[0]
}

func assertMetricAttribute(t *testing.T, attrs attribute.Set, key, want string) {
	t.Helper()

	value, ok := attrs.Value(attribute.Key(key))
	require.True(t, ok, "missing attribute %s", key)
	assert.Equal(t, want, value.AsString())
}
