// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type archiveWrite struct {
	key  string
	body string
}

type archiveServer struct {
	ambiguous atomic.Bool
	block     atomic.Bool
	fail      atomic.Bool
	writes    chan archiveWrite
	release   chan struct{}
}

func archiveTestConfig(t *testing.T) (*Config, *archiveServer) {
	t.Helper()
	sink := &archiveServer{writes: make(chan archiveWrite, 100), release: make(chan struct{})}
	sink.fail.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fail := sink.fail.Load()
		ambiguous := sink.ambiguous.Swap(false)
		block := sink.block.Load()
		sink.writes <- archiveWrite{key: r.URL.Path, body: string(body)}
		if block {
			select {
			case <-r.Context().Done():
				return
			case <-sink.release:
			}
		}
		if ambiguous {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if fail {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(server.Close)
	cfg := createDefaultConfig().(*Config)
	cfg.ArchiveRecovery = true
	cfg.S3Uploader.Endpoint = server.URL
	cfg.S3Uploader.S3Bucket = "archives"
	cfg.S3Uploader.S3ForcePathStyle = true
	cfg.S3Uploader.AccessKeyID = "test"
	cfg.S3Uploader.SecretAccessKey = "test"
	cfg.S3Uploader.RetryMode = "nop"
	cfg.TimeoutSettings.Timeout = time.Second
	return cfg, sink
}

func newArchiveTestExporter(t *testing.T, configure ...func(*Config)) (*s3Exporter, *archiveServer) {
	t.Helper()
	cfg, sink := archiveTestConfig(t)
	for _, option := range configure {
		option(cfg)
	}
	exp := newS3Exporter(cfg, "logs", exportertest.NewNopSettings(component.MustNewType("awss3")))
	require.NoError(t, exp.start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		sink.fail.Store(false)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		_ = exp.shutdown(ctx)
	})
	return exp, sink
}

func TestArchiveRecoveryRetainsEarlierBatchesAfterFailedFlush(t *testing.T) {
	exp, sink := newArchiveTestExporter(t)
	logs := getTestLogs(t)
	require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
	require.NoError(t, exp.ConsumeLogs(t.Context(), logs))

	err := exp.flushMarshaler(t.Context(), "timer")
	sink.fail.Store(false)
	first := receiveArchive(t, sink)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, err)
	require.NoError(t, exp.shutdown(ctx))
	require.Equal(t, first, receiveArchive(t, sink))
	require.Equal(t, 2, strings.Count(first.body, "log entry14"))
}

func receiveArchive(t *testing.T, sink *archiveServer) archiveWrite {
	t.Helper()
	select {
	case w := <-sink.writes:
		return w
	case <-time.After(10 * time.Second):
		t.Fatal("S3 did not receive the retained archive")
		return archiveWrite{}
	}
}

func TestArchiveRecoveryRejectsCanceledInputBeforeEncoding(t *testing.T) {
	exp, sink := newArchiveTestExporter(t)
	logs := getTestLogs(t)
	require.NoError(t, exp.ConsumeLogs(t.Context(), logs))
	require.NoError(t, exp.flushMarshaler(t.Context(), "timer"))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	err := exp.ConsumeLogs(canceled, logs)
	sink.fail.Store(false)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, exp.shutdown(t.Context()))
	first := receiveArchive(t, sink)
	require.Equal(t, first, receiveArchive(t, sink))
}

func TestArchiveRecoveryRetriesSameObjectAfterLostResponse(t *testing.T) {
	exp, sink := newArchiveTestExporter(t)
	sink.fail.Store(false)
	sink.ambiguous.Store(true)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))

	err := exp.flushMarshaler(t.Context(), "timer")
	require.NoError(t, err)
	require.NoError(t, exp.shutdown(t.Context()))
	first := receiveArchive(t, sink)
	require.Equal(t, first, receiveArchive(t, sink))
}

func TestArchiveRecoveryReportsIncompleteShutdownDuringOutage(t *testing.T) {
	exp, _ := newArchiveTestExporter(t)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	require.NoError(t, exp.flushMarshaler(t.Context(), "timer"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := exp.shutdown(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func newQueuedArchiveExporter(t *testing.T) (exporter.Logs, *archiveServer) {
	t.Helper()
	cfg, sink := archiveTestConfig(t)
	cfg.MaxFileSizeBytes = 1
	queue := exporterhelper.NewDefaultQueueConfig()
	queue.NumConsumers = 2
	queue.QueueSize = 10
	cfg.QueueSettings = configoptional.Some(queue)
	exp, err := createLogsExporter(t.Context(), exportertest.NewNopSettings(component.MustNewType("awss3")), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	return exp, sink
}

func TestArchiveRecoveryQueuedShutdownStopsBlockedConsumers(t *testing.T) {
	exp, sink := newQueuedArchiveExporter(t)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	receiveArchive(t, sink)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := exp.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestArchiveRecoveryQueuedShutdownDrainsAfterRecovery(t *testing.T) {
	exp, sink := newQueuedArchiveExporter(t)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	first := receiveArchive(t, sink)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	sink.fail.Store(false)

	err := exp.Shutdown(t.Context())
	require.NoError(t, err)
	require.Equal(t, first, receiveArchive(t, sink))
	require.Equal(t, first.body, receiveArchive(t, sink).body)
}

func TestArchiveRecoverySerializesConcurrentArchives(t *testing.T) {
	exp, sink := newQueuedArchiveExporter(t)
	sink.fail.Store(false)
	var senders sync.WaitGroup
	senders.Add(2)
	go func() { defer senders.Done(); _ = exp.ConsumeLogs(t.Context(), getTestLogs(t)) }()
	go func() { defer senders.Done(); _ = exp.ConsumeLogs(t.Context(), getTestLogs(t)) }()
	senders.Wait()

	err := exp.Shutdown(t.Context())
	require.NoError(t, err)
	first := receiveArchive(t, sink)
	require.Equal(t, first.body, receiveArchive(t, sink).body)
}

func TestArchiveRecoveryContinuesAfterUploadCallerCancellation(t *testing.T) {
	exp, sink := newArchiveTestExporter(t)
	sink.fail.Store(false)
	sink.block.Store(true)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- exp.flushMarshaler(ctx, "timer") }()
	first := receiveArchive(t, sink)

	sink.block.Store(false)
	cancel()
	require.NoError(t, <-result)
	require.NoError(t, exp.shutdown(t.Context()))
	require.Equal(t, first, receiveArchive(t, sink))
}

func TestArchiveRecoveryPreservesCompressedOversizedArchive(t *testing.T) {
	exp, sink := newArchiveTestExporter(t, func(c *Config) {
		c.MaxFileSizeBytes = 1
		c.S3Uploader.Compression = configcompression.TypeGzip
	})
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	first := receiveArchive(t, sink)
	sink.fail.Store(false)

	err := exp.shutdown(t.Context())
	require.NoError(t, err)
	require.Equal(t, first, receiveArchive(t, sink))
	require.Contains(t, decompressArchive(t, first.body), "log entry14")
}

func decompressArchive(t *testing.T, body string) string {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewBufferString(body))
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(content)
}

func TestArchiveRecoveryReportsRetainedArchiveAndAttemptMetrics(t *testing.T) {
	exp, sink := newArchiveTestExporter(t)
	tel := componenttest.NewTelemetry()
	defer func() { _ = tel.Shutdown(context.WithoutCancel(t.Context())) }()
	exp.telemetry = newExporterTelemetry(tel.NewTelemetrySettings(), exp.logger)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	require.NoError(t, exp.flushMarshaler(t.Context(), "timer"))
	sink.fail.Store(false)

	require.NoError(t, exp.shutdown(t.Context()))
	require.Equal(t, int64(1), requireSumPoint(t, tel, uploadFailedMetricName).Value)
	require.Equal(t, int64(0), requireGaugePoint(t, tel, "otelcol_exporter_awss3_retained_archives").Value)
	require.Equal(t, int64(0), requireGaugePoint(t, tel, "otelcol_exporter_awss3_retained_archive_bytes").Value)
}

func TestArchiveRecoveryRequiresBoundedUploadAttempts(t *testing.T) {
	cfg, _ := archiveTestConfig(t)
	cfg.TimeoutSettings.Timeout = 0
	require.ErrorContains(t, cfg.Validate(), "archive_recovery requires a positive timeout")
}

func TestArchiveRecoveryMetricsSeparateDestinations(t *testing.T) {
	tel := componenttest.NewTelemetry()
	defer func() { _ = tel.Shutdown(context.WithoutCancel(t.Context())) }()
	settings := exportertest.NewNopSettings(component.MustNewType("awss3"))
	settings.ID = component.MustNewIDWithName("awss3", "failed")
	settings.TelemetrySettings = tel.NewTelemetrySettings()
	failed := newS3Exporter(createDefaultConfig().(*Config), "logs", settings)
	settings.ID = component.MustNewIDWithName("awss3", "healthy")
	healthy := newS3Exporter(createDefaultConfig().(*Config), "logs", settings)

	failed.telemetry.recordRetainedArchive(t.Context(), "logs", 1, 100)
	healthy.telemetry.recordRetainedArchive(t.Context(), "logs", 0, 0)
	m, err := tel.GetMetric("otelcol_exporter_awss3_retained_archives")
	require.NoError(t, err)
	require.Len(t, m.Data.(metricdata.Gauge[int64]).DataPoints, 2)
}

func TestArchiveRecoveryRejectsUnsafeQueues(t *testing.T) {
	for _, mode := range []string{"disabled", "wait_for_result", "block_on_overflow", "storage"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _ := archiveTestConfig(t)
			queue := exporterhelper.NewDefaultQueueConfig()
			switch mode {
			case "wait_for_result":
				queue.WaitForResult = true
			case "block_on_overflow":
				queue.BlockOnOverflow = true
			case "storage":
				id := component.MustNewID("file_storage")
				queue.StorageID = &id
			}
			if mode != "disabled" {
				cfg.QueueSettings = configoptional.Some(queue)
			}
			require.ErrorContains(t, cfg.Validate(), "archive_recovery requires")
		})
	}
}

func TestShutdownAllowsActiveTimerUploadToFinishWithoutRecovery(t *testing.T) {
	exp, sink := newArchiveTestExporter(t, func(c *Config) { c.ArchiveRecovery = false })
	sink.fail.Store(false)
	sink.block.Store(true)
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	timerCtx, cancel := context.WithCancel(t.Context())
	exp.timerCancel = cancel
	exp.timerWG.Add(1)
	uploadResult := make(chan error, 1)
	go func() {
		defer exp.timerWG.Done()
		uploadResult <- exp.flushMarshaler(timerCtx, "timer")
	}()
	receiveArchive(t, sink)
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- exp.shutdown(t.Context()) }()
	<-exp.done
	// Closing the scheduler must leave the active request's context usable.
	contextErr := timerCtx.Err()
	close(sink.release)
	require.NoError(t, <-shutdownResult)
	require.NoError(t, contextErr)
	require.NoError(t, <-uploadResult)
}

func TestShutdownReportsFailedTimerUploadWithoutRecovery(t *testing.T) {
	exp, sink := newArchiveTestExporter(t, func(c *Config) { c.ArchiveRecovery = false })
	require.NoError(t, exp.ConsumeLogs(t.Context(), getTestLogs(t)))
	exp.flushOnTimer(t.Context())
	sink.fail.Store(false)
	require.ErrorContains(t, exp.shutdown(t.Context()), "503")
}

func TestArchiveAdmissionPreservesExpiredCallerDeadline(t *testing.T) {
	recovery := newArchiveRecovery()
	recovery.cancel()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	for range 100 {
		require.ErrorIs(t, recovery.acquire(ctx), context.DeadlineExceeded)
	}
}
