// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter_test

import (
	"context"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
)

// Run with -benchtime=100x to bound the retained backlog identically in both modes.
func BenchmarkCentralQueueStorageAdmission(b *testing.B) {
	for _, durable := range []bool{false, true} {
		name := "memory"
		if durable {
			name = "fsync"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			extensions := make(map[component.ID]component.Component)
			var ext component.Component
			if durable {
				factory := filestorage.NewFactory()
				cfg := factory.CreateDefaultConfig().(*filestorage.Config)
				cfg.Directory = dir
				cfg.FSync = true
				var err error
				ext, err = factory.Create(b.Context(), extensiontest.NewNopSettings(factory.Type()), cfg)
				require.NoError(b, err)
				require.NoError(b, ext.Start(b.Context(), componenttest.NewNopHost()))
				extensions[component.MustNewIDWithName("file_storage", "queue")] = ext
			}
			central := map[string]any{"enabled": true, "shutdown_timeout": "1ms", "max_compressed_bytes": 64 << 20, "max_uncompressed_batch_bytes": 1 << 20, "max_inflight_uncompressed_bytes": 4 << 20, "target_compressed_bytes": 128 << 10, "max_batch_delay": "10ms", "num_consumers": 1, "lane_count": 1}
			if durable {
				central["storage"] = "file_storage/queue"
			}
			factory := loadbalancingexporter.NewFactory()
			cfg := factory.CreateDefaultConfig()
			require.NoError(b, confmap.NewFromStringMap(map[string]any{
				"protocol":      map[string]any{"otlp": map[string]any{"tls": map[string]any{"insecure": true}}},
				"resolver":      map[string]any{"static": map[string]any{"hostnames": []string{"127.0.0.1:1"}}},
				"central_queue": central,
			}).Unmarshal(cfg))
			settings := exportertest.NewNopSettings(factory.Type())
			settings.ID = component.NewIDWithName(factory.Type(), "ordinal-0")
			exp, err := factory.CreateLogs(b.Context(), settings, cfg)
			require.NoError(b, err)
			require.NoError(b, exp.Start(b.Context(), durableTestHost{extensions: extensions}))
			input := plog.NewLogs()
			records := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			random := mathrand.NewChaCha8([32]byte{1})
			for range 64 {
				payload := make([]byte, 768)
				_, err = random.Read(payload)
				require.NoError(b, err)
				records.AppendEmpty().Body().SetEmptyBytes().FromRaw(payload)
			}
			b.ReportAllocs()
			b.SetBytes(64 * 768)
			b.ResetTimer()
			for range b.N {
				require.NoError(b, exp.ConsumeLogs(b.Context(), input))
			}
			b.StopTimer()
			ctx, cancel := context.WithTimeout(context.WithoutCancel(b.Context()), time.Second)
			defer cancel()
			_ = exp.Shutdown(ctx) // Destination is deliberately unavailable.
			if ext != nil {
				require.NoError(b, ext.Shutdown(context.WithoutCancel(b.Context())))
			}
			var diskBytes int64
			require.NoError(b, filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				info, err := entry.Info()
				if err == nil {
					diskBytes += info.Size()
				}
				return err
			}))
			b.ReportMetric(float64(diskBytes), "disk-bytes")
			if durable {
				storageFactory := filestorage.NewFactory()
				storageConfig := storageFactory.CreateDefaultConfig().(*filestorage.Config)
				storageConfig.Directory = dir
				storageConfig.FSync = true
				reopenedStore, err := storageFactory.Create(b.Context(), extensiontest.NewNopSettings(storageFactory.Type()), storageConfig)
				require.NoError(b, err)
				require.NoError(b, reopenedStore.Start(b.Context(), componenttest.NewNopHost()))
				reopened, err := factory.CreateLogs(b.Context(), settings, cfg)
				require.NoError(b, err)
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				require.NoError(b, reopened.Start(b.Context(), durableTestHost{extensions: map[component.ID]component.Component{component.MustNewIDWithName("file_storage", "queue"): reopenedStore}}))
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(elapsed.Nanoseconds()), "replay-ns")
				b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc), "replay-allocated-bytes")
				stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(b.Context()), time.Second)
				defer stopCancel()
				_ = reopened.Shutdown(stopCtx)
				require.NoError(b, reopenedStore.Shutdown(context.WithoutCancel(b.Context())))
			}
		})
	}
}
