// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
)

var errArchiveRetained = errors.New("archive retained for retry")

// The admission token covers both encoder mutation and pending archive ownership.
// A failed upload transfers that token to the retry worker. No subsequent call
// can reset the encoder or allocate another archive until the worker succeeds.
type archiveRecovery struct {
	gate         chan struct{}
	ctx          context.Context
	cancel       context.CancelFunc
	workers      sync.WaitGroup
	pending      upload.PreparedUpload
	meta         flushMetadata
	archiveBytes int64
}

func newArchiveRecovery() *archiveRecovery {
	ctx, cancel := context.WithCancel(context.Background())
	r := &archiveRecovery{gate: make(chan struct{}, 1), ctx: ctx, cancel: cancel}
	r.gate <- struct{}{}
	return r
}

func (e *s3Exporter) withArchiveAdmission(ctx context.Context, action func() error) error {
	r := e.recovery
	if r == nil {
		return action()
	}
	if err := r.acquire(ctx); err != nil {
		return err
	}
	err := action()
	if errors.Is(err, errArchiveRetained) {
		r.workers.Add(1)
		go e.retryArchive()
		// All input in this archive is now owned by the exporter. Returning an
		// error here would invite upstream to resend only the last input batch.
		return nil
	}
	r.release()
	return err
}

func (r *archiveRecovery) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.ctx.Done():
		return errors.Join(ctx.Err(), r.ctx.Err())
	case <-r.gate:
	}
	// Cancellation can race a free slot. Reject before touching the encoder.
	if err := errors.Join(ctx.Err(), r.ctx.Err()); err != nil {
		r.release()
		return err
	}
	return nil
}

func (r *archiveRecovery) release() { r.gate <- struct{}{} }

func (e *s3Exporter) attemptArchive(ctx context.Context) error {
	r := e.recovery
	attemptCtx, cancel := context.WithTimeout(ctx, e.config.TimeoutSettings.Timeout)
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	defer cancel()
	started := time.Now()
	e.telemetry.recordUploadStart(attemptCtx, e.signalType)
	n, err := r.pending.Upload(attemptCtx)
	e.telemetry.recordUploadComplete(attemptCtx, e.signalType, started, time.Since(started), n, r.meta, err)
	if err != nil {
		e.logger.Error("S3 archive upload failed; archive retained for retry", zap.String("operation", "upload"), zap.String("stage", "archive"), zap.Error(err))
		return err
	}
	r.pending = nil
	r.archiveBytes = 0
	e.telemetry.recordRetainedArchive(ctx, e.signalType, 0, 0)
	return nil
}

func (e *s3Exporter) retryArchive() {
	r := e.recovery
	defer r.workers.Done()
	defer r.release()
	delay := backoff.NewExponentialBackOff()
	for {
		timer := time.NewTimer(delay.NextBackOff())
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := e.attemptArchive(r.ctx); err == nil {
			return
		}
	}
}

func (e *s3Exporter) shutdownArchives(ctx context.Context) (result error) {
	if e.recovery != nil {
		stop := context.AfterFunc(ctx, e.recovery.cancel)
		defer stop()
		defer func() {
			e.recovery.cancel()
			e.recovery.workers.Wait()
			if result != nil && e.recovery.pending != nil {
				result = fmt.Errorf("%w; retained_archives=1 retained_archive_bytes=%d (memory only)", result, e.recovery.archiveBytes)
			}
		}()
	}
	e.shutOnce.Do(func() {
		close(e.done)
	})
	if e.timerCancel != nil {
		// Stop scheduling immediately, but allow an active upload to finish.
		stop := context.AfterFunc(ctx, e.timerCancel)
		defer stop()
		defer e.timerCancel()
	}
	e.timerWG.Wait()
	defer func() { result = errors.Join(result, e.timerErr) }()
	if err := e.flushMarshaler(ctx, "shutdown"); err != nil {
		return fmt.Errorf("S3 archive shutdown incomplete: %w", err)
	}
	if e.recovery != nil {
		// The final flush can itself start a retry. Wait for its acknowledgement.
		if err := e.recovery.acquire(ctx); err != nil {
			return fmt.Errorf("S3 archive shutdown incomplete: %w", err)
		}
		e.recovery.release()
	}
	return nil
}

// exporterhelper drains its queue before invoking the exporter shutdown hook.
// Cancel recovery when the shutdown deadline expires so consumers waiting for
// archive admission cannot prevent the helper from reaching that hook.
type archiveRecoveryLifecycle struct {
	component.Component
	recovery *archiveRecovery
}

func (c *archiveRecoveryLifecycle) Shutdown(ctx context.Context) error {
	stop := context.AfterFunc(ctx, c.recovery.cancel)
	defer stop()
	return c.Component.Shutdown(ctx)
}

func (e *s3Exporter) helperTimeout() exporterhelper.TimeoutConfig {
	if e.recovery != nil {
		// The configured timeout applies to each S3 attempt, not admission.
		return exporterhelper.TimeoutConfig{}
	}
	return e.config.TimeoutSettings
}

func (e *s3Exporter) lifecycle(c component.Component) component.Component {
	if e.recovery == nil {
		return c
	}
	return &archiveRecoveryLifecycle{Component: c, recovery: e.recovery}
}
