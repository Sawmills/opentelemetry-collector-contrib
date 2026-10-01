// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package upload // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfermanagertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/klauspost/compress/zstd"
	"github.com/tilinna/clock"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.uber.org/zap"
)

type Manager interface {
	Upload(ctx context.Context, data []byte, opts *UploadOptions) (int64, error)
}

// PreparedUpload owns an archive and its destination across upload attempts.
// Call Upload serially; each attempt starts with the complete immutable body.
type PreparedUpload interface {
	Upload(context.Context) (int64, error)
}

type PreparingManager interface {
	Manager
	Prepare(context.Context, []byte, *UploadOptions) PreparedUpload
}

// CleanupDeadline shares a shutdown deadline with uploads that outlive their
// attempt context, including multipart cleanup already in progress.
type CleanupDeadline struct {
	mu       sync.RWMutex
	deadline time.Time
	shutdown context.Context
	updated  chan struct{}
}

// Set records the deadline and cancellation signal from a shutdown context.
func (d *CleanupDeadline) Set(shutdown context.Context) {
	deadline, ok := shutdown.Deadline()
	if !ok {
		return
	}
	d.mu.Lock()
	d.deadline = deadline
	d.shutdown = shutdown
	if d.updated != nil {
		close(d.updated)
	}
	d.updated = make(chan struct{})
	d.mu.Unlock()
}

// Get returns the current shutdown deadline, when one is configured.
func (d *CleanupDeadline) Get() (time.Time, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.deadline.IsZero() {
		return time.Time{}, false
	}
	return d.deadline, true
}

// Watch cancels an active cleanup when shutdown reaches its deadline.
func (d *CleanupDeadline) Watch(ctx context.Context, cancel context.CancelFunc) func() {
	d.mu.Lock()
	if d.updated == nil {
		d.updated = make(chan struct{})
	}
	d.mu.Unlock()
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			d.mu.RLock()
			shutdown, updated := d.shutdown, d.updated
			d.mu.RUnlock()
			if updated == nil {
				updated = make(chan struct{})
			}
			var shutdownDone <-chan struct{}
			var stopAfter func() bool
			if shutdown != nil {
				shutdownDone = shutdown.Done()
				stopAfter = context.AfterFunc(shutdown, cancel)
			}
			select {
			case <-ctx.Done():
				if stopAfter != nil {
					stopAfter()
				}
				return
			case <-stop:
				if stopAfter != nil {
					stopAfter()
				}
				return
			case <-shutdownDone:
				return
			case <-updated:
				if stopAfter != nil {
					stopAfter()
				}
			}
		}
	}()
	return func() { once.Do(func() { close(stop) }) }
}

type cleanupDeadlineKey struct{}

// WithCleanupDeadline bounds multipart cleanup by an enclosing shutdown
// deadline while leaving ordinary per-attempt cancellation independent.
func WithCleanupDeadline(ctx context.Context, deadline *CleanupDeadline) context.Context {
	return context.WithValue(ctx, cleanupDeadlineKey{}, deadline)
}

type ManagerOpt func(Manager)

type UploadOptions struct {
	OverrideBucket string
	OverridePrefix string
}

type s3manager struct {
	logger       *zap.Logger
	bucket       string
	builder      *PartitionKeyBuilder
	uploader     *transfermanager.Client
	service      *s3.Client
	storageClass s3types.StorageClass
	acl          s3types.ObjectCannedACL
}

var _ Manager = (*s3manager)(nil)

func NewS3Manager(logger *zap.Logger, bucket string, builder *PartitionKeyBuilder, service *s3.Client, storageClass s3types.StorageClass, opts ...ManagerOpt) Manager {
	manager := &s3manager{
		logger:       logger,
		bucket:       bucket,
		builder:      builder,
		uploader:     transfermanager.New(service),
		service:      service,
		storageClass: storageClass,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(manager)
		}
	}

	return manager
}

func (sw *s3manager) Upload(ctx context.Context, data []byte, opts *UploadOptions) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return sw.Prepare(ctx, data, opts).Upload(ctx)
}

type preparedUpload struct {
	manager  *s3manager
	raw      []byte
	content  []byte
	input    transfermanager.UploadObjectInput
	uploadID string
}

func (sw *s3manager) Prepare(ctx context.Context, data []byte, opts *UploadOptions) PreparedUpload {
	prefix, bucket := "", sw.bucket
	if opts != nil {
		prefix = opts.OverridePrefix
		if opts.OverrideBucket != "" {
			bucket = opts.OverrideBucket
		}
	}
	input := transfermanager.UploadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(sw.builder.Build(clock.Now(ctx), prefix)),
		StorageClass: transfermanagertypes.StorageClass(sw.storageClass),
		ACL:          transfermanagertypes.ObjectCannedACL(sw.acl),
	}
	// Archive compression belongs to the file, not HTTP Content-Encoding.
	if sw.builder.Compression.IsCompressed() && !sw.builder.IsCompressed {
		input.ContentEncoding = aws.String(string(sw.builder.Compression))
	}
	return &preparedUpload{manager: sw, raw: data, input: input}
}

func (p *preparedUpload) Upload(ctx context.Context) (int64, error) {
	// Do not create another multipart upload while parts from a failed attempt
	// remain. An expired attempt context cannot clean those parts up.
	if err := p.abortMultipart(ctx); err != nil {
		return int64(len(p.content)), err
	}
	if p.content == nil {
		content, err := p.manager.contentBuffer(p.raw)
		if err != nil {
			return 0, err
		}
		p.content = content.Bytes()
		p.raw = nil
	}
	input := p.input
	input.Body = bytes.NewReader(p.content)
	p.manager.logger.Debug("uploading object", zap.String("bucket", *input.Bucket), zap.String("key", *input.Key))
	_, err := p.manager.uploader.UploadObject(ctx, &input)
	var multipartErr transfermanager.MultipartUploadError
	if errors.As(err, &multipartErr) {
		p.uploadID = multipartErr.UploadID()
		err = errors.Join(err, p.abortMultipart(ctx))
	}
	return int64(len(p.content)), err
}

func (p *preparedUpload) abortMultipart(ctx context.Context) error {
	if p.uploadID == "" {
		return nil
	}
	cleanupDeadline := time.Now().Add(5 * time.Second)
	if shutdown, ok := ctx.Value(cleanupDeadlineKey{}).(*CleanupDeadline); ok {
		if deadline, ok := shutdown.Get(); ok && deadline.Before(cleanupDeadline) {
			cleanupDeadline = deadline
		}
	}
	cleanupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), cleanupDeadline)
	defer cancel()
	if shutdown, ok := ctx.Value(cleanupDeadlineKey{}).(*CleanupDeadline); ok {
		defer shutdown.Watch(cleanupCtx, cancel)()
	}
	_, err := p.manager.service.AbortMultipartUpload(cleanupCtx, &s3.AbortMultipartUploadInput{
		Bucket: p.input.Bucket, Key: p.input.Key, UploadId: aws.String(p.uploadID),
	})
	var missing *s3types.NoSuchUpload
	if err != nil && !errors.As(err, &missing) {
		p.manager.logger.Error("Failed to clean up multipart archive", zap.String("operation", "upload"), zap.String("stage", "multipart_cleanup"), zap.Error(err))
		return err
	}
	p.uploadID = ""
	return nil
}

func (sw *s3manager) contentBuffer(raw []byte) (*bytes.Buffer, error) {
	switch sw.builder.Compression {
	case configcompression.TypeGzip:
		content := bytes.NewBuffer(nil)

		zipper := gzip.NewWriter(content)
		if _, err := zipper.Write(raw); err != nil {
			return nil, err
		}
		if err := zipper.Close(); err != nil {
			return nil, err
		}

		return content, nil
	case configcompression.TypeZstd:
		content := bytes.NewBuffer(nil)
		zipper, err := zstd.NewWriter(content)
		if err != nil {
			return nil, err
		}
		_, err = zipper.Write(raw)
		if err != nil {
			return nil, err
		}
		err = zipper.Close()
		if err != nil {
			return nil, err
		}

		return content, nil
	default:
		return bytes.NewBuffer(raw), nil
	}
}

func WithACL(acl s3types.ObjectCannedACL) func(Manager) {
	return func(m Manager) {
		s3m, ok := m.(*s3manager)
		if !ok {
			return
		}
		s3m.acl = acl
	}
}
