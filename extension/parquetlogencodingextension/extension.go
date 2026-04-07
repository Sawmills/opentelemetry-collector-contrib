// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters/datadog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters/snowflake"
)

const (
	flushReasonManual = "manual"
	flushReasonSize   = "size"
)

type parquetLogExtension struct {
	memFile               *MemFile
	logger                *zap.Logger
	ctx                   context.Context
	config                *Config
	mutex                 sync.Mutex
	pendingRecords        []any
	oldestPendingRecord   time.Time
	pendingEstimatedBytes int64
	writer                *writer.ParquetWriter
	adapter               adapters.ParquetAdapter
	telemetry             *parquetTelemetry
	maxFileSizeBytes      int64
	fileName              string
	oldestBufferedRecord  time.Time
	bufferStateTickerDone chan struct{}
	bufferStateTickerStop chan struct{}
	bufferStateInterval   time.Duration
	writeStopFn           func() error
	readWriterBytes       func() ([]byte, error)
	reinitializeWriterFn  func() error
	nowFn                 func() time.Time
	recordBufferStateFn   func(int, int64, int64)
	recordFlushFn         func(string, int64, int64, time.Duration)
	writeRecordFn         func(any) error
}

type bufferedStateSnapshot struct {
	records              []any
	oldestBufferedRecord time.Time
}

func NewParquetLogExtension(
	ctx context.Context,
	params extension.Settings,
	baseCfg component.Config,
) (extension.Extension, error) {
	cfg, ok := baseCfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected parquet config type %T", baseCfg)
	}

	telemetry, err := newParquetTelemetry(params)
	if err != nil {
		return nil, err
	}

	adapter, err := newParquetAdapter(cfg, params, telemetry)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet adapter: %w", err)
	}

	ext := &parquetLogExtension{
		logger:              params.Logger,
		ctx:                 context.WithoutCancel(ctx),
		config:              cfg,
		adapter:             adapter,
		telemetry:           telemetry,
		maxFileSizeBytes:    cfg.MaxFileSizeBytes,
		fileName:            fmt.Sprintf("parquet-log-%d.parquet", time.Now().UnixNano()),
		bufferStateInterval: 5 * time.Second,
		nowFn:               time.Now,
	}
	ext.writeStopFn = ext.defaultWriteStop
	ext.readWriterBytes = ext.defaultReadWriterBytes
	ext.reinitializeWriterFn = ext.initializeWriter
	ext.recordBufferStateFn = ext.telemetry.recordBufferState
	ext.recordFlushFn = ext.telemetry.recordFlush
	ext.writeRecordFn = ext.Write

	if err := ext.initializeWriter(); err != nil {
		return nil, err
	}
	ext.recordBufferStateLocked()

	return ext, nil
}

func newParquetAdapter(
	cfg *Config,
	params extension.Settings,
	telemetry *parquetTelemetry,
) (adapters.ParquetAdapter, error) {
	switch strings.ToLower(cfg.Schema) {
	case "", defaultSchema:
		return datadog.NewDatadogParquetAdapter(params)
	case "snowflake":
		var recordDropFn func(string, int64)
		if telemetry != nil {
			recordDropFn = telemetry.recordDroppedRecords
		}
		return snowflake.NewSnowflakeParquetAdapter(params, snowflake.Config{
			AttributesHotKeys: cfg.snowflakeAttributesHotKeys(),
			TagsHotKeys:       cfg.snowflakeTagsHotKeys(),
			RecordDropFn:      recordDropFn,
		})
	default:
		return nil, fmt.Errorf("unsupported parquet schema: %s", cfg.Schema)
	}
}

func (e *parquetLogExtension) initializeWriter() error {
	if e.config.CompressionCodec == "" {
		e.config.CompressionCodec = defaultCompressionCodec
	}
	if e.config.PageSizeBytes <= 0 {
		e.config.PageSizeBytes = defaultPageSizeBytes
	}
	if e.config.RowGroupSizeBytes <= 0 {
		e.config.RowGroupSizeBytes = defaultRowGroupSizeBytes
	}

	memFile, err := NewMemFileWriter(e.fileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create memory file writer: %w", err)
	}

	pw, err := writer.NewParquetWriter(memFile, e.adapter.Schema(), e.config.NumberOfGoRoutines)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	compressionType, err := parquetCompressionCodec(e.config.CompressionCodec)
	if err != nil {
		return err
	}

	pw.CompressionType = compressionType
	pw.PageSize = e.config.PageSizeBytes
	pw.RowGroupSize = e.config.RowGroupSizeBytes
	e.writer = pw
	e.memFile = memFile.(*MemFile)

	return nil
}

func (e *parquetLogExtension) Start(context.Context, component.Host) error {
	e.startBufferStateLoop()
	return nil
}

func (e *parquetLogExtension) Shutdown(context.Context) error {
	e.mutex.Lock()
	if len(e.pendingRecords) > 0 {
		if err := e.movePendingRecordsIntoWriterLocked(); err != nil {
			e.mutex.Unlock()
			e.stopBufferStateLoop()
			e.telemetry.shutdown()
			return fmt.Errorf("move pending parquet spill payloads into buffer: %w", err)
		}
	}
	if e.writer != nil && len(e.writer.Objs) > 0 {
		e.mutex.Unlock()
		return errors.New("buffered parquet records remain at shutdown; flush before shutdown")
	}
	e.mutex.Unlock()

	e.stopBufferStateLoop()
	e.telemetry.shutdown()
	return nil
}

func (e *parquetLogExtension) startBufferStateLoop() {
	if e.bufferStateInterval <= 0 {
		return
	}

	e.mutex.Lock()
	if e.bufferStateTickerStop != nil {
		e.mutex.Unlock()
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	e.bufferStateTickerStop = stopCh
	e.bufferStateTickerDone = doneCh
	interval := e.bufferStateInterval
	e.mutex.Unlock()

	go func() {
		defer close(doneCh)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				e.mutex.Lock()
				e.recordBufferStateLocked()
				e.mutex.Unlock()
			case <-stopCh:
				return
			}
		}
	}()
}

func (e *parquetLogExtension) stopBufferStateLoop() {
	e.mutex.Lock()
	stopCh := e.bufferStateTickerStop
	doneCh := e.bufferStateTickerDone
	e.bufferStateTickerStop = nil
	e.bufferStateTickerDone = nil
	e.mutex.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}

func (e *parquetLogExtension) FlushLogs() ([]byte, error) {
	return e.FlushLogsWithReason(flushReasonManual)
}

func (e *parquetLogExtension) FlushLogsWithReason(reason string) ([]byte, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	buf, _, _, err := e.flushWithPendingLocked(reason)
	return buf, err
}

func (e *parquetLogExtension) FlushLogsWithReasonAndMetadata(
	reason string,
) ([]byte, string, time.Time, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.flushWithPendingLocked(reason)
}

func (e *parquetLogExtension) flushWithPendingLocked(
	reason string,
) ([]byte, string, time.Time, error) {
	if len(e.pendingRecords) > 0 {
		buf, flushReason, completedAt, err := e.addRecordsWithFlushMetadataLocked(nil)
		if err != nil {
			return nil, "", time.Time{}, err
		}
		if len(buf) > 0 {
			if reason != "" {
				flushReason = reason
			}
			return buf, flushReason, completedAt, nil
		}
	}

	return e.checkAndFlushWithMetadata(true, reason)
}

func (e *parquetLogExtension) MarshalLogs(ld plog.Logs) ([]byte, error) {
	buf, _, _, err := e.MarshalLogsWithFlushMetadata(ld)
	return buf, err
}

func (e *parquetLogExtension) MarshalLogsWithFlushMetadata(
	ld plog.Logs,
) ([]byte, string, time.Time, error) {
	toParquet, err := e.adapter.ConvertToParquet(e.ctx, ld)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	if len(toParquet) == 0 {
		return nil, "", time.Time{}, nil
	}

	buf, reason, completedAt, err := e.addLogRecordWithFlushMetadata(toParquet)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	if buf != nil {
		return buf, reason, completedAt, nil
	}
	return nil, "", time.Time{}, nil
}

func (e *parquetLogExtension) addLogRecordWithFlushMetadata(
	records []any,
) ([]byte, string, time.Time, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.addRecordsWithFlushMetadataLocked(records)
}

func (e *parquetLogExtension) addRecordsWithFlushMetadataLocked(
	records []any,
) ([]byte, string, time.Time, error) {
	if err := e.ensureWriterLocked(); err != nil {
		return nil, "", time.Time{}, err
	}

	pendingRecordCount := len(e.pendingRecords)
	pendingOldestRecord := e.oldestPendingRecord
	if len(e.pendingRecords) > 0 {
		records = append(append(make([]any, 0, len(e.pendingRecords)+len(records)), e.pendingRecords...), records...)
		e.clearPendingRecordsLocked()
	}

	if len(records) > 0 && len(e.writer.Objs) == 0 {
		e.oldestBufferedRecord = e.nowFn()
		if !pendingOldestRecord.IsZero() {
			e.oldestBufferedRecord = pendingOldestRecord
		}
	}

	var flushedBytes []byte
	var flushReason string
	var flushCompletedAt time.Time

	for i, record := range records {
		if e.oldestBufferedRecord.IsZero() {
			e.oldestBufferedRecord = e.nowFn()
		}
		if err := e.writeRecordLocked(record); err != nil {
			e.appendPendingRecordsLocked(
				records[i:],
				tailOldestRecord(pendingRecordCount, pendingOldestRecord, i, e.nowFn()),
				e.estimatePendingBytesFromBufferedState(
					len(e.writer.Objs),
					e.getCurrentCompressedSize(),
					len(records[i:]),
				),
			)
			return nil, "", time.Time{}, fmt.Errorf("failed to write record: %w", err)
		}
		e.recordBufferStateLocked()
		if e.getCurrentCompressedSize() < e.maxFileSizeBytes {
			continue
		}
		bufferedRecordCount := len(e.writer.Objs)
		bufferedCompressedBytes := e.getCurrentCompressedSize()

		buf, reason, completedAt, err := e.checkAndFlushWithMetadata(false, flushReasonSize)
		if err != nil {
			if i+1 < len(records) {
				e.appendPendingRecordsLocked(
					records[i+1:],
					tailOldestRecord(pendingRecordCount, pendingOldestRecord, i+1, e.nowFn()),
					e.estimatePendingBytesFromBufferedState(
						bufferedRecordCount,
						bufferedCompressedBytes,
						len(records[i+1:]),
					),
				)
			}
			return nil, "", time.Time{}, err
		}
		if len(buf) == 0 {
			continue
		}
		flushedBytes = buf
		flushReason = reason
		flushCompletedAt = completedAt
		if i+1 < len(records) {
			e.appendPendingRecordsLocked(
				records[i+1:],
				tailOldestRecord(pendingRecordCount, pendingOldestRecord, i+1, flushCompletedAt),
				e.estimatePendingBytesFromBufferedState(
					bufferedRecordCount,
					bufferedCompressedBytes,
					len(records[i+1:]),
				),
			)
		}
		break
	}
	e.recordBufferStateLocked()

	if flushedBytes != nil {
		return flushedBytes, flushReason, flushCompletedAt, nil
	}

	return nil, "", time.Time{}, nil
}

func (e *parquetLogExtension) checkAndFlushWithMetadata(
	forceFlush bool,
	reason string,
) ([]byte, string, time.Time, error) {
	if e.writer == nil {
		return nil, "", time.Time{}, nil
	}

	if e.getCurrentCompressedSize() < e.maxFileSizeBytes && !forceFlush {
		return nil, "", time.Time{}, nil
	}

	e.telemetry.recordFlushAttempt(reason)
	bufferedRecords := len(e.writer.Objs)
	if bufferedRecords == 0 {
		e.oldestBufferedRecord = time.Time{}
		e.recordBufferStateLocked()
		e.telemetry.recordEmptyFlush(reason)
		return nil, "", time.Time{}, nil
	}

	flushStartedAt := e.nowFn()
	snapshot := e.snapshotBufferedStateLocked()

	if err := e.writeStopFn(); err != nil {
		e.telemetry.recordFailedFlush(reason)
		e.logger.Error("failed to stop parquet writer", zap.Error(err))
		if restoreErr := e.restoreBufferedStateLocked(snapshot); restoreErr != nil {
			e.logger.Error("failed to restore buffered parquet state", zap.Error(restoreErr))
			return nil, "", time.Time{}, errors.Join(
				fmt.Errorf("write stop failed: %w", err),
				fmt.Errorf("restore buffered parquet state: %w", restoreErr),
			)
		}
		return nil, "", time.Time{}, err
	}

	pqBytes, err := e.readWriterBytes()
	if err != nil {
		e.telemetry.recordFailedFlush(reason)
		e.logger.Error("failed to read parquet bytes", zap.Error(err))
		if restoreErr := e.restoreBufferedStateLocked(snapshot); restoreErr != nil {
			e.logger.Error("failed to restore buffered parquet state", zap.Error(restoreErr))
			return nil, "", time.Time{}, errors.Join(
				fmt.Errorf("read parquet bytes failed: %w", err),
				fmt.Errorf("restore buffered parquet state: %w", restoreErr),
			)
		}
		return nil, "", time.Time{}, err
	}

	if err := e.reinitializeWriterFn(); err != nil {
		e.telemetry.recordFailedFlush(reason)
		e.logger.Error("failed to reinitialize parquet writer", zap.Error(err))
		e.writer = nil
		e.memFile = nil
		e.oldestBufferedRecord = time.Time{}
		e.recordBufferStateLocked()
		completedAt := e.nowFn()
		e.recordFlushFn(
			reason,
			int64(bufferedRecords),
			int64(len(pqBytes)),
			completedAt.Sub(flushStartedAt),
		)
		return pqBytes, reason, completedAt, nil
	}

	completedAt := e.nowFn()
	e.recordFlushFn(
		reason,
		int64(bufferedRecords),
		int64(len(pqBytes)),
		completedAt.Sub(flushStartedAt),
	)
	e.oldestBufferedRecord = time.Time{}
	e.recordBufferStateLocked()

	return pqBytes, reason, completedAt, nil
}

func (e *parquetLogExtension) snapshotBufferedStateLocked() bufferedStateSnapshot {
	snapshot := bufferedStateSnapshot{
		records:              make([]any, len(e.writer.Objs)),
		oldestBufferedRecord: e.oldestBufferedRecord,
	}
	copy(snapshot.records, e.writer.Objs)
	return snapshot
}

func (e *parquetLogExtension) restoreBufferedStateLocked(snapshot bufferedStateSnapshot) error {
	if err := e.reinitializeWriterFn(); err != nil {
		return err
	}

	e.oldestBufferedRecord = snapshot.oldestBufferedRecord
	for _, record := range snapshot.records {
		if err := e.Write(record); err != nil {
			return fmt.Errorf("rewrite buffered record: %w", err)
		}
	}
	e.recordBufferStateLocked()

	return nil
}

func (e *parquetLogExtension) movePendingRecordsIntoWriterLocked() error {
	if len(e.pendingRecords) == 0 {
		return nil
	}
	if err := e.ensureWriterLocked(); err != nil {
		return err
	}

	pendingRecords := append([]any(nil), e.pendingRecords...)
	pendingOldestRecord := e.oldestPendingRecord
	e.clearPendingRecordsLocked()

	if len(e.writer.Objs) == 0 {
		e.oldestBufferedRecord = e.nowFn()
		if !pendingOldestRecord.IsZero() {
			e.oldestBufferedRecord = pendingOldestRecord
		}
	}

	for i, record := range pendingRecords {
		if e.oldestBufferedRecord.IsZero() {
			e.oldestBufferedRecord = e.nowFn()
		}
		if err := e.writeRecordLocked(record); err != nil {
			e.appendPendingRecordsLocked(
				pendingRecords[i:],
				tailOldestRecord(len(pendingRecords), pendingOldestRecord, i, e.nowFn()),
				e.estimateCompressedBytesForRecords(len(pendingRecords[i:])),
			)
			return fmt.Errorf("write pending record: %w", err)
		}
	}
	e.recordBufferStateLocked()
	return nil
}

func (e *parquetLogExtension) writeRecordLocked(record any) error {
	if e.writeRecordFn != nil {
		return e.writeRecordFn(record)
	}
	return e.Write(record)
}

func (e *parquetLogExtension) ensureWriterLocked() error {
	if e.writer != nil {
		return nil
	}
	if err := e.reinitializeWriterFn(); err != nil {
		return fmt.Errorf("reinitialize parquet writer: %w", err)
	}
	return nil
}

func (e *parquetLogExtension) Write(src any) error {
	pw := e.writer
	// Buffer records without parquet-go auto-flushing; flush lifecycle is owned here.
	ln := int64(len(pw.Objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
		src = val.Interface()
	}

	if pw.CheckSizeCritical <= ln {
		pw.ObjSize = (pw.ObjSize+common.SizeOf(val))/2 + 1
	}
	pw.ObjsSize += pw.ObjSize
	pw.Objs = append(pw.Objs, src)

	criSize := pw.NP * pw.PageSize * pw.SchemaHandler.GetColumnNum()
	dln := (criSize - pw.ObjsSize + pw.ObjSize - 1) / pw.ObjSize / 2
	pw.CheckSizeCritical = dln + ln

	return nil
}

func (e *parquetLogExtension) defaultWriteStop() error {
	return e.writer.WriteStop()
}

func (e *parquetLogExtension) defaultReadWriterBytes() ([]byte, error) {
	pqFile, err := e.memFile.Open(e.memFile.FilePath)
	if err != nil {
		return nil, fmt.Errorf("open memory file: %w", err)
	}
	pqBytes, err := io.ReadAll(pqFile)
	if err != nil {
		_ = pqFile.Close()
		return nil, fmt.Errorf("read memory file: %w", err)
	}
	if err := pqFile.Close(); err != nil {
		return nil, fmt.Errorf("close memory file: %w", err)
	}

	return pqBytes, nil
}

func (e *parquetLogExtension) getCurrentCompressedSize() int64 {
	if e.writer == nil {
		return 0
	}

	return int64(float64(e.writer.ObjsSize) * compressionRatioEstimate(e.config.CompressionCodec))
}

func (e *parquetLogExtension) estimateCompressedBytesForRecords(count int) int64 {
	if count <= 0 || e.writer == nil || e.writer.ObjSize <= 0 {
		return 0
	}

	return int64(float64(int64(count)*e.writer.ObjSize) * compressionRatioEstimate(e.config.CompressionCodec))
}

func (e *parquetLogExtension) clearPendingRecordsLocked() {
	e.pendingRecords = nil
	e.oldestPendingRecord = time.Time{}
	e.pendingEstimatedBytes = 0
}

func (e *parquetLogExtension) estimatePendingBytesFromBufferedState(
	bufferedRecordCount int,
	bufferedCompressedBytes int64,
	pendingRecordCount int,
) int64 {
	if pendingRecordCount <= 0 {
		return 0
	}
	if bufferedRecordCount > 0 && bufferedCompressedBytes > 0 {
		bytesPerRecord := bufferedCompressedBytes / int64(bufferedRecordCount)
		if bytesPerRecord == 0 {
			bytesPerRecord = 1
		}
		return bytesPerRecord * int64(pendingRecordCount)
	}
	return e.estimateCompressedBytesForRecords(pendingRecordCount)
}

func (e *parquetLogExtension) appendPendingRecordsLocked(records []any, oldest time.Time, estimatedBytes int64) {
	if len(records) == 0 {
		return
	}

	e.pendingRecords = append(e.pendingRecords, append([]any(nil), records...)...)
	if e.oldestPendingRecord.IsZero() || (!oldest.IsZero() && oldest.Before(e.oldestPendingRecord)) {
		e.oldestPendingRecord = oldest
	}
	if estimatedBytes > 0 {
		e.pendingEstimatedBytes += estimatedBytes
		return
	}
	e.pendingEstimatedBytes += e.estimateCompressedBytesForRecords(len(records))
}

func tailOldestRecord(pendingCount int, pendingOldest time.Time, tailStart int, fallback time.Time) time.Time {
	if pendingCount > tailStart && !pendingOldest.IsZero() {
		return pendingOldest
	}
	return fallback
}

func (e *parquetLogExtension) recordBufferStateLocked() {
	records := len(e.pendingRecords)
	estimatedBytes := e.pendingEstimatedBytes
	var oldestRecord time.Time
	if e.writer != nil {
		records += len(e.writer.Objs)
		estimatedBytes += e.getCurrentCompressedSize()
		if len(e.writer.Objs) > 0 && !e.oldestBufferedRecord.IsZero() {
			oldestRecord = e.oldestBufferedRecord
		}
	}
	if len(e.pendingRecords) > 0 && !e.oldestPendingRecord.IsZero() &&
		(oldestRecord.IsZero() || e.oldestPendingRecord.Before(oldestRecord)) {
		oldestRecord = e.oldestPendingRecord
	}

	oldestRecordAge := int64(0)
	if records > 0 && !oldestRecord.IsZero() {
		oldestRecordAge = int64(e.nowFn().Sub(oldestRecord).Seconds())
	}

	e.recordBufferStateFn(records, estimatedBytes, oldestRecordAge)
}

func parquetCompressionCodec(codec string) (parquet.CompressionCodec, error) {
	switch strings.ToLower(codec) {
	case "snappy":
		return parquet.CompressionCodec_SNAPPY, nil
	case "zstd":
		return parquet.CompressionCodec_ZSTD, nil
	case "gzip":
		return parquet.CompressionCodec_GZIP, nil
	case "uncompressed":
		return parquet.CompressionCodec_UNCOMPRESSED, nil
	default:
		return parquet.CompressionCodec_UNCOMPRESSED, fmt.Errorf(
			"unsupported parquet compression codec: %s",
			codec,
		)
	}
}

func compressionRatioEstimate(codec string) float64 {
	switch strings.ToLower(codec) {
	case "zstd":
		return 0.085
	case "gzip":
		return 0.15
	case "snappy":
		return 0.25
	case "uncompressed":
		return 1.0
	default:
		return 0.25
	}
}
