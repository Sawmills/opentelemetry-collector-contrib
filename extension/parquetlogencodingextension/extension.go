package parquetlogencodingextension

import (
	"context"
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
)

const (
	flushReasonManual = "manual"
	flushReasonSize   = "size"
)

type parquetLogExtension struct {
	memFile              *MemFile
	logger               *zap.Logger
	ctx                  context.Context
	config               *Config
	mutex                sync.Mutex
	writer               *writer.ParquetWriter
	adapter              adapters.ParquetAdapter
	telemetry            *parquetTelemetry
	maxFileSizeBytes     int64
	fileName             string
	oldestBufferedRecord time.Time
	writeStopFn          func() error
	readWriterBytes      func() ([]byte, error)
	reinitializeWriterFn func() error
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
	cfg := baseCfg.(*Config)

	adapter, err := datadog.NewDatadogParquetAdapter(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet adapter: %w", err)
	}

	telemetry, err := newParquetTelemetry(params)
	if err != nil {
		return nil, err
	}

	ext := &parquetLogExtension{
		logger:           params.Logger,
		ctx:              ctx,
		config:           cfg,
		adapter:          adapter,
		telemetry:        telemetry,
		maxFileSizeBytes: cfg.MaxFileSizeBytes,
		fileName:         fmt.Sprintf("parquet-log-%d.parquet", time.Now().UnixNano()),
	}
	ext.writeStopFn = ext.defaultWriteStop
	ext.readWriterBytes = ext.defaultReadWriterBytes
	ext.reinitializeWriterFn = ext.initializeWriter

	if err := ext.initializeWriter(); err != nil {
		return nil, err
	}
	if err := telemetry.registerOldestRecordAgeCallback(ext.bufferOldestRecordAgeSeconds); err != nil {
		return nil, err
	}
	ext.recordBufferStateLocked()

	return ext, nil
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
	return nil
}

func (e *parquetLogExtension) Shutdown(context.Context) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.writer != nil && len(e.writer.Objs) > 0 {
		e.telemetry.shutdown()
		return fmt.Errorf("buffered parquet records remain at shutdown; flush before shutdown")
	}

	e.telemetry.shutdown()
	return nil
}

func (e *parquetLogExtension) FlushLogs() ([]byte, error) {
	return e.FlushLogsWithReason(flushReasonManual)
}

func (e *parquetLogExtension) FlushLogsWithReason(reason string) ([]byte, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	buf, _, _, err := e.checkAndFlushWithMetadata(true, reason)
	return buf, err
}

func (e *parquetLogExtension) FlushLogsWithReasonAndMetadata(
	reason string,
) ([]byte, string, time.Time, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

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

	return e.addLogRecordWithFlushMetadata(toParquet)
}

func (e *parquetLogExtension) addLogRecordWithFlushMetadata(
	records []any,
) ([]byte, string, time.Time, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if len(records) > 0 && len(e.writer.Objs) == 0 {
		e.oldestBufferedRecord = time.Now()
	}
	for _, record := range records {
		if err := e.Write(record); err != nil {
			return nil, "", time.Time{}, fmt.Errorf("failed to write record: %w", err)
		}
	}
	e.recordBufferStateLocked()

	return e.checkAndFlushWithMetadata(false, flushReasonSize)
}

func (e *parquetLogExtension) checkAndFlushWithMetadata(
	forceFlush bool,
	reason string,
) ([]byte, string, time.Time, error) {
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

	flushStartedAt := e.oldestBufferedRecord
	if flushStartedAt.IsZero() {
		flushStartedAt = time.Now()
	}
	snapshot := e.snapshotBufferedStateLocked()

	if err := e.writeStopFn(); err != nil {
		e.telemetry.recordFailedFlush(reason)
		e.logger.Error("failed to stop parquet writer", zap.Error(err))
		if restoreErr := e.restoreBufferedStateLocked(snapshot); restoreErr != nil {
			e.logger.Error("failed to restore buffered parquet state", zap.Error(restoreErr))
			return nil, "", time.Time{}, fmt.Errorf(
				"write stop failed: %w; restore failed: %v",
				err,
				restoreErr,
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
			return nil, "", time.Time{}, fmt.Errorf(
				"read parquet bytes failed: %w; restore failed: %v",
				err,
				restoreErr,
			)
		}
		return nil, "", time.Time{}, err
	}

	if err := e.reinitializeWriterFn(); err != nil {
		e.telemetry.recordFailedFlush(reason)
		e.logger.Error("failed to reinitialize parquet writer", zap.Error(err))
		if restoreErr := e.restoreBufferedStateLocked(snapshot); restoreErr != nil {
			e.logger.Error("failed to restore buffered parquet state", zap.Error(restoreErr))
			return nil, "", time.Time{}, fmt.Errorf(
				"reinitialize parquet writer failed: %w; restore failed: %v",
				err,
				restoreErr,
			)
		}
		return nil, "", time.Time{}, err
	}

	e.telemetry.recordFlush(
		reason,
		int64(bufferedRecords),
		int64(len(pqBytes)),
		time.Since(flushStartedAt),
	)
	completedAt := time.Now()
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

func (e *parquetLogExtension) bufferOldestRecordAgeSeconds() int64 {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.writer == nil || len(e.writer.Objs) == 0 || e.oldestBufferedRecord.IsZero() {
		return 0
	}

	return int64(time.Since(e.oldestBufferedRecord).Seconds())
}

func (e *parquetLogExtension) recordBufferStateLocked() {
	records := 0
	estimatedBytes := int64(0)
	if e.writer != nil {
		records = len(e.writer.Objs)
		estimatedBytes = e.getCurrentCompressedSize()
	}

	e.telemetry.recordBufferState(records, estimatedBytes)
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
