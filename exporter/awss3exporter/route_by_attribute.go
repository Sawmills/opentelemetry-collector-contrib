// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter"

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awss3exporter/internal/upload"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/awss3marshaler"
)

// defaultRouteFolder names the folder for records without a usable value.
const defaultRouteFolder = "__default__"

// routedLogs is the part of a batch that belongs to one folder value. An
// empty folder means the default folder.
type routedLogs struct {
	folder string
	logs   plog.Logs
}

// routeFolder turns an attribute value into a single path segment. Values are
// kept as-is except for '/', which would create nested folders. Empty, "." and
// ".." values cannot name a folder and fall back to the default folder.
func routeFolder(value string) (string, bool) {
	if value == "" || value == "." || value == ".." {
		return "", false
	}
	return strings.ReplaceAll(value, "/", "_"), true
}

func routePrefix(prefix, folder string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return folder
	}
	return prefix + "/" + folder
}

// lookupAttribute returns the value of key, either as a flat attribute or as a
// dotted path into map values (sawmills.service -> sawmills: {service: ...}).
func lookupAttribute(attrs pcommon.Map, key string) (string, bool) {
	if v, ok := attrs.Get(key); ok {
		return v.AsString(), true
	}
	for i := strings.IndexByte(key, '.'); i >= 0; {
		if v, ok := attrs.Get(key[:i]); ok && v.Type() == pcommon.ValueTypeMap {
			if value, found := lookupAttribute(v.Map(), key[i+1:]); found {
				return value, true
			}
		}
		next := strings.IndexByte(key[i+1:], '.')
		if next < 0 {
			break
		}
		i += next + 1
	}
	return "", false
}

// splitLogsByRoute groups records by the attribute's value, looking at the
// record first and its resource second, keeping resource and scope data.
func splitLogsByRoute(ld plog.Logs, attribute string) []routedLogs {
	var groups []routedLogs
	index := map[string]int{}
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceValue, _ := lookupAttribute(rl.Resource().Attributes(), attribute)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			// One destination ScopeLogs per folder for this source ScopeLogs.
			scopes := map[string]plog.ScopeLogs{}
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				value, ok := lookupAttribute(lr.Attributes(), attribute)
				if !ok {
					value = resourceValue
				}
				folder, _ := routeFolder(value)

				dst, ok := scopes[folder]
				if !ok {
					gi, exists := index[folder]
					if !exists {
						gi = len(groups)
						index[folder] = gi
						groups = append(groups, routedLogs{folder: folder, logs: plog.NewLogs()})
					}
					drl := groups[gi].logs.ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(drl.Resource())
					drl.SetSchemaUrl(rl.SchemaUrl())
					dst = drl.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(dst.Scope())
					dst.SetSchemaUrl(sl.SchemaUrl())
					scopes[folder] = dst
				}
				lr.CopyTo(dst.LogRecords().AppendEmpty())
			}
		}
	}
	return groups
}

// logEncoderFactory is implemented by encoding extensions that can create
// independent encoders, so each folder keeps its own buffer.
type logEncoderFactory interface {
	NewLogEncoder() (plog.Marshaler, error)
}

// logRoutes holds one marshaler per folder. Buffering marshalers keep
// buffering per folder, so a folder is uploaded on its own size limit and on
// timer/shutdown flushes rather than on every batch.
type logRoutes struct {
	attribute  string
	maxFolders int
	newSink    func() (marshaler, error)

	mu        sync.Mutex
	sinks     map[string]marshaler // keyed by folder value; "" is the default folder
	capLogged bool
}

type routeSink struct {
	value     string
	marshaler marshaler
	// overflow is set when the value was sent to the default folder because
	// the folder cap was reached; firstOverflow only for the first such value.
	overflow      bool
	firstOverflow bool
	folders       int
}

func newLogRoutes(cfg RouteByAttribute, newSink func() (marshaler, error)) *logRoutes {
	maxFolders := cfg.MaxFolders
	if maxFolders == 0 {
		maxFolders = defaultRouteMaxFolders
	}
	return &logRoutes{
		attribute:  cfg.Attribute,
		maxFolders: maxFolders,
		newSink:    newSink,
		sinks:      map[string]marshaler{},
	}
}

func (r *logRoutes) sink(value string) (routeSink, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := routeSink{value: value}
	if _, ok := r.sinks[value]; !ok && value != "" && r.valueFolders() >= r.maxFolders {
		out.value, out.overflow = "", true
		out.firstOverflow = !r.capLogged
		r.capLogged = true
	}
	m, ok := r.sinks[out.value]
	if !ok {
		var err error
		if m, err = r.newSink(); err != nil {
			return routeSink{}, err
		}
		r.sinks[out.value] = m
	}
	out.marshaler = m
	out.folders = r.valueFolders()
	return out, nil
}

// valueFolders counts folders named after a value, excluding the default one.
func (r *logRoutes) valueFolders() int {
	if _, ok := r.sinks[""]; ok {
		return len(r.sinks) - 1
	}
	return len(r.sinks)
}

func (r *logRoutes) snapshot() map[string]marshaler {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.sinks)
}

var hiveKeyReplacer = strings.NewReplacer(".", "_", "/", "_")

// hiveFolder names a folder as <attribute>=<value>, with the attribute made
// usable as a partition column name (Athena rejects dots).
func hiveFolder(attribute, value string) string {
	if value == "" {
		value = defaultRouteFolder
	}
	return hiveKeyReplacer.Replace(attribute) + "=" + value
}

func (e *s3Exporter) routeUploadOpts(value string) *upload.UploadOptions {
	folder := hiveFolder(e.routes.attribute, value)
	return &upload.UploadOptions{OverridePrefix: routePrefix(e.config.S3Uploader.S3Prefix, folder)}
}

// newRouteSinkFactory builds per-folder marshalers matching the configured
// marshaler.
func (e *s3Exporter) newRouteSinkFactory(host component.Host) (func() (marshaler, error), error) {
	switch {
	case e.config.Encoding != nil:
		factory, ok := host.GetExtensions()[*e.config.Encoding].(logEncoderFactory)
		if !ok {
			return nil, fmt.Errorf("route_by_attribute: encoding %q cannot create per-folder encoders", e.config.Encoding)
		}
		return func() (marshaler, error) {
			enc, err := factory.NewLogEncoder()
			if err != nil {
				return nil, err
			}
			return newEncodingMarshaler(enc, e.config.EncodingFileExtension), nil
		}, nil
	case e.config.MaxFileSizeBytes > 0:
		return func() (marshaler, error) {
			return awss3marshaler.NewMarshalerWithConfig(e.config.MarshalerName, e.config.MaxFileSizeBytes, e.logger)
		}, nil
	default:
		return func() (marshaler, error) { return awss3marshaler.NewMarshaler(e.config.MarshalerName) }, nil
	}
}

func (e *s3Exporter) consumeRoutedLogs(ctx context.Context, logs plog.Logs) error {
	for _, group := range splitLogsByRoute(logs, e.routes.attribute) {
		sink, err := e.routes.sink(group.folder)
		if err != nil {
			return err
		}
		e.telemetry.recordRouteFolders(ctx, e.signalType, int64(sink.folders))
		if sink.overflow {
			e.telemetry.recordRouteOverflow(ctx, e.signalType, int64(group.logs.LogRecordCount()))
			if sink.firstOverflow {
				e.logger.Warn("route_by_attribute folder cap reached; new values go to the default folder",
					zap.String("attribute", e.routes.attribute),
					zap.Int("max_folders", e.routes.maxFolders),
					zap.String("value", group.folder))
			}
		}

		flushStartedAt := time.Now()
		e.telemetry.recordFlushStart(ctx, e.signalType)
		buf, flushMeta, err := marshalLogs(sink.marshaler, group.logs)
		e.telemetry.recordFlushComplete(ctx, e.signalType, flushMeta.reason, time.Since(flushStartedAt), err)
		if err != nil {
			return err
		}
		if err := e.uploadBuffer(ctx, buf, e.routeUploadOpts(sink.value), flushMeta); err != nil {
			return err
		}
	}
	return nil
}

// flushRoutes uploads every folder's buffer to its own prefix.
func (e *s3Exporter) flushRoutes(ctx context.Context, reason string) error {
	var errs error
	for value, m := range e.routes.snapshot() {
		buf, err := flushLogs(m, reason)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		errs = errors.Join(errs, e.uploadBuffer(ctx, buf, e.routeUploadOpts(value), flushMetadata{reason: reason}))
	}
	return errs
}

func marshalLogs(m marshaler, ld plog.Logs) ([]byte, flushMetadata, error) {
	if metadataMarshaler, ok := m.(marshalerWithFlushMetadata); ok {
		return metadataMarshaler.MarshalLogsWithFlushMetadata(ld)
	}
	buf, err := m.MarshalLogs(ld)
	return buf, flushMetadata{}, err
}

func flushLogs(m marshaler, reason string) ([]byte, error) {
	switch flusher := m.(type) {
	case logFlusherWithReason:
		return flusher.FlushLogsWithReason(reason)
	case logFlusher:
		return flusher.FlushLogs()
	}
	return nil, nil
}
