// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"context"
	"strings"
	"sync"

	"github.com/elastic/go-docappender/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/metadata"
)

const (
	maxRejectionShapes = 8192 // Eight packed field tags per document, at most 64 KiB per session.
	maxRejectionSeries = 64
)

var rejectionFields = [...]struct{ path, class string }{
	{"timestamp", "timestamp"},
	{"@timestamp", "at_timestamp"},
	{"duration", "duration"},
	{"level", "level"},
	{"error", "error"},
	{"message", "message"},
	{"errorsData", "errors_data"},
	{"change", "change"},
}

var (
	rejectionTypes   = [...]string{"unknown", "map", "slice", "string", "int", "double", "bool", "bytes", "empty", "derived"}
	rejectionSources = [...]string{"unknown", "resource", "scope", "log", "body", "log_alias", "generated", "mixed"}
)

type logFieldShape uint64

func (s logFieldShape) with(field int, kind, source uint8) logFieldShape {
	shift := uint(field * 8)
	if uint8(s>>shift) != 0 {
		kind, source = 0, 7
	}
	return s&^(logFieldShape(255)<<shift) | logFieldShape(source<<4|kind)<<shift
}

func describeLogFields(ec encodingContext, record plog.LogRecord) logFieldShape {
	if ec.mappingMode != MappingECS {
		return 0
	}
	var shape logFieldShape
	for source, attrs := range []pcommon.Map{ec.resource.Attributes(), ec.scope.Attributes(), record.Attributes()} {
		if attrs.Len() == 0 {
			continue
		}
		for i, field := range rejectionFields {
			if value, exists := attrs.Get(field.path); exists {
				var kind uint8
				switch value.Type() {
				case pcommon.ValueTypeMap:
					kind = 1
				case pcommon.ValueTypeSlice:
					kind = 2
				case pcommon.ValueTypeStr:
					kind = 3
				case pcommon.ValueTypeInt:
					kind = 4
				case pcommon.ValueTypeDouble:
					kind = 5
				case pcommon.ValueTypeBool:
					kind = 6
				case pcommon.ValueTypeBytes:
					kind = 7
				case pcommon.ValueTypeEmpty:
					kind = 8
				}
				shape = shape.with(i, kind, uint8(source+1))
			}
		}
	}
	// ECS constructs an error object from these log-level aliases. Record only
	// their presence; the scalar contents never enter diagnostic state.
	for _, alias := range [...]string{"exception.message", "exception.type", "exception.stacktrace"} {
		if _, exists := record.Attributes().Get(alias); exists {
			shape = shape.with(4, 9, 5)
			break
		}
	}
	shape = shape.with(1, 9, 6) // ECS generates @timestamp.
	if record.Body().Type() != pcommon.ValueTypeEmpty {
		kind := uint8(9)
		if record.Body().Type() == pcommon.ValueTypeStr {
			kind = 3
		}
		shape = shape.with(5, kind, 4)
	}
	return shape
}

type rejectionClass struct {
	status, field, expected, kind, source string
}

type rejectionDiagnostics struct {
	telemetry *metadata.TelemetryBuilder
	exporter  string
	buffers   chan *[maxRejectionShapes]logFieldShape
	mu        sync.Mutex
	series    map[rejectionClass]struct{}
}

func newRejectionDiagnostics(tb *metadata.TelemetryBuilder, exporter string, concurrency int) *rejectionDiagnostics {
	concurrency = max(1, min(concurrency, 16))
	d := &rejectionDiagnostics{
		telemetry: tb, exporter: exporter,
		buffers: make(chan *[maxRejectionShapes]logFieldShape, concurrency),
		series:  make(map[rejectionClass]struct{}, maxRejectionSeries),
	}
	for range concurrency {
		d.buffers <- new([maxRejectionShapes]logFieldShape)
	}
	return d
}

type rejectionBatch struct {
	owner   *rejectionDiagnostics
	shapes  *[maxRejectionShapes]logFieldShape
	tracked int
}

func (d *rejectionDiagnostics) begin() rejectionBatch {
	b := rejectionBatch{owner: d}
	if d != nil {
		select {
		case b.shapes = <-d.buffers:
		default:
		}
	}
	return b
}

func (b *rejectionBatch) record(position int, shape logFieldShape) {
	if b.shapes != nil && position >= 0 && position < len(b.shapes) {
		b.shapes[position] = shape
		b.tracked = position + 1
	}
}

func (b *rejectionBatch) end() {
	if b.shapes != nil {
		b.owner.buffers <- b.shapes
		b.shapes = nil
	}
	b.tracked = 0
}

func (b *rejectionBatch) observe(ctx context.Context, stat docappender.BulkIndexerResponseStat, count int, flushErr error) {
	defer func() { b.tracked = 0 }() // Retained retry positions are not exposed by docappender.
	if b.owner == nil || flushErr != nil || stat.Indexed+stat.RetriedDocs+int64(len(stat.FailedDocs)) != int64(count) {
		return
	}
	previous := -1
	for _, item := range stat.FailedDocs {
		if item.Position <= previous || item.Position >= count {
			return
		}
		previous = item.Position
	}
	for _, item := range stat.FailedDocs {
		var shape logFieldShape
		if b.shapes != nil && item.Position < b.tracked {
			shape = b.shapes[item.Position]
		}
		b.owner.add(ctx, classifyRejectedItem(item, shape))
	}
}

func classifyRejectedItem(item docappender.BulkIndexerResponseItem, shape logFieldShape) rejectionClass {
	c := rejectionClass{status: "other", field: "unknown", expected: "unknown", kind: "unknown", source: "unknown"}
	switch {
	case item.Status == 400:
		c.status = "400"
	case item.Status == 409:
		c.status = "409"
	case item.Status == 429:
		c.status = "429"
	case item.Status >= 400 && item.Status < 500:
		c.status = "other_4xx"
	case item.Status >= 500 && item.Status < 600:
		c.status = "5xx"
	}
	if item.Status != 400 || len(item.Error.Reason) > 2048 ||
		(item.Error.Type != "mapper_parsing_exception" && item.Error.Type != "document_parsing_exception") {
		return c
	}
	for field, name := range rejectionFields {
		for _, expected := range []struct{ literal, class string }{
			{"text", "text"},
			{"keyword", "text"},
			{"date", "date"},
			{"date_nanos", "date"},
			{"float", "number"},
			{"double", "number"},
			{"long", "number"},
			{"integer", "number"},
			{"object", "object"},
			{"nested", "object"},
			{"boolean", "boolean"},
		} {
			prefix := "failed to parse field [" + name.path + "] of type [" + expected.literal + "]"
			if !strings.HasPrefix(item.Error.Reason, prefix) {
				continue
			}
			suffix := item.Error.Reason[len(prefix):]
			if suffix != "" && !strings.HasPrefix(suffix, " in document") && !strings.HasPrefix(suffix, ". ") {
				return c
			}
			c.field, c.expected = name.class, expected.class
			tag := uint8(shape >> uint(field*8))
			c.kind, c.source = rejectionTypes[tag&15], rejectionSources[tag>>4]
			return c
		}
	}
	return c
}

func (d *rejectionDiagnostics) add(ctx context.Context, c rejectionClass) {
	d.mu.Lock()
	if _, exists := d.series[c]; !exists {
		if len(d.series) < maxRejectionSeries {
			d.series[c] = struct{}{}
		} else {
			c.field, c.expected, c.kind, c.source = "unknown", "unknown", "unknown", "unknown"
		}
	}
	d.mu.Unlock()
	d.telemetry.ElasticsearchDocsFailedByField.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("exporter", d.exporter), attribute.String("item_status_class", c.status),
		attribute.String("field_class", c.field), attribute.String("expected_type", c.expected),
		attribute.String("value_type", c.kind), attribute.String("source", c.source),
	)))
}
