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
	rejectionFieldTimestamp:   {"timestamp", "timestamp"},
	rejectionFieldAtTimestamp: {"@timestamp", "at_timestamp"},
	rejectionFieldDuration:    {"duration", "duration"},
	rejectionFieldLevel:       {"level", "level"},
	rejectionFieldError:       {"error", "error"},
	rejectionFieldMessage:     {"message", "message"},
	rejectionFieldErrorsData:  {"errorsData", "errors_data"},
	rejectionFieldChange:      {"change", "change"},
}

type rejectionField uint8

const (
	rejectionFieldTimestamp rejectionField = iota
	rejectionFieldAtTimestamp
	rejectionFieldDuration
	rejectionFieldLevel
	rejectionFieldError
	rejectionFieldMessage
	rejectionFieldErrorsData
	rejectionFieldChange
	rejectionFieldUnknown
)

type rejectionKind uint8

const (
	rejectionKindUnknown rejectionKind = iota
	rejectionKindMap
	rejectionKindSlice
	rejectionKindString
	rejectionKindInt
	rejectionKindDouble
	rejectionKindBool
	rejectionKindBytes
	rejectionKindEmpty
	rejectionKindDerived
)

type rejectionSource uint8

const (
	rejectionSourceUnknown rejectionSource = iota
	rejectionSourceResource
	rejectionSourceScope
	rejectionSourceLog
	rejectionSourceBody
	rejectionSourceLogAlias
	rejectionSourceGenerated
	rejectionSourceMixed
)

type rejectionStatus uint8

const (
	rejectionStatusOther rejectionStatus = iota
	rejectionStatus400
	rejectionStatus409
	rejectionStatus429
	rejectionStatusOther4xx
	rejectionStatus5xx
)

type rejectionExpected uint8

const (
	rejectionExpectedUnknown rejectionExpected = iota
	rejectionExpectedText
	rejectionExpectedDate
	rejectionExpectedNumber
	rejectionExpectedObject
	rejectionExpectedBoolean
)

var rejectionTypes = [...]string{
	rejectionKindUnknown: "unknown", rejectionKindMap: "map", rejectionKindSlice: "slice",
	rejectionKindString: "string", rejectionKindInt: "int", rejectionKindDouble: "double",
	rejectionKindBool: "bool", rejectionKindBytes: "bytes", rejectionKindEmpty: "empty", rejectionKindDerived: "derived",
}

var rejectionSources = [...]string{
	rejectionSourceUnknown: "unknown", rejectionSourceResource: "resource", rejectionSourceScope: "scope",
	rejectionSourceLog: "log", rejectionSourceBody: "body", rejectionSourceLogAlias: "log_alias",
	rejectionSourceGenerated: "generated", rejectionSourceMixed: "mixed",
}

var rejectionStatuses = [...]string{
	rejectionStatusOther: "other", rejectionStatus400: "400", rejectionStatus409: "409",
	rejectionStatus429: "429", rejectionStatusOther4xx: "other_4xx", rejectionStatus5xx: "5xx",
}

var rejectionExpectedTypes = [...]string{
	rejectionExpectedUnknown: "unknown", rejectionExpectedText: "text", rejectionExpectedDate: "date",
	rejectionExpectedNumber: "number", rejectionExpectedObject: "object", rejectionExpectedBoolean: "boolean",
}

type logFieldShape uint64

func (s logFieldShape) with(field rejectionField, kind rejectionKind, source rejectionSource) logFieldShape {
	shift := uint(field * 8)
	if uint8(s>>shift) != 0 {
		kind, source = rejectionKindUnknown, rejectionSourceMixed
	}
	return s&^(logFieldShape(255)<<shift) | logFieldShape(uint8(source)<<4|uint8(kind))<<shift
}

func describeLogFields(ec encodingContext, record plog.LogRecord) logFieldShape {
	if ec.mappingMode != MappingECS {
		return 0
	}
	var shape logFieldShape
	for source, attrs := range []pcommon.Map{ec.resource.Attributes(), ec.scope.Attributes(), record.Attributes()} {
		if attrs.Len() > 128 {
			return 0 // Uninspected contributors may change any field's provenance.
		}
		if attrs.Len() == 0 {
			continue
		}
		for key, value := range attrs.All() {
			for i, field := range rejectionFields {
				if key == field.path {
					var kind rejectionKind
					switch value.Type() {
					case pcommon.ValueTypeMap:
						kind = rejectionKindMap
					case pcommon.ValueTypeSlice:
						kind = rejectionKindSlice
					case pcommon.ValueTypeStr:
						kind = rejectionKindString
					case pcommon.ValueTypeInt:
						kind = rejectionKindInt
					case pcommon.ValueTypeDouble:
						kind = rejectionKindDouble
					case pcommon.ValueTypeBool:
						kind = rejectionKindBool
					case pcommon.ValueTypeBytes:
						kind = rejectionKindBytes
					case pcommon.ValueTypeEmpty:
						kind = rejectionKindEmpty
					}
					shape = shape.with(rejectionField(i), kind, rejectionSource(source+1))
				} else if len(key) > len(field.path) && strings.HasPrefix(key, field.path) && key[len(field.path)] == '.' {
					shape = shape.with(rejectionField(i), rejectionKindDerived, rejectionSource(source+1))
				}
			}
		}
	}
	// ECS constructs an error object from these log-level aliases. Record only
	// their presence; the scalar contents never enter diagnostic state.
	for _, alias := range [...]string{"exception.message", "exception.type", "exception.stacktrace"} {
		if _, exists := record.Attributes().Get(alias); exists {
			shape = shape.with(rejectionFieldError, rejectionKindDerived, rejectionSourceLogAlias)
			break
		}
	}
	if record.Body().Type() == pcommon.ValueTypeMap {
		if record.Body().Map().Len() > 128 {
			return 0
		}
		// Preprocessing can copy a body alias into log attributes. Its original
		// location is no longer distinguishable at this seam.
		for _, alias := range [...]string{"exception.message", "exception.type", "exception.stacktrace"} {
			if _, exists := record.Body().Map().Get(alias); exists {
				shape = shape.with(rejectionFieldError, rejectionKindDerived, rejectionSourceBody)
				break
			}
		}
	}
	shape = shape.with(rejectionFieldAtTimestamp, rejectionKindDerived, rejectionSourceGenerated) // ECS generates @timestamp.
	if record.Body().Type() != pcommon.ValueTypeEmpty {
		kind := rejectionKindDerived
		if record.Body().Type() == pcommon.ValueTypeStr {
			kind = rejectionKindString
		}
		shape = shape.with(rejectionFieldMessage, kind, rejectionSourceBody)
	}
	return shape
}

type rejectionClass struct {
	status   rejectionStatus
	field    rejectionField
	expected rejectionExpected
	kind     rejectionKind
	source   rejectionSource
}

// The registry retains five enum nibbles, never strings from a bulk response.
func (c rejectionClass) key() uint32 {
	return uint32(c.status)<<16 | uint32(c.field)<<12 | uint32(c.expected)<<8 | uint32(c.kind)<<4 | uint32(c.source)
}

type rejectionDiagnostics struct {
	telemetry *metadata.TelemetryBuilder
	exporter  string
	buffers   chan *[maxRejectionShapes]logFieldShape
	mu        sync.Mutex
	series    map[uint32]struct{}
}

func newRejectionDiagnostics(tb *metadata.TelemetryBuilder, exporter string, concurrency int) *rejectionDiagnostics {
	concurrency = max(1, min(concurrency, 16))
	d := &rejectionDiagnostics{
		telemetry: tb, exporter: exporter,
		buffers: make(chan *[maxRejectionShapes]logFieldShape, concurrency),
		series:  make(map[uint32]struct{}, maxRejectionSeries),
	}
	for range concurrency {
		d.buffers <- nil // Reserve capacity without allocating for unused signals.
	}
	return d
}

type rejectionBatch struct {
	owner    *rejectionDiagnostics
	shapes   *[maxRejectionShapes]logFieldShape
	tracked  int
	reserved bool
}

func (d *rejectionDiagnostics) begin() rejectionBatch {
	b := rejectionBatch{owner: d}
	if d != nil {
		select {
		case b.shapes = <-d.buffers:
			b.reserved = true
		default:
		}
	}
	return b
}

func (b *rejectionBatch) record(position int, shape logFieldShape) {
	if b.reserved && b.shapes == nil && shape != 0 && position >= 0 && position < maxRejectionShapes {
		b.shapes = new([maxRejectionShapes]logFieldShape)
	}
	if b.shapes != nil && position >= 0 && position < len(b.shapes) {
		b.shapes[position] = shape
		b.tracked = position + 1
	}
}

func (b *rejectionBatch) end() {
	if b.reserved {
		b.owner.buffers <- b.shapes
		b.shapes = nil
		b.reserved = false
	}
	b.tracked = 0
}

func (b *rejectionBatch) observe(stat docappender.BulkIndexerResponseStat, count int, flushErr error) {
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
		b.owner.add(classifyRejectedItem(item, shape))
	}
}

func classifyRejectedItem(item docappender.BulkIndexerResponseItem, shape logFieldShape) rejectionClass {
	c := rejectionClass{status: rejectionStatusOther, field: rejectionFieldUnknown}
	switch {
	case item.Status == 400:
		c.status = rejectionStatus400
	case item.Status == 409:
		c.status = rejectionStatus409
	case item.Status == 429:
		c.status = rejectionStatus429
	case item.Status >= 400 && item.Status < 500:
		c.status = rejectionStatusOther4xx
	case item.Status >= 500 && item.Status < 600:
		c.status = rejectionStatus5xx
	}
	if item.Status != 400 || len(item.Error.Reason) > 2048 ||
		(item.Error.Type != "mapper_parsing_exception" && item.Error.Type != "document_parsing_exception") {
		return c
	}
	reason := rejectionReasonHeader(item.Error.Reason)
	for field, name := range rejectionFields {
		if reason == "object mapping for ["+name.path+"] tried to parse field ["+name.path+"] as object, but found a concrete value" {
			c.field, c.expected = rejectionField(field), rejectionExpectedObject
			tag := uint8(shape >> uint(field*8))
			c.kind, c.source = rejectionKind(tag&15), rejectionSource(tag>>4)
			return c
		}
		for _, expected := range []struct {
			literal string
			class   rejectionExpected
		}{
			{"text", rejectionExpectedText},
			{"keyword", rejectionExpectedText},
			{"date", rejectionExpectedDate},
			{"date_nanos", rejectionExpectedDate},
			{"float", rejectionExpectedNumber},
			{"double", rejectionExpectedNumber},
			{"long", rejectionExpectedNumber},
			{"integer", rejectionExpectedNumber},
			{"object", rejectionExpectedObject},
			{"nested", rejectionExpectedObject},
			{"boolean", rejectionExpectedBoolean},
		} {
			prefix := "failed to parse field [" + name.path + "] of type [" + expected.literal + "]"
			if !strings.HasPrefix(reason, prefix) {
				continue
			}
			suffix := reason[len(prefix):]
			if suffix != "" && !strings.HasPrefix(suffix, " in document with id '") && !strings.HasPrefix(suffix, ". Preview of field's value: '") {
				return c
			}
			c.field, c.expected = rejectionField(field), expected.class
			tag := uint8(shape >> uint(field*8))
			c.kind, c.source = rejectionKind(tag&15), rejectionSource(tag>>4)
			return c
		}
	}
	return c
}

// Elasticsearch may prefix the fixed error header with an XContent location.
// Consume only one exact [digits:digits] prefix, never search inside a suffix.
func rejectionReasonHeader(reason string) string {
	if len(reason) == 0 || reason[0] != '[' {
		return reason
	}
	i := 1
	for part := range 2 {
		start := i
		for i < len(reason) && reason[i] >= '0' && reason[i] <= '9' {
			i++
		}
		if i == start {
			return reason
		}
		if part == 0 {
			if i >= len(reason) || reason[i] != ':' {
				return reason
			}
			i++
		}
	}
	if !strings.HasPrefix(reason[i:], "] ") {
		return reason
	}
	return reason[i+2:]
}

func (d *rejectionDiagnostics) add(c rejectionClass) {
	d.mu.Lock()
	if _, exists := d.series[c.key()]; !exists {
		if len(d.series) < maxRejectionSeries {
			d.series[c.key()] = struct{}{}
		} else {
			c.field, c.expected, c.kind, c.source = rejectionFieldUnknown, rejectionExpectedUnknown, rejectionKindUnknown, rejectionSourceUnknown
		}
	}
	d.mu.Unlock()
	fieldName := "unknown"
	if c.field < rejectionFieldUnknown {
		fieldName = rejectionFields[c.field].class
	}
	// This diagnostic stream must not carry request trace/span IDs via exemplars.
	d.telemetry.ElasticsearchDocsFailedByField.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("exporter", d.exporter), attribute.String("item_status_class", rejectionStatuses[c.status]),
		attribute.String("field_class", fieldName), attribute.String("expected_type", rejectionExpectedTypes[c.expected]),
		attribute.String("value_type", rejectionTypes[c.kind]), attribute.String("source", rejectionSources[c.source]),
	)))
}
