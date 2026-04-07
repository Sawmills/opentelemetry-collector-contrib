// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflake

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes/source"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters"
)

const (
	schemaVersion              = "snowflake_v1"
	serviceNameKey             = "service.name"
	maxArchivedColdAttributes  = 128
	maxArchivedStringValueSize = 4096
)

var (
	signalTypeSet             = attribute.NewSet(attribute.String("signal", "logs"))
	excludedColdAttributeKeys = map[string]struct{}{
		"k8s.node.uid": {},
		"k8s.pod.ip":   {},
		"k8s.pod.uid":  {},
	}
	protectedColdAttributeKeys = map[string]struct{}{
		"customer.id":    {},
		"transaction.id": {},
	}
)

func DefaultAttributesHotKeys() []string {
	return []string{}
}

func DefaultTagsHotKeys() []string {
	return []string{"env", "version"}
}

type Config struct {
	AttributesHotKeys []string
	TagsHotKeys       []string
	RecordDropFn      func(reason string, count int64)
}

type ParquetLog struct {
	TS                 int64   `parquet:"name=ts, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Service            string  `parquet:"name=service, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Status             string  `parquet:"name=status, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	MessageText        string  `parquet:"name=message_text, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	Host               string  `parquet:"name=host, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Source             string  `parquet:"name=source, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	TraceID            string  `parquet:"name=trace_id, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	SpanID             string  `parquet:"name=span_id, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	SchemaVersion      string  `parquet:"name=schema_version, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	BodyJSONText       *string `parquet:"name=body_json_text, type=BYTE_ARRAY, logicaltype=STRING, repetitiontype=OPTIONAL, encoding=DELTA_BYTE_ARRAY"`
	AttributesHotText  string  `parquet:"name=attributes_hot_text, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	AttributesColdText string  `parquet:"name=attributes_cold_text, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	TagsHotText        string  `parquet:"name=tags_hot_text, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	TagsColdText       string  `parquet:"name=tags_cold_text, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
}

type snowflakeParquetAdapter struct {
	logger               *zap.Logger
	attributesTranslator *attributes.Translator
	attributesHotKeys    []string
	tagsHotKeys          []string
	recordDropFn         func(reason string, count int64)
}

type snowflakeArchiveItem struct {
	TS                 int64
	Service            string
	Status             string
	MessageText        string
	Host               string
	Source             string
	TraceID            string
	SpanID             string
	BodyJSONText       *string
	AttributesHotText  string
	AttributesColdText string
	TagsHotText        string
	TagsColdText       string
}

func NewSnowflakeParquetAdapter(
	params extension.Settings,
	cfg Config,
) (adapters.ParquetAdapter, error) {
	attributesTranslator, err := attributes.NewTranslator(params.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create attributes translator: %w", err)
	}

	attributesHotKeys := cfg.AttributesHotKeys
	if attributesHotKeys == nil {
		attributesHotKeys = DefaultAttributesHotKeys()
	}
	tagsHotKeys := cfg.TagsHotKeys
	if tagsHotKeys == nil {
		tagsHotKeys = DefaultTagsHotKeys()
	}

	return &snowflakeParquetAdapter{
		logger:               params.Logger,
		attributesTranslator: attributesTranslator,
		attributesHotKeys:    slices.Clone(attributesHotKeys),
		tagsHotKeys:          slices.Clone(tagsHotKeys),
		recordDropFn:         cfg.RecordDropFn,
	}, nil
}

func (a *snowflakeParquetAdapter) ConvertToParquet(ctx context.Context, ld plog.Logs) ([]any, error) {
	resourceLogs := ld.ResourceLogs()
	records := make([]any, 0)
	droppedZeroTimestamp := 0

	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		res := rl.Resource()
		resourceHost, resourceService := a.hostNameAndServiceNameFromResource(ctx, res)
		scopeLogs := rl.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			logRecords := scopeLogs.At(j).LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				record := logRecords.At(k)
				if record.Timestamp() == 0 {
					droppedZeroTimestamp++
					if a.recordDropFn != nil {
						a.recordDropFn("row_drop_invalid_ts", 1)
					}
					continue
				}

				host := resourceHost
				service := resourceService
				if host == "" {
					host = a.hostFromAttributes(ctx, record.Attributes())
				}
				if service == "" {
					if value, ok := record.Attributes().Get(serviceNameKey); ok {
						service = value.AsString()
					}
				}

				item, err := a.transform(record, host, service, res)
				if err != nil {
					return nil, err
				}
				records = append(records, ParquetLog{
					TS:                 item.TS,
					Service:            item.Service,
					Status:             item.Status,
					MessageText:        item.MessageText,
					Host:               item.Host,
					Source:             item.Source,
					TraceID:            item.TraceID,
					SpanID:             item.SpanID,
					SchemaVersion:      schemaVersion,
					BodyJSONText:       item.BodyJSONText,
					AttributesHotText:  item.AttributesHotText,
					AttributesColdText: item.AttributesColdText,
					TagsHotText:        item.TagsHotText,
					TagsColdText:       item.TagsColdText,
				})
			}
		}
	}

	if droppedZeroTimestamp > 0 {
		a.logger.Warn("dropped log records with zero timestamp", zap.Int("count", droppedZeroTimestamp))
	}

	return records, nil
}

func (*snowflakeParquetAdapter) Schema() any {
	return new(ParquetLog)
}

func (a *snowflakeParquetAdapter) transform(
	record plog.LogRecord,
	host, service string,
	resource pcommon.Resource,
) (*snowflakeArchiveItem, error) {
	item := &snowflakeArchiveItem{
		TS:      record.Timestamp().AsTime().UnixMilli(),
		Host:    host,
		Service: service,
	}

	attrs := make(map[string]any)
	status := ""

	record.Attributes().Range(func(key string, value pcommon.Value) bool {
		switch strings.ToLower(key) {
		case "ddsource", "datadog.log.source":
			item.Source = value.AsString()
		case "hostname":
			item.Host = value.AsString()
		case "service":
			item.Service = value.AsString()
		case "status", "severity", "level", "syslog.severity":
			status = value.AsString()
		default:
			attrs[key] = valueToAny(value)
		}
		return true
	})

	resource.Attributes().Range(func(key string, value pcommon.Value) bool {
		if key == "ddtags" || key == "hostname" || key == "service" {
			return true
		}
		for k, v := range flattenAttribute(key, value, 1) {
			if _, exists := attrs[k]; exists {
				continue
			}
			attrs[k] = v
		}
		return true
	})

	if status == "" && record.SeverityText() != "" {
		status = record.SeverityText()
	}
	if status == "" && record.SeverityNumber() != 0 {
		status = adapters.StatusFromSeverityNumber(record.SeverityNumber())
	}
	item.Status = strings.ToLower(status)

	item.TraceID = traceIDString(record, attrs)
	item.SpanID = spanIDString(record, attrs)
	bodyJSONText, messageText, err := deriveBodyJSONAndMessage(record.Body())
	if err != nil {
		return nil, err
	}
	item.BodyJSONText = bodyJSONText
	item.MessageText = messageText

	if item.Service != "" {
		attrs["service"] = item.Service
	}
	if item.Host != "" {
		attrs["hostname"] = item.Host
	}
	if item.Source != "" {
		attrs["source"] = item.Source
	}
	if item.Status != "" {
		attrs["status"] = item.Status
	}

	tags := extractTags(resource, record)
	attributesHot, attributesCold := splitMap(attrs, a.attributesHotKeys)
	attributesCold = sanitizeArchivedMap(
		attributesCold,
		maxArchivedColdAttributes,
		excludedColdAttributeKeys,
		protectedColdAttributeKeys,
	)
	tagsHot, tagsCold := splitMap(tags, a.tagsHotKeys)
	tagsCold = sanitizeArchivedMap(tagsCold, maxArchivedColdAttributes, excludedColdAttributeKeys, nil)

	item.AttributesHotText, err = canonicalJSONString(attributesHot)
	if err != nil {
		return nil, fmt.Errorf("marshal hot attributes: %w", err)
	}
	item.AttributesColdText, err = canonicalJSONString(attributesCold)
	if err != nil {
		return nil, fmt.Errorf("marshal cold attributes: %w", err)
	}
	item.TagsHotText, err = canonicalJSONString(tagsHot)
	if err != nil {
		return nil, fmt.Errorf("marshal hot tags: %w", err)
	}
	item.TagsColdText, err = canonicalJSONString(tagsCold)
	if err != nil {
		return nil, fmt.Errorf("marshal cold tags: %w", err)
	}

	return item, nil
}

func extractTags(resource pcommon.Resource, record plog.LogRecord) map[string]any {
	tagMap := make(map[string]any)
	resourceTags := attributes.TagsFromAttributes(resource.Attributes())
	maps.Copy(tagMap, tagsToMap(resourceTags))
	if ddtags, ok := resource.Attributes().Get("ddtags"); ok && ddtags.Type() == pcommon.ValueTypeMap {
		ddtags.Map().Range(func(key string, value pcommon.Value) bool {
			tagMap[key] = valueToAny(value)
			return true
		})
	}

	if value, ok := record.Attributes().Get("ddtags"); ok && value.Type() == pcommon.ValueTypeStr {
		maps.Copy(tagMap, tagsToMap(strings.Split(value.AsString(), ",")))
	}
	return tagMap
}

func splitMap(source map[string]any, hotKeys []string) (map[string]any, map[string]any) {
	hot := make(map[string]any)
	cold := make(map[string]any)
	for key, value := range source {
		if slices.Contains(hotKeys, key) {
			hot[key] = value
			continue
		}
		cold[key] = value
	}
	return hot, cold
}

func sanitizeArchivedMap(
	source map[string]any,
	maxKeys int,
	excludedKeys map[string]struct{},
	protectedKeys map[string]struct{},
) map[string]any {
	if len(source) == 0 {
		return source
	}

	keys := make([]string, 0, len(source))
	protected := make([]string, 0, len(source))
	for key := range source {
		if _, excluded := excludedKeys[key]; excluded {
			continue
		}
		if _, keep := protectedKeys[key]; keep {
			protected = append(protected, key)
			continue
		}
		keys = append(keys, key)
	}
	slices.Sort(protected)
	slices.Sort(keys)
	if maxKeys > 0 && len(keys) > maxKeys {
		keys = keys[:maxKeys]
	}

	sanitized := make(map[string]any, len(protected)+len(keys))
	for _, key := range protected {
		sanitized[key] = sanitizeArchivedValue(source[key])
	}
	for _, key := range keys {
		sanitized[key] = sanitizeArchivedValue(source[key])
	}
	return sanitized
}

func sanitizeArchivedValue(value any) any {
	switch v := value.(type) {
	case string:
		return truncateUTF8(v, maxArchivedStringValueSize)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, child := range v {
			out[key] = sanitizeArchivedValue(child)
		}
		return out
	case []any:
		out := make([]any, 0, len(v))
		for _, child := range v {
			out = append(out, sanitizeArchivedValue(child))
		}
		return out
	default:
		return v
	}
}

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	truncated := value[:maxBytes]
	for truncated != "" && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

func deriveBodyJSONAndMessage(body pcommon.Value) (*string, string, error) {
	if body.Type() == pcommon.ValueTypeEmpty {
		return nil, "", nil
	}

	switch body.Type() {
	case pcommon.ValueTypeStr:
		return deriveFromStringBody(body.Str())
	case pcommon.ValueTypeMap:
		raw := valueToAny(body)
		text, err := canonicalJSONString(raw)
		if err != nil {
			return nil, "", err
		}
		message := extractPreferredMessage(raw)
		if message == "" {
			message = text
		}
		return stringPtr(text), message, nil
	default:
		message, err := canonicalBodyString(body)
		if err != nil {
			return nil, "", err
		}
		return nil, message, nil
	}
}

func deriveFromStringBody(raw string) (*string, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, "", nil
	}
	if !strings.HasPrefix(trimmed, "{") {
		return nil, raw, nil
	}

	var payload any
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, raw, nil
	}

	object, ok := payload.(map[string]any)
	if !ok {
		return nil, raw, nil
	}

	text, err := canonicalJSONString(object)
	if err != nil {
		return nil, "", err
	}
	message := extractPreferredMessage(object)
	if message == "" {
		message = raw
	}
	return stringPtr(text), message, nil
}

func extractPreferredMessage(payload any) string {
	if message, ok := extractStringKey(payload, "message"); ok {
		return message
	}
	if message, ok := extractStringKey(payload, "msg"); ok {
		return message
	}
	if message, ok := extractStringKey(payload, "log"); ok {
		return message
	}
	if object, ok := payload.(map[string]any); ok {
		if nested, ok := object["error"].(map[string]any); ok {
			if message, ok := extractStringKey(nested, "message"); ok {
				return message
			}
		}
	}
	return ""
}

func extractStringKey(payload any, key string) (string, bool) {
	object, ok := payload.(map[string]any)
	if !ok {
		return "", false
	}
	value, ok := object[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	return text, ok
}

func canonicalBodyString(body pcommon.Value) (string, error) {
	raw := valueToAny(body)
	if text, ok := raw.(string); ok {
		return text, nil
	}
	return canonicalJSONString(raw)
}

func canonicalJSONString(value any) (string, error) {
	normalized, err := normalizeJSONValue(value)
	if err != nil {
		return "", err
	}
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(normalized); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

func normalizeJSONValue(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		normalized := make(map[string]any, len(v))
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			child, err := normalizeJSONValue(v[key])
			if err != nil {
				return nil, err
			}
			normalized[key] = child
		}
		return normalized, nil
	case []any:
		normalized := make([]any, 0, len(v))
		for _, child := range v {
			item, err := normalizeJSONValue(child)
			if err != nil {
				return nil, err
			}
			normalized = append(normalized, item)
		}
		return normalized, nil
	case json.Number:
		return normalizeJSONNumber(v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
		return v, nil
	case string, bool, int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		return v, nil
	}
}

func normalizeJSONNumber(number json.Number) (any, error) {
	text := number.String()
	if strings.ContainsAny(text, ".eE") {
		original, _, err := big.ParseFloat(text, 10, 256, big.ToNearestEven)
		if err != nil {
			return text, nil
		}

		value, err := number.Float64()
		if err != nil {
			return text, nil
		}

		roundTrip := new(big.Float).SetPrec(original.Prec()).SetMode(big.ToNearestEven)
		roundTrip.SetFloat64(value)
		if original.Cmp(roundTrip) != 0 {
			return text, nil
		}
		return value, nil
	}

	if value, err := number.Int64(); err == nil {
		return value, nil
	}
	return text, nil
}

func traceIDString(record plog.LogRecord, attrs map[string]any) string {
	traceID := record.TraceID()
	if !traceID.IsEmpty() {
		return traceID.String()
	}
	for _, key := range []string{"trace_id", "otel.trace_id", "dd.trace_id"} {
		if value, ok := attrs[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func spanIDString(record plog.LogRecord, attrs map[string]any) string {
	spanID := record.SpanID()
	if !spanID.IsEmpty() {
		return spanID.String()
	}
	for _, key := range []string{"span_id", "otel.span_id", "dd.span_id"} {
		if value, ok := attrs[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func (a *snowflakeParquetAdapter) hostNameAndServiceNameFromResource(
	ctx context.Context,
	resource pcommon.Resource,
) (string, string) {
	host := ""
	if src, ok := a.attributesTranslator.ResourceToSource(ctx, resource, signalTypeSet, nil); ok &&
		src.Kind == source.HostnameKind {
		host = src.Identifier
	}

	service := ""
	if value, ok := resource.Attributes().Get(serviceNameKey); ok {
		service = value.AsString()
	}

	return host, service
}

func (a *snowflakeParquetAdapter) hostFromAttributes(
	ctx context.Context,
	attrs pcommon.Map,
) string {
	if src, ok := a.attributesTranslator.AttributesToSource(ctx, attrs); ok &&
		src.Kind == source.HostnameKind {
		return src.Identifier
	}
	return ""
}

func flattenAttribute(key string, value pcommon.Value, depth int) map[string]any {
	result := make(map[string]any)
	if value.Type() != pcommon.ValueTypeMap || depth == 10 {
		result[key] = valueToAny(value)
		return result
	}

	value.Map().Range(func(childKey string, childValue pcommon.Value) bool {
		maps.Copy(result, flattenAttribute(key+"."+childKey, childValue, depth+1))
		return true
	})

	return result
}

func valueToAny(value pcommon.Value) any {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return value.Str()
	case pcommon.ValueTypeBool:
		return value.Bool()
	case pcommon.ValueTypeInt:
		return value.Int()
	case pcommon.ValueTypeDouble:
		number := value.Double()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return strconv.FormatFloat(number, 'g', -1, 64)
		}
		return number
	case pcommon.ValueTypeBytes:
		return hex.EncodeToString(value.Bytes().AsRaw())
	case pcommon.ValueTypeMap:
		out := make(map[string]any, value.Map().Len())
		value.Map().Range(func(key string, child pcommon.Value) bool {
			out[key] = valueToAny(child)
			return true
		})
		return out
	case pcommon.ValueTypeSlice:
		out := make([]any, 0, value.Slice().Len())
		for i := 0; i < value.Slice().Len(); i++ {
			out = append(out, valueToAny(value.Slice().At(i)))
		}
		return out
	case pcommon.ValueTypeEmpty:
		return nil
	default:
		return nil
	}
}

func tagsToMap(tags []string) map[string]any {
	return adapters.TagsToMap(tags, true, func(value string) (any, bool) {
		if !isNumeric(value) {
			return nil, false
		}
		return json.Number(value), true
	})
}

func isNumeric(value string) bool {
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false
	}
	return !math.IsNaN(number) && !math.IsInf(number, 0)
}

func stringPtr(value string) *string {
	return &value
}
