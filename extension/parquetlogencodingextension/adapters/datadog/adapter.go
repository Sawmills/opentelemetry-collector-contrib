package datadog

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes"
	"github.com/DataDog/opentelemetry-mapping-go/pkg/otlp/attributes/source"
	jsoniter "github.com/json-iterator/go"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension/adapters"
)

const serviceNameKey = "service.name"

var signalTypeSet = attribute.NewSet(attribute.String("signal", "logs"))

type ParquetLog struct {
	Date       string `parquet:"name=date, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	Status     string `parquet:"name=status, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Service    string `parquet:"name=service, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Source     string `parquet:"name=source, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Host       string `parquet:"name=host, type=BYTE_ARRAY, logicaltype=STRING, encoding=RLE_DICTIONARY"`
	Message    string `parquet:"name=message, type=BYTE_ARRAY, logicaltype=STRING, encoding=DELTA_BYTE_ARRAY"`
	Attributes string `parquet:"name=attributes, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=JSON, encoding=DELTA_BYTE_ARRAY"`
	Tags       string `parquet:"name=tags, type=BYTE_ARRAY, convertedtype=UTF8, logicaltype=JSON, encoding=DELTA_BYTE_ARRAY"`
}

type datadogParquetAdapter struct {
	logger               *zap.Logger
	attributesTranslator *attributes.Translator
}

type archiveItem struct {
	Date       string
	Status     string
	Service    string
	Source     string
	Host       string
	Message    string
	Tags       []string
	Attributes map[string]any
}

func NewDatadogParquetAdapter(params extension.Settings) (adapters.ParquetAdapter, error) {
	attributesTranslator, err := attributes.NewTranslator(params.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create attributes translator: %w", err)
	}

	return &datadogParquetAdapter{
		logger:               params.Logger,
		attributesTranslator: attributesTranslator,
	}, nil
}

func (a *datadogParquetAdapter) ConvertToParquet(ctx context.Context, ld plog.Logs) ([]any, error) {
	resourceLogs := ld.ResourceLogs()
	records := make([]any, 0)

	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		res := rl.Resource()
		resourceHost, resourceService := a.hostNameAndServiceNameFromResource(ctx, res)
		scopeLogs := rl.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			logRecords := scopeLogs.At(j).LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				record := logRecords.At(k)
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
					a.logger.Error("failed to transform log record", zap.Error(err))
					continue
				}

				attributesJSON, err := jsoniter.MarshalToString(item.Attributes)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal attributes: %w", err)
				}
				tagsJSON, err := jsoniter.MarshalToString(tagsToMap(item.Tags))
				if err != nil {
					return nil, fmt.Errorf("failed to marshal tags: %w", err)
				}

				records = append(records, ParquetLog{
					Date:       item.Date,
					Status:     item.Status,
					Service:    item.Service,
					Source:     item.Source,
					Host:       item.Host,
					Message:    item.Message,
					Attributes: attributesJSON,
					Tags:       tagsJSON,
				})
			}
		}
	}

	return records, nil
}

func (a *datadogParquetAdapter) Schema() any {
	return new(ParquetLog)
}

func (a *datadogParquetAdapter) transform(
	record plog.LogRecord,
	host, service string,
	resource pcommon.Resource,
) (*archiveItem, error) {
	if hasDdtagsAttribute(resource) {
		return a.transformWithDdTags(record, host, service, resource), nil
	}
	return a.transformDefault(record, host, service, resource), nil
}

func (a *datadogParquetAdapter) transformWithDdTags(
	record plog.LogRecord,
	host, service string,
	resource pcommon.Resource,
) *archiveItem {
	item := &archiveItem{
		Host:       host,
		Service:    service,
		Attributes: make(map[string]any),
	}

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
			for k, v := range flattenAttribute(key, value, 1) {
				item.Attributes[k] = v
			}
		}
		return true
	})

	resource.Attributes().Range(func(key string, value pcommon.Value) bool {
		if key == "ddtags" || key == "hostname" || key == "service" {
			return true
		}
		for k, v := range flattenAttribute(key, value, 1) {
			item.Attributes[k] = v
		}
		return true
	})

	if record.SeverityText() != "" && status == "" {
		status = record.SeverityText()
	}
	if record.SeverityNumber() != 0 && status == "" {
		status = statusFromSeverityNumber(record.SeverityNumber())
	}

	item.Attributes["status"] = status
	item.Status = strings.ToLower(status)
	if record.Timestamp() != 0 {
		date := record.Timestamp().AsTime().Format("2006-01-02T15:04:05.000Z07:00")
		item.Attributes["@timestamp"] = date
		item.Date = date
	}
	if item.Message == "" {
		item.Message = record.Body().AsString()
	}

	ddtags, ok := resource.Attributes().Get("ddtags")
	if ok && ddtags.Type() == pcommon.ValueTypeMap {
		ddtags.Map().Range(func(key string, value pcommon.Value) bool {
			item.Tags = append(item.Tags, key+":"+value.AsString())
			return true
		})
	}
	if item.Service != "" {
		item.Attributes["service"] = item.Service
		item.Tags = append(item.Tags, "service:"+item.Service)
	}
	if item.Source != "" {
		item.Tags = append(item.Tags, "source:"+item.Source)
	}
	if item.Host != "" {
		item.Attributes["hostname"] = item.Host
	}

	return item
}

func (a *datadogParquetAdapter) transformDefault(
	record plog.LogRecord,
	host, service string,
	resource pcommon.Resource,
) *archiveItem {
	item := &archiveItem{
		Host:       host,
		Service:    service,
		Attributes: make(map[string]any),
	}

	status := ""
	record.Attributes().Range(func(key string, value pcommon.Value) bool {
		switch strings.ToLower(key) {
		case "msg", "message", "log":
			item.Message = value.AsString()
		case "ddsource", "datadog.log.source":
			item.Source = value.AsString()
		case "status", "severity", "level", "syslog.severity":
			status = value.AsString()
		case "ddtags":
			item.Tags = append(item.Tags, attributes.TagsFromAttributes(resource.Attributes())...)
			if value.Type() == pcommon.ValueTypeStr {
				item.Tags = append(item.Tags, strings.Split(value.AsString(), ",")...)
			}
		default:
			item.Attributes[key] = valueToAny(value)
		}
		return true
	})

	resource.Attributes().Range(func(key string, value pcommon.Value) bool {
		item.Attributes[key] = valueToAny(value)
		return true
	})

	if record.SeverityText() != "" && status == "" {
		status = record.SeverityText()
	}
	if record.SeverityNumber() != 0 && status == "" {
		status = statusFromSeverityNumber(record.SeverityNumber())
	}

	item.Status = status
	if record.Timestamp() != 0 {
		date := record.Timestamp().AsTime().Format("2006-01-02T15:04:05.000Z07:00")
		item.Attributes["@timestamp"] = date
		item.Date = date
	}
	if item.Message == "" {
		item.Message = record.Body().AsString()
	}
	if len(item.Tags) == 0 {
		item.Tags = attributes.TagsFromAttributes(resource.Attributes())
	}

	return item
}

func (a *datadogParquetAdapter) hostNameAndServiceNameFromResource(
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

func (a *datadogParquetAdapter) hostFromAttributes(
	ctx context.Context,
	attrs pcommon.Map,
) string {
	if src, ok := a.attributesTranslator.AttributesToSource(ctx, attrs); ok &&
		src.Kind == source.HostnameKind {
		return src.Identifier
	}
	return ""
}

func hasDdtagsAttribute(resource pcommon.Resource) bool {
	ddtags, ok := resource.Attributes().Get("ddtags")
	return ok && ddtags.Type() == pcommon.ValueTypeMap
}

func flattenAttribute(key string, value pcommon.Value, depth int) map[string]string {
	result := make(map[string]string)
	if value.Type() != pcommon.ValueTypeMap || depth == 10 {
		result[key] = value.AsString()
		return result
	}

	value.Map().Range(func(childKey string, childValue pcommon.Value) bool {
		for nestedKey, nestedValue := range flattenAttribute(key+"."+childKey, childValue, depth+1) {
			result[nestedKey] = nestedValue
		}
		return true
	})

	return result
}

func statusFromSeverityNumber(severity plog.SeverityNumber) string {
	switch {
	case severity <= 4:
		return "trace"
	case severity <= 8:
		return "debug"
	case severity <= 12:
		return "info"
	case severity <= 16:
		return "warn"
	case severity <= 20:
		return "error"
	case severity <= 24:
		return "fatal"
	default:
		return "error"
	}
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
		return value.Double()
	case pcommon.ValueTypeBytes:
		return string(value.Bytes().AsRaw())
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
	default:
		return nil
	}
}

func tagsToMap(tags []string) map[string]any {
	tagMap := make(map[string]any, len(tags))
	for _, tag := range tags {
		parts := strings.SplitN(tag, ":", 2)
		if len(parts) != 2 {
			continue
		}

		value := parts[1]
		switch {
		case value == "true":
			tagMap[parts[0]] = true
		case value == "false":
			tagMap[parts[0]] = false
		case isNumeric(value):
			number, _ := strconv.ParseFloat(value, 64)
			tagMap[parts[0]] = number
		default:
			tagMap[parts[0]] = value
		}
	}
	return tagMap
}

func isNumeric(value string) bool {
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}
