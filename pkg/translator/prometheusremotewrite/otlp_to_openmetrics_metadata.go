// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package prometheusremotewrite // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheusremotewrite"

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/prompb"
	prom "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"

	prometheustranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
)

func otelMetricTypeToPromMetricType(otelMetric pmetric.Metric) prompb.MetricMetadata_MetricType {
	// metric metadata can be used to support Prometheus types that don't exist
	// in OpenTelemetry.
	typeFromMetadata, hasTypeFromMetadata := otelMetric.Metadata().Get(prometheustranslator.MetricMetadataTypeKey)
	switch otelMetric.Type() {
	case pmetric.MetricTypeGauge:
		if hasTypeFromMetadata && typeFromMetadata.Str() == string(model.MetricTypeUnknown) {
			return prompb.MetricMetadata_UNKNOWN
		}
		return prompb.MetricMetadata_GAUGE
	case pmetric.MetricTypeSum:
		if otelMetric.Sum().IsMonotonic() {
			return prompb.MetricMetadata_COUNTER
		}
		if hasTypeFromMetadata && typeFromMetadata.Str() == string(model.MetricTypeInfo) {
			return prompb.MetricMetadata_INFO
		}
		if hasTypeFromMetadata && typeFromMetadata.Str() == string(model.MetricTypeStateset) {
			return prompb.MetricMetadata_STATESET
		}
		return prompb.MetricMetadata_GAUGE
	case pmetric.MetricTypeHistogram:
		return prompb.MetricMetadata_HISTOGRAM
	case pmetric.MetricTypeSummary:
		return prompb.MetricMetadata_SUMMARY
	case pmetric.MetricTypeExponentialHistogram:
		return prompb.MetricMetadata_HISTOGRAM
	}
	return prompb.MetricMetadata_UNKNOWN
}

func OtelMetricsToMetadata(md pmetric.Metrics, addMetricSuffixes bool, namespace string) ([]*prompb.MetricMetadata, error) {
	return OtelMetricsToMetadataWithSettings(md, Settings{AddMetricSuffixes: addMetricSuffixes, Namespace: namespace})
}

// OtelMetricsToMetadataWithSettings translates remote-write v1 metadata and
// retains declared source families when NoTranslation preserves scraped samples.
func OtelMetricsToMetadataWithSettings(md pmetric.Metrics, settings Settings) ([]*prompb.MetricMetadata, error) {
	if err := ValidateTranslationStrategy(settings.TranslationStrategy); err != nil {
		return nil, err
	}
	resourceMetricsSlice := md.ResourceMetrics()

	metadataLength := 0
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		scopeMetricsSlice := resourceMetricsSlice.At(i).ScopeMetrics()
		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			metadataLength += scopeMetricsSlice.At(j).Metrics().Len()
		}
	}

	metricNamer := settings.metricNamer()
	unitNamer := otlptranslator.UnitNamer{}
	metadata := make([]*prompb.MetricMetadata, 0, metadataLength)
	var errs error
	for i := 0; i < resourceMetricsSlice.Len(); i++ {
		resourceMetrics := resourceMetricsSlice.At(i)
		scopeMetricsSlice := resourceMetrics.ScopeMetrics()

		for j := 0; j < scopeMetricsSlice.Len(); j++ {
			scopeMetrics := scopeMetricsSlice.At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)
				if present, ok := metric.Metadata().Get(prometheustranslator.MetricMetadataPresentKey); ok && present.Type() == pcommon.ValueTypeBool && !present.Bool() {
					// A creation sample can carry its declared orphan parent.
					if settings.TranslationStrategy == otlptranslator.NoTranslation {
						if parent := createdFamilyMetadata(metric, settings.Namespace); parent != nil {
							metadata = append(metadata, parent)
						}
					}
					continue
				}
				translated := prom.TranslatorMetricFromOtelMetric(metric)
				if settings.TranslationStrategy == otlptranslator.NoTranslation {
					if family, ok := metric.Metadata().Get(prometheustranslator.MetricMetadataFamilyKey); ok && family.Type() == pcommon.ValueTypeStr && family.Str() != "" {
						// Require the original sample name, including for a rename
						// that only adds or removes the counter suffix.
						source, hasSource := metric.Metadata().Get(prometheustranslator.MetricMetadataSourceNameKey)
						if hasSource && source.Type() == pcommon.ValueTypeStr && source.Str() == metric.Name() &&
							(family.Str() == metric.Name() || (metric.Type() == pmetric.MetricTypeSum && metric.Sum().IsMonotonic() && family.Str()+"_total" == metric.Name())) {
							translated.Name = family.Str()
						}
					}
				}
				metricName, err := metricNamer.Build(translated)
				if err != nil {
					errs = multierr.Append(errs, err)
					continue
				}
				entry := prompb.MetricMetadata{
					Type:             otelMetricTypeToPromMetricType(metric),
					MetricFamilyName: metricName,
					Unit:             unitNamer.Build(metric.Unit()),
					Help:             metric.Description(),
				}
				metadata = append(metadata, &entry)
			}
		}
	}

	return metadata, errs
}

// createdFamilyMetadata retains a real declaration whose only remaining sample
// is _created. Renaming that sample invalidates its parent's provenance.
func createdFamilyMetadata(metric pmetric.Metric, namespace string) *prompb.MetricMetadata {
	declaration, ok := metric.Metadata().Get(prometheustranslator.MetricMetadataCreatedFamilyKey)
	if !ok || declaration.Type() != pcommon.ValueTypeMap {
		return nil
	}
	fields := declaration.Map()
	values := make(map[string]string, 5)
	for _, key := range []string{"sample_name", "family", "type", "help", "unit"} {
		field, ok := fields.Get(key)
		if !ok || field.Type() != pcommon.ValueTypeStr {
			return nil
		}
		values[key] = field.Str()
	}
	if values["sample_name"] != metric.Name() || values["family"] == "" || values["family"]+"_created" != metric.Name() {
		return nil
	}
	var typ prompb.MetricMetadata_MetricType
	switch model.MetricType(values["type"]) {
	case model.MetricTypeCounter:
		typ = prompb.MetricMetadata_COUNTER
	case model.MetricTypeHistogram:
		typ = prompb.MetricMetadata_HISTOGRAM
	case model.MetricTypeSummary:
		typ = prompb.MetricMetadata_SUMMARY
	default:
		return nil
	}
	name := values["family"]
	if namespace != "" {
		name = namespace + "_" + name
	}
	return &prompb.MetricMetadata{Type: typ, MetricFamilyName: name, Help: values["help"], Unit: values["unit"]}
}
