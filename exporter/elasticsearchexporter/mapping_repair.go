// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/elastic/go-docappender/v2"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/rejectionrepair"
)

// processRepairProof shares the fixed sample budget across all exporters and
// batches for this process. It retains no payload or exporter identifiers.
var processRepairProof rejectionrepair.Proof

// repairRejectedItems runs after the original flush has recorded its unchanged
// telemetry. A separate indexer makes the single repair attempt independent of
// the original indexer's ordinary 429 retries.
func (s *syncBulkIndexer) repairRejectedItems(ctx context.Context, stat docappender.BulkIndexerResponseStat, count int, flushErr error) error {
	attrs := metric.WithAttributes(attribute.String("exporter", s.exporterID))
	var original400 int64
	for _, item := range stat.FailedDocs {
		if item.Status == 400 {
			original400++
		}
	}
	s.telemetryBuilder.ElasticsearchMappingRepairOriginal400.Add(ctx, original400, attrs)
	s.telemetryBuilder.ElasticsearchMappingRepairAttempted.Add(ctx, 0, attrs)
	// Proof counters never carry trace exemplars or request data.
	proofCtx := context.Background()
	s.telemetryBuilder.ElasticsearchMappingRepairProofEqual.Add(proofCtx, 0, attrs)
	s.telemetryBuilder.ElasticsearchMappingRepairProofUnequal.Add(proofCtx, 0, attrs)
	s.telemetryBuilder.ElasticsearchMappingRepairProofUnverified.Add(proofCtx, 0, attrs)
	s.telemetryBuilder.ElasticsearchMappingRepairSucceeded.Add(ctx, 0, attrs)
	s.telemetryBuilder.ElasticsearchMappingRepairUnconfirmed.Add(ctx, 0, attrs)
	terminal := max(0, int64(count)-stat.Indexed-stat.RetriedDocs)
	defer func() { s.telemetryBuilder.ElasticsearchMappingRepairTerminalUnrepaired.Add(ctx, terminal, attrs) }()
	if flushErr != nil {
		terminal = 0
		s.telemetryBuilder.ElasticsearchMappingRepairUnconfirmed.Add(ctx, 1, attrs)
		return nil
	}
	if !correlatedRepairResponse(stat, count) {
		terminal = 0
		s.telemetryBuilder.ElasticsearchMappingRepairUnconfirmed.Add(ctx, 1, attrs)
		return s.mappingRepairFailure("uncorrelated_original_response", count)
	}
	if terminal == 0 {
		return nil
	}
	config := s.config
	if s.repairClient == nil {
		return s.mappingRepairFailure("repair_client_unavailable", count)
	}
	validator := &mappingResponseClient{next: s.repairClient}
	config.Client = validator
	config.MaxDocumentRetries = 0
	config.PopulateFailedDocsInput = false
	retry, err := docappender.NewBulkIndexer(config)
	if err != nil {
		return s.mappingRepairFailure("indexer_configuration", count)
	}
	for _, failed := range stat.FailedDocs {
		field := mappingFailureField(failed)
		if field == "" {
			continue
		}
		item, proof, ok := repairedBulkItem(failed.Input, field, &processRepairProof)
		if !ok {
			continue
		}
		switch proof {
		case rejectionrepair.Equal:
			s.telemetryBuilder.ElasticsearchMappingRepairProofEqual.Add(proofCtx, 1, attrs)
		case rejectionrepair.Unequal:
			s.telemetryBuilder.ElasticsearchMappingRepairProofUnequal.Add(proofCtx, 1, attrs)
		case rejectionrepair.Unverified:
			s.telemetryBuilder.ElasticsearchMappingRepairProofUnverified.Add(proofCtx, 1, attrs)
		}
		if addErr := retry.Add(item); addErr != nil {
			return s.mappingRepairFailure("buffer_failed", count)
		}
	}
	if retry.Items() == 0 {
		return nil
	}
	attempted := retry.Items()
	validator.count = attempted
	s.telemetryBuilder.ElasticsearchMappingRepairAttempted.Add(ctx, int64(attempted), attrs)
	diagnostics := s.diagnostics.begin()
	defer diagnostics.end()
	// Rejected retry responses may quote the preserved value. Keep payloads out
	// of both logs and returned errors; emit only a fixed failure reason below.
	repaired, err := flushBulkIndexer(ctx, retry, s.flushTimeout, s.metadataKeys, s.telemetryBuilder, zap.NewNop(), nil, s.getErrorHintFunc, s.exporterID, &diagnostics)
	if err != nil {
		s.telemetryBuilder.ElasticsearchMappingRepairUnconfirmed.Add(ctx, 1, attrs)
		return s.mappingRepairFailure("request_unconfirmed", attempted)
	}
	if !correlatedRepairResponse(repaired, attempted) {
		s.telemetryBuilder.ElasticsearchMappingRepairUnconfirmed.Add(ctx, 1, attrs)
		return s.mappingRepairFailure("uncorrelated_repair_response", attempted)
	}
	terminal -= repaired.Indexed
	s.telemetryBuilder.ElasticsearchMappingRepairSucceeded.Add(ctx, repaired.Indexed, attrs)
	return nil
}

func (s *syncBulkIndexer) mappingRepairFailure(reason string, items int) error {
	s.logger.Error("mapping repair did not confirm item acceptance", zap.String("reason", reason), zap.Int("items", items))
	// A retry of the whole input batch could replay members already indexed.
	return consumererror.NewPermanent(errors.New("mapping repair failed: " + reason))
}

func correlatedRepairResponse(stat docappender.BulkIndexerResponseStat, count int) bool {
	if stat.Indexed+stat.RetriedDocs+int64(len(stat.FailedDocs)) != int64(count) {
		return false
	}
	previous := -1
	for _, item := range stat.FailedDocs {
		if item.Position <= previous || item.Position >= count {
			return false
		}
		previous = item.Position
	}
	return true
}

func mappingFailureField(item docappender.BulkIndexerResponseItem) string {
	if item.Status != 400 || len(item.Error.Reason) > 2048 ||
		(item.Error.Type != "mapper_parsing_exception" && item.Error.Type != "document_parsing_exception") {
		return ""
	}
	reason := rejectionReasonHeader(item.Error.Reason)
	if rest, ok := strings.CutPrefix(reason, "failed to parse field ["); ok {
		field, typeAndSuffix, ok := strings.Cut(rest, "] of type [")
		if !ok {
			return ""
		}
		_, suffix, ok := strings.Cut(typeAndSuffix, "]")
		if ok && (suffix == "" || strings.HasPrefix(suffix, " in document with id '") || strings.HasPrefix(suffix, ". Preview of field's value: '")) {
			return field
		}
	}
	if rest, ok := strings.CutPrefix(reason, "object mapping for ["); ok {
		field, suffix, ok := strings.Cut(rest, "]")
		if ok && suffix == " tried to parse field ["+field+"] as object, but found a concrete value" {
			return field
		}
	}
	return ""
}

func repairedBulkItem(input, field string, proof *rejectionrepair.Proof) (docappender.BulkIndexerItem, rejectionrepair.ProofResult, bool) {
	metaLine, document, ok := strings.Cut(input, "\n")
	if !ok || !strings.HasSuffix(document, "\n") {
		return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
	}
	document = strings.TrimSuffix(document, "\n")
	if strings.Contains(document, "\n") {
		return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
	}
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(metaLine), &envelope) != nil || len(envelope) != 1 {
		return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
	}
	for action, raw := range envelope {
		if action != docappender.ActionCreate && action != docappender.ActionIndex {
			return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
		}
		var meta struct {
			Index             string            `json:"_index"`
			ID                string            `json:"_id"`
			Pipeline          string            `json:"pipeline"`
			DynamicTemplates  map[string]string `json:"dynamic_templates"`
			RequireDataStream bool              `json:"require_data_stream"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&meta) != nil {
			return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
		}
		if _, err := decoder.Token(); err != io.EOF {
			return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
		}
		body, changed := rejectionrepair.Relocate([]byte(document), field)
		if !changed {
			return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
		}
		return docappender.BulkIndexerItem{Index: meta.Index, DocumentID: meta.ID, Pipeline: meta.Pipeline, DynamicTemplates: meta.DynamicTemplates, RequireDataStream: meta.RequireDataStream, Action: action, Body: bytes.NewReader(body)}, proof.Observe([]byte(document), body, field), true
	}
	return docappender.BulkIndexerItem{}, rejectionrepair.NotSampled, false
}
