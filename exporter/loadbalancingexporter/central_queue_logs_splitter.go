// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"
)

const centralQueueLogSplitHeadroom = 4096

type centralQueueLogSplitter struct {
	exporter  *logExporterImp
	codec     *queuePayloadCodec
	hardLimit int
	limit     int
	now       time.Time
	laneCount int

	marshaler              *plog.ProtoMarshaler
	emptyTraceFallbackKeys map[[2]int]pcommon.TraceID
	laneRoutingKeys        map[uint32][]byte
	lanes                  map[string]*centralQueueLogLaneBuilder
}

type centralQueueLogLaneBuilder struct {
	routingKey []byte
	logs       plog.Logs
	size       *centralQueueLogSizeState
	records    int
}

type centralQueueLogSizeState struct {
	resources []centralQueueLogResourceSizeState
	bytes     int
}

type centralQueueLogResourceSizeState struct {
	resource plog.ResourceLogs
	size     int
	scopes   []centralQueueLogScopeSizeState
}

type centralQueueLogScopeSizeState struct {
	scope plog.ScopeLogs
	size  int
}

func newCentralQueueLogSplitter(exporter *logExporterImp, limit int, now time.Time) *centralQueueLogSplitter {
	effectiveLimit := limit
	if effectiveLimit > centralQueueLogSplitHeadroom {
		effectiveLimit -= centralQueueLogSplitHeadroom
	}
	return &centralQueueLogSplitter{
		exporter:               exporter,
		codec:                  exporter.centralCodec,
		hardLimit:              limit,
		limit:                  effectiveLimit,
		now:                    now,
		laneCount:              exporter.effectiveCentralQueueLaneCount(now),
		marshaler:              &plog.ProtoMarshaler{},
		emptyTraceFallbackKeys: make(map[[2]int]pcommon.TraceID),
		laneRoutingKeys:        make(map[uint32][]byte),
		lanes:                  make(map[string]*centralQueueLogLaneBuilder),
	}
}

func centralQueueEffectiveUncompressedItemLimit(settings centralQueueSettings) int {
	limit := settings.maxUncompressedBatchBytes
	if settings.maxInflightUncompressedBytes > 0 && (limit <= 0 || settings.maxInflightUncompressedBytes < int64(limit)) {
		limit = int(settings.maxInflightUncompressedBytes)
	}
	return limit
}

func (s *centralQueueLogSplitter) consume(ctx context.Context, ld plog.Logs) error {
	if err := s.rejectUnsplittableRecords(ctx, ld); err != nil {
		return err
	}

	var errs error
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				rec := sl.LogRecords().At(k)
				balancingKey := s.exporter.routingKeyForLogRecord(rec, [2]int{i, j}, s.emptyTraceFallbackKeys)
				queueRoutingKey := s.balancedLaneRoutingKey(balancingKey)
				lane := s.lane(queueRoutingKey)
				if !lane.empty() && !lane.canFit(rl, sl, rec, s.marshaler, s.limit) {
					if err := s.flushLane(lane); err != nil {
						return multierr.Append(errs, err)
					}
				}
				lane.appendRecord(rl, sl, rec, s.marshaler)
			}
		}
	}

	for _, lane := range s.lanes {
		errs = multierr.Append(errs, s.flushLane(lane))
	}
	return errs
}

func (s *centralQueueLogSplitter) balancedLaneRoutingKey(balancingKey pcommon.TraceID) []byte {
	if s.laneCount <= 0 {
		return balancingKey[:]
	}
	lane := centralQueueLaneIndex(signalKindLogs, balancingKey[:], s.laneCount)
	if key, ok := s.laneRoutingKeys[lane]; ok {
		return key
	}
	key := centralQueueBalancedLaneRoutingKeyForLoadBalancerLane(s.exporter.loadBalancer, signalKindLogs, lane)
	s.laneRoutingKeys[lane] = key
	return key
}

func (s *centralQueueLogSplitter) rejectUnsplittableRecords(ctx context.Context, ld plog.Logs) error {
	if s.hardLimit <= 0 {
		return nil
	}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				single := plog.NewLogs()
				insertLogRecord(single, rl, sl, sl.LogRecords().At(k))
				payload, err := s.marshaler.MarshalLogs(single)
				if err != nil {
					return err
				}
				if len(payload) <= s.hardLimit {
					continue
				}
				encoded, err := s.codec.Encode(payload)
				if err != nil {
					return err
				}
				s.exporter.centralQueue.settings.telemetry.recordRejected(ctx, int64(len(encoded)))
				return errCentralQueueItemTooLarge
			}
		}
	}
	return nil
}

func (s *centralQueueLogSplitter) lane(routingKey []byte) *centralQueueLogLaneBuilder {
	key := string(routingKey)
	lane, ok := s.lanes[key]
	if ok {
		return lane
	}
	lane = newCentralQueueLogLaneBuilder(routingKey)
	s.lanes[key] = lane
	return lane
}

func newCentralQueueLogLaneBuilder(routingKey []byte) *centralQueueLogLaneBuilder {
	return &centralQueueLogLaneBuilder{
		routingKey: append([]byte(nil), routingKey...),
		logs:       plog.NewLogs(),
		size:       &centralQueueLogSizeState{},
	}
}

func (b *centralQueueLogLaneBuilder) empty() bool {
	return b.records == 0
}

func (b *centralQueueLogLaneBuilder) reset() {
	b.logs = plog.NewLogs()
	b.size.reset()
	b.records = 0
}

func (b *centralQueueLogLaneBuilder) canFit(srcRL plog.ResourceLogs, srcSL plog.ScopeLogs, rec plog.LogRecord, marshaler *plog.ProtoMarshaler, limit int) bool {
	if limit <= 0 {
		return true
	}
	return b.size.bytes+b.size.deltaForRecord(srcRL, srcSL, rec, marshaler) <= limit
}

func (b *centralQueueLogLaneBuilder) appendRecord(srcRL plog.ResourceLogs, srcSL plog.ScopeLogs, rec plog.LogRecord, marshaler *plog.ProtoMarshaler) {
	b.size.addRecord(srcRL, srcSL, rec, marshaler)
	insertLogRecord(b.logs, srcRL, srcSL, rec)
	b.records++
}

func (s *centralQueueLogSplitter) flushLane(lane *centralQueueLogLaneBuilder) error {
	if lane.empty() {
		return nil
	}
	defer lane.reset()

	item, err := newCentralQueueLogsItem(lane.routingKey, lane.logs, s.codec, s.now)
	if err != nil {
		return err
	}
	if !s.itemExceedsLimit(item) {
		err := s.exporter.centralQueue.enqueue(item)
		if err == nil {
			s.exporter.observeCentralQueueLaneBytes(item.compressedBytes, s.now)
		}
		return err
	}
	if lane.records == 1 {
		return errCentralQueueItemTooLarge
	}
	return s.exactSplitAndEnqueue(lane.routingKey, lane.logs)
}

func (s *centralQueueLogSplitter) itemExceedsLimit(item centralQueueItem) bool {
	return s.hardLimit > 0 && item.uncompressedBytes > s.hardLimit
}

func (s *centralQueueLogSplitter) exactSplitAndEnqueue(routingKey []byte, logs plog.Logs) error {
	current := plog.NewLogs()
	currentRecords := 0

	flushCurrent := func() error {
		if currentRecords == 0 {
			return nil
		}
		item, err := newCentralQueueLogsItem(routingKey, current, s.codec, s.now)
		if err == nil && s.itemExceedsLimit(item) {
			err = errCentralQueueItemTooLarge
		}
		if err == nil {
			err = s.exporter.centralQueue.enqueue(item)
			if err == nil {
				s.exporter.observeCentralQueueLaneBytes(item.compressedBytes, s.now)
			}
		}
		if err != nil {
			return err
		}
		current = plog.NewLogs()
		currentRecords = 0
		return nil
	}

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				rec := sl.LogRecords().At(k)
				candidate := plog.NewLogs()
				current.CopyTo(candidate)
				insertLogRecord(candidate, rl, sl, rec)
				item, err := newCentralQueueLogsItem(routingKey, candidate, s.codec, s.now)
				if err != nil {
					return err
				}
				if s.itemExceedsLimit(item) {
					if currentRecords == 0 {
						return errCentralQueueItemTooLarge
					}
					if err := flushCurrent(); err != nil {
						return err
					}
					insertLogRecord(current, rl, sl, rec)
					currentRecords = 1
					singleItem, singleErr := newCentralQueueLogsItem(routingKey, current, s.codec, s.now)
					if singleErr == nil && s.itemExceedsLimit(singleItem) {
						singleErr = errCentralQueueItemTooLarge
					}
					if singleErr != nil {
						return singleErr
					}
					continue
				}
				current = candidate
				currentRecords++
			}
		}
	}
	return flushCurrent()
}

func (s *centralQueueLogSizeState) reset() {
	s.resources = nil
	s.bytes = 0
}

func (s *centralQueueLogSizeState) deltaForRecord(srcRL plog.ResourceLogs, srcSL plog.ScopeLogs, rec plog.LogRecord, marshaler *plog.ProtoMarshaler) int {
	recordDelta := logProtoDeltaSize(marshaler.LogRecordSize(rec))
	resourceIndex := s.findResource(srcRL)
	if resourceIndex == -1 {
		resource := plog.NewLogs().ResourceLogs().AppendEmpty()
		srcRL.Resource().CopyTo(resource.Resource())
		resource.SetSchemaUrl(srcRL.SchemaUrl())
		resourceSize := marshaler.ResourceLogsSize(resource)
		scope := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		srcSL.Scope().CopyTo(scope.Scope())
		scope.SetSchemaUrl(srcSL.SchemaUrl())
		scopeSize := marshaler.ScopeLogsSize(scope) + recordDelta
		return logProtoDeltaSize(resourceSize + logProtoDeltaSize(scopeSize))
	}

	resource := s.resources[resourceIndex]
	scopeIndex := s.findScope(resourceIndex, srcSL)
	if scopeIndex == -1 {
		scope := plog.NewLogs().ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		srcSL.Scope().CopyTo(scope.Scope())
		scope.SetSchemaUrl(srcSL.SchemaUrl())
		scopeSize := marshaler.ScopeLogsSize(scope) + recordDelta
		newResourceSize := resource.size + logProtoDeltaSize(scopeSize)
		return logProtoDeltaSize(newResourceSize) - logProtoDeltaSize(resource.size)
	}

	oldScopeSize := resource.scopes[scopeIndex].size
	newScopeSize := oldScopeSize + recordDelta
	newResourceSize := resource.size + logProtoDeltaSize(newScopeSize) - logProtoDeltaSize(oldScopeSize)
	return logProtoDeltaSize(newResourceSize) - logProtoDeltaSize(resource.size)
}

func (s *centralQueueLogSizeState) addRecord(srcRL plog.ResourceLogs, srcSL plog.ScopeLogs, rec plog.LogRecord, marshaler *plog.ProtoMarshaler) {
	delta := s.deltaForRecord(srcRL, srcSL, rec, marshaler)
	s.bytes += delta

	recordDelta := logProtoDeltaSize(marshaler.LogRecordSize(rec))
	resourceIndex := s.findResource(srcRL)
	if resourceIndex == -1 {
		resource := plog.NewLogs().ResourceLogs().AppendEmpty()
		srcRL.Resource().CopyTo(resource.Resource())
		resource.SetSchemaUrl(srcRL.SchemaUrl())
		resourceSize := marshaler.ResourceLogsSize(resource)
		scope := resource.ScopeLogs().AppendEmpty()
		srcSL.Scope().CopyTo(scope.Scope())
		scope.SetSchemaUrl(srcSL.SchemaUrl())
		scopeSize := marshaler.ScopeLogsSize(scope) + recordDelta
		s.resources = append(s.resources, centralQueueLogResourceSizeState{
			resource: resource,
			size:     resourceSize + logProtoDeltaSize(scopeSize),
			scopes: []centralQueueLogScopeSizeState{{
				scope: scope,
				size:  scopeSize,
			}},
		})
		return
	}

	scopeIndex := s.findScope(resourceIndex, srcSL)
	if scopeIndex == -1 {
		scope := s.resources[resourceIndex].resource.ScopeLogs().AppendEmpty()
		srcSL.Scope().CopyTo(scope.Scope())
		scope.SetSchemaUrl(srcSL.SchemaUrl())
		scopeSize := marshaler.ScopeLogsSize(scope) + recordDelta
		s.resources[resourceIndex].scopes = append(s.resources[resourceIndex].scopes, centralQueueLogScopeSizeState{
			scope: scope,
			size:  scopeSize,
		})
		s.resources[resourceIndex].size += logProtoDeltaSize(scopeSize)
		return
	}

	oldScopeSize := s.resources[resourceIndex].scopes[scopeIndex].size
	newScopeSize := oldScopeSize + recordDelta
	s.resources[resourceIndex].scopes[scopeIndex].size = newScopeSize
	s.resources[resourceIndex].size += logProtoDeltaSize(newScopeSize) - logProtoDeltaSize(oldScopeSize)
}

func (s *centralQueueLogSizeState) findResource(resource plog.ResourceLogs) int {
	for i, existing := range s.resources {
		if resourceLogsMatches(existing.resource, resource) {
			return i
		}
	}
	return -1
}

func (s *centralQueueLogSizeState) findScope(resourceIndex int, scope plog.ScopeLogs) int {
	for i, existing := range s.resources[resourceIndex].scopes {
		if scopeLogsMatches(existing.scope, scope) {
			return i
		}
	}
	return -1
}
