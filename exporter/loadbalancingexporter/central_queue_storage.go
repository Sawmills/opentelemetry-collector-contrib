// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"math"
	"strconv"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.uber.org/zap"
)

const (
	centralStoreManifestKey = "central_queue_v1"
	centralStoreFormatKey   = "central_queue_format"
)

type centralStoreManifest struct {
	Version                  int
	Compression              QueuePayloadCompression
	Head, Tail, NextID       uint64
	Payloads, Records, Bytes int64
	RejectedRecords          int64
}
type centralStoreNode struct {
	Previous, Next uint64
	enqueuedAt     int64 // In-memory metadata; payload owns the persisted timestamp.
}
type centralStoredItem struct {
	Signal                   signalKind
	RoutingKey, Payload      []byte
	UncompressedBytes, Count int
	EnqueuedAt               int64
}
type centralQueueStorage struct {
	client       storage.Client
	mu           sync.Mutex
	manifest     centralStoreManifest
	nodes        map[uint64]centralStoreNode
	failed       error
	failedWrites chan struct{}
	telemetry    *centralQueueTelemetry
	logger       *zap.Logger
}

func openCentralQueueStorage(ctx context.Context, host component.Host, storageID, exporterID component.ID, telemetry *centralQueueTelemetry, logger *zap.Logger) (_ *centralQueueStorage, resultErr error) {
	defer func() {
		if resultErr != nil {
			telemetry.recordStorageFailure(ctx, "open", "extension")
			logger.Error("failed to open durable central queue storage", zap.Error(resultErr))
		}
	}()
	if storageID.Type().String() != "file_storage" {
		return nil, fmt.Errorf("central queue storage %s requires file_storage", storageID)
	}
	extension, ok := host.GetExtensions()[storageID].(storage.Extension)
	if !ok {
		return nil, fmt.Errorf("central queue storage extension %s is unavailable", storageID)
	}
	client, err := extension.GetClient(ctx, component.KindExporter, exporterID, "central_queue_logs")
	if err != nil {
		return nil, fmt.Errorf("open central queue storage %s: %w", storageID, err)
	}
	return &centralQueueStorage{client: client, failedWrites: make(chan struct{}), nodes: make(map[uint64]centralStoreNode), telemetry: telemetry, logger: logger}, nil
}
func centralStoreNodeKey(id uint64) string    { return "index/" + strconv.FormatUint(id, 10) }
func centralStorePayloadKey(id uint64) string { return "item/" + strconv.FormatUint(id, 10) }
func centralStoreNodeBytes(node centralStoreNode) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, node.Previous)
	binary.BigEndian.PutUint64(b[8:], node.Next)
	return b
}

func encodeCentralStoredItem(item centralQueueItem) ([]byte, error) {
	data, err := json.Marshal(centralStoredItem{Signal: item.signal, RoutingKey: item.routingKey, Payload: item.payload, UncompressedBytes: item.uncompressedBytes, Count: item.count, EnqueuedAt: item.enqueuedAtUnixNano})
	if err != nil {
		return nil, err
	}
	result := make([]byte, len(data)+4)
	binary.BigEndian.PutUint32(result, crc32.ChecksumIEEE(data))
	copy(result[4:], data)
	return result, nil
}

func decodeCentralStoredItem(id uint64, data []byte) (centralQueueItem, error) {
	if len(data) < 4 || binary.BigEndian.Uint32(data) != crc32.ChecksumIEEE(data[4:]) {
		return centralQueueItem{}, errors.New("durable central queue payload checksum mismatch")
	}
	var item centralStoredItem
	if err := json.Unmarshal(data[4:], &item); err != nil {
		return centralQueueItem{}, err
	}
	if item.Signal != signalKindLogs || len(item.Payload) == 0 || item.Count <= 0 || item.UncompressedBytes <= 0 || item.EnqueuedAt <= 0 {
		return centralQueueItem{}, errors.New("invalid durable central queue payload")
	}
	return centralQueueItem{durableID: id, signal: item.Signal, routingKey: item.RoutingKey, routingKeyID: string(item.RoutingKey), payload: item.Payload, compressedBytes: len(item.Payload), uncompressedBytes: item.UncompressedBytes, count: item.Count, enqueuedAtUnixNano: item.EnqueuedAt}, nil
}

// load reads the linked pending index without deleting or rewriting any entry.
func (s *centralQueueStorage) load(ctx context.Context, compression QueuePayloadCompression, maxBytes int64) (_ []centralQueueItem, resultErr error) {
	reason := "read"
	defer func() {
		if resultErr != nil {
			s.failure("replay", reason, resultErr)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.client.Get(ctx, centralStoreManifestKey)
	if err != nil {
		return nil, fmt.Errorf("read durable central queue manifest: %w", err)
	}
	format, err := s.client.Get(ctx, centralStoreFormatKey)
	if err != nil {
		return nil, fmt.Errorf("read durable central queue format: %w", err)
	}
	reason = "format"
	if (data == nil) != (format == nil) {
		return nil, errors.New("durable central queue manifest or format marker is missing")
	}
	if format != nil && string(format) != "central_queue/1" {
		return nil, errors.New("incompatible durable central queue format")
	}
	if data == nil {
		reason = "write"
		s.manifest = centralStoreManifest{Version: 1, Compression: compression, NextID: 1}
		encoded, _ := json.Marshal(s.manifest)
		if err = s.client.Batch(ctx, storage.SetOperation(centralStoreManifestKey, encoded), storage.SetOperation(centralStoreFormatKey, []byte("central_queue/1"))); err != nil {
			return nil, fmt.Errorf("initialize durable central queue: %w", err)
		}
		return nil, nil
	}
	reason = "format"
	if err = json.Unmarshal(data, &s.manifest); err != nil {
		return nil, fmt.Errorf("decode durable central queue manifest: %w", err)
	}
	m := s.manifest
	if m.Version != 1 || m.Compression != compression || m.NextID == 0 || m.Payloads < 0 || m.Records < 0 || m.Bytes < 0 || m.RejectedRecords < 0 || (m.Head == 0) != (m.Tail == 0) {
		return nil, errors.New("incompatible or invalid durable central queue manifest")
	}
	if m.Bytes > maxBytes {
		return nil, errors.New("retained durable backlog exceeds configured compressed capacity")
	}
	var items []centralQueueItem
	var previous uint64
	var bytes, records int64
	for id := m.Head; id != 0; {
		if _, exists := s.nodes[id]; exists || int64(len(items)) >= m.Payloads || id >= m.NextID {
			return nil, errors.New("invalid durable central queue index")
		}
		index, payload := storage.GetOperation(centralStoreNodeKey(id)), storage.GetOperation(centralStorePayloadKey(id))
		if err = s.client.Batch(ctx, index, payload); err != nil {
			reason = "read"
			return nil, fmt.Errorf("read durable central queue entry: %w", err)
		}
		if len(index.Value) != 16 {
			return nil, errors.New("missing durable central queue index entry")
		}
		node := centralStoreNode{Previous: binary.BigEndian.Uint64(index.Value), Next: binary.BigEndian.Uint64(index.Value[8:])}
		if node.Previous != previous {
			return nil, errors.New("broken durable central queue index link")
		}
		item, decodeErr := decodeCentralStoredItem(id, payload.Value)
		if decodeErr != nil {
			return nil, decodeErr
		}
		node.enqueuedAt = item.enqueuedAtUnixNano
		s.nodes[id] = node
		items = append(items, item)
		bytes += int64(item.compressedBytes)
		if bytes > maxBytes {
			return nil, errors.New("retained durable backlog exceeds configured compressed capacity")
		}
		records += int64(item.count)
		previous = id
		id = node.Next
	}
	if previous != m.Tail || int64(len(items)) != m.Payloads || bytes != m.Bytes || records != m.Records {
		return nil, errors.New("durable central queue manifest does not match retained entries")
	}
	s.recordPending()
	return items, nil
}

func (s *centralQueueStorage) append(ctx context.Context, items []centralQueueItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	if len(items) == 0 {
		return nil
	}
	m := s.manifest
	if uint64(len(items)) > math.MaxUint64-m.NextID {
		return errors.New("durable central queue identity exhausted")
	}
	ops := make([]*storage.Operation, 0, 2*len(items)+2)
	nodes := make(map[uint64]centralStoreNode, len(items)+1)
	for i := range items {
		id := m.NextID
		m.NextID++
		items[i].durableID = id
		node := centralStoreNode{Previous: m.Tail, enqueuedAt: items[i].enqueuedAtUnixNano}
		if m.Tail != 0 {
			previous, ok := nodes[m.Tail]
			if !ok {
				previous = s.nodes[m.Tail]
			}
			previous.Next = id
			nodes[m.Tail] = previous
		} else {
			m.Head = id
		}
		nodes[id] = node
		m.Tail = id
		m.Payloads++
		m.Records += int64(items[i].count)
		m.Bytes += int64(items[i].compressedBytes)
		data, err := encodeCentralStoredItem(items[i])
		if err != nil {
			return err
		}
		ops = append(ops, storage.SetOperation(centralStorePayloadKey(id), data))
	}
	for id, node := range nodes {
		ops = append(ops, storage.SetOperation(centralStoreNodeKey(id), centralStoreNodeBytes(node)))
	}
	encoded, _ := json.Marshal(m)
	ops = append(ops, storage.SetOperation(centralStoreManifestKey, encoded))
	if err := s.client.Batch(ctx, ops...); err != nil {
		// A failed commit can have an unknown outcome. Stop writes until reopen;
		// overwriting from stale metadata could orphan an accepted entry.
		s.failed = fmt.Errorf("commit durable central queue admission: %w", err)
		close(s.failedWrites)
		s.failure("admit", "write", s.failed)
		return s.failed
	}
	s.manifest = m
	maps.Copy(s.nodes, nodes)
	s.recordPending()
	return nil
}

func (s *centralQueueStorage) acknowledge(ctx context.Context, items []centralQueueItem, rejected int64, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	var records int64
	for i := range items {
		item := &items[i]
		records += int64(item.count)
	}
	if rejected < 0 || rejected > records {
		return s.pauseLocked("acknowledge", "response", errors.New("invalid durable central queue terminal rejection count"))
	}
	m := s.manifest
	m.RejectedRecords += rejected
	changed := make(map[uint64]centralStoreNode)
	removed := make(map[uint64]struct{}, len(items))
	get := func(id uint64) centralStoreNode {
		if n, ok := changed[id]; ok {
			return n
		}
		return s.nodes[id]
	}
	ops := make([]*storage.Operation, 0, 4*len(items)+1)
	for i := range items {
		item := &items[i]
		id := item.durableID
		if _, ok := s.nodes[id]; !ok {
			return errors.New("durable central queue acknowledgement has no owner")
		}
		if _, ok := removed[id]; ok {
			return errors.New("duplicate durable central queue acknowledgement")
		}
		node := get(id)
		removed[id] = struct{}{}
		if node.Previous == 0 {
			m.Head = node.Next
		} else {
			previous := get(node.Previous)
			previous.Next = node.Next
			changed[node.Previous] = previous
		}
		if node.Next == 0 {
			m.Tail = node.Previous
		} else {
			next := get(node.Next)
			next.Previous = node.Previous
			changed[node.Next] = next
		}
		m.Payloads--
		m.Records -= int64(item.count)
		m.Bytes -= int64(item.compressedBytes)
		ops = append(ops, storage.DeleteOperation(centralStoreNodeKey(id)), storage.DeleteOperation(centralStorePayloadKey(id)))
	}
	for id, node := range changed {
		if _, ok := removed[id]; !ok {
			ops = append(ops, storage.SetOperation(centralStoreNodeKey(id), centralStoreNodeBytes(node)))
		}
	}
	encoded, _ := json.Marshal(m)
	ops = append(ops, storage.SetOperation(centralStoreManifestKey, encoded))
	if err := s.client.Batch(ctx, ops...); err != nil {
		s.failed = fmt.Errorf("commit durable central queue acknowledgement: %w", err)
		close(s.failedWrites)
		s.failure("acknowledge", "write", s.failed)
		return s.failed
	}
	s.manifest = m
	for id := range removed {
		delete(s.nodes, id)
	}
	for id, node := range changed {
		if _, ok := removed[id]; !ok {
			s.nodes[id] = node
		}
	}
	s.recordPending()
	if rejected > 0 {
		s.logger.Warn("durable central queue records terminally rejected by destination", zap.Int64("rejected_items", rejected), zap.String("reason", reason))
	}
	return nil
}

func (s *centralQueueStorage) recordPending() {
	var oldest int64
	if s.manifest.Head != 0 {
		oldest = s.nodes[s.manifest.Head].enqueuedAt / 1e9
	}
	s.telemetry.recordDurable(s.manifest.Records, s.manifest.Bytes, oldest)
	s.telemetry.recordDurableRejected(s.manifest.RejectedRecords)
}

func (s *centralQueueStorage) failure(operation, reason string, err error) {
	s.telemetry.recordStorageFailure(context.Background(), operation, reason)
	s.logger.Error("durable central queue storage operation failed", zap.String("operation", operation), zap.String("reason", reason), zap.Error(err))
}

// pause preserves uncertain or unreadable entries and refuses mutations until
// the original owner closes the store and recovery opens it again.
func (s *centralQueueStorage) pause(operation, reason string, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pauseLocked(operation, reason, err)
}

func (s *centralQueueStorage) pauseLocked(operation, reason string, err error) error {
	if s.failed == nil {
		s.failed = err
		close(s.failedWrites)
		s.failure(operation, reason, err)
	}
	return s.failed
}

// Receiving the closed channel publishes the immutable failure without waiting
// behind a storage operation or logging I/O. Later callers only read this error.
func (s *centralQueueStorage) failureError() error {
	select {
	case <-s.failedWrites:
		return s.failed
	default:
		return nil
	}
}
