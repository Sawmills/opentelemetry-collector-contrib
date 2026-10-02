// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestLaneFloorEndToEnd runs in its own network namespace: see ../README.md.
// Only public interfaces are used: collector configuration, OTLP, DNS and metrics.
func TestLaneFloorEndToEnd(t *testing.T) {
	binary := os.Getenv("QUEUEDRAIN_COLLECTOR_BINARY")
	if binary == "" {
		t.Skip("set QUEUEDRAIN_COLLECTOR_BINARY in the isolated lane-floor container")
	}
	artifacts := os.Getenv("QUEUEDRAIN_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	laneFloorMust(t, os.MkdirAll(artifacts, 0o755))

	var membership atomic.Int32
	membership.Store(25)
	startLaneFloorDNS(t, &membership)
	ledger := &laneFloorLedger{records: map[string]int{}, endpoints: map[string]map[int]int{}}
	backends := make([]*grpc.Server, 35)
	startBackend := func(i int) {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.%d:5317", i+10))
		laneFloorMust(t, err)
		srv := grpc.NewServer()
		logspb.RegisterLogsServiceServer(srv, &laneFloorSink{ledger: ledger, endpoint: i})
		backends[i] = srv
		go func() { _ = srv.Serve(listener) }()
	}
	t.Cleanup(func() {
		for _, srv := range backends {
			if srv != nil {
				srv.Stop()
			}
		}
	})
	for i := range backends {
		startBackend(i)
	}
	configPath := filepath.Join(artifacts, "collector.yaml")
	laneFloorMust(t, os.WriteFile(configPath, []byte(laneFloorCollectorConfig), 0o600))
	logFile, err := os.Create(filepath.Join(artifacts, "collector.log"))
	laneFloorMust(t, err)
	t.Cleanup(func() { _ = logFile.Close() })
	collector := exec.CommandContext(t.Context(), binary, "--config", configPath)
	collector.Env = append(os.Environ(), "GODEBUG=netdns=go")
	collector.Stdout, collector.Stderr = logFile, logFile
	laneFloorMust(t, collector.Start())
	t.Cleanup(func() {
		_ = collector.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- collector.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = collector.Process.Kill()
			<-done
		}
	})
	conn, err := grpc.NewClient("127.0.0.1:4317", grpc.WithTransportCredentials(insecure.NewCredentials()))
	laneFloorMust(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := logspb.NewLogsServiceClient(conn)
	httpClient := &http.Client{Timeout: time.Second}
	metrics := func() string {
		resp, getErr := httpClient.Get("http://127.0.0.1:8888/metrics")
		if getErr != nil {
			return ""
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	wait := func(description string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if ready() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s; collector log: %s", description, logFile.Name())
	}
	var expected []string
	sendBatch := func(phase string, batch, size int) {
		t.Helper()
		records := make([]*logpb.LogRecord, size)
		for i := range records {
			id := fmt.Sprintf("%s/%d/%d", phase, batch, i)
			expected = append(expected, id)
			records[i] = &logpb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: id}}}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		response, exportErr := client.Export(ctx, &logspb.ExportLogsServiceRequest{
			ResourceLogs: []*logpb.ResourceLogs{{ScopeLogs: []*logpb.ScopeLogs{{LogRecords: records}}}},
		})
		laneFloorMust(t, exportErr)
		if response.GetPartialSuccess().GetRejectedLogRecords() != 0 {
			t.Fatal("collector rejected input records")
		}
	}
	delivered := func() bool {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		return len(ledger.records) >= len(expected)
	}
	drained := func() bool {
		return laneFloorMetric(metrics(), "otelcol_loadbalancer_central_queue_compressed_bytes", "") == 0
	}
	for _, phase := range []struct {
		name              string
		resolved, healthy int
		lanes             int
		duration          time.Duration
	}{
		{"initial", 25, 25, 25, 3 * time.Second},
		{"growth", 35, 35, 35, 35 * time.Second}, // Cross the 30-second ingest-rate window.
		{"quarantine", 35, 20, 35, 3 * time.Second},
		{"recovery", 35, 35, 35, 5 * time.Second},
		{"shrink", 17, 17, 17, 3 * time.Second},
		{"regrowth", 25, 25, 25, 3 * time.Second},
	} {
		var release func()
		if phase.name != "initial" {
			block := make(chan struct{})
			ledger.mu.Lock()
			ledger.block = block
			ledger.mu.Unlock()
			release = sync.OnceFunc(func() { close(block) })
			t.Cleanup(release)
			sendBatch("transition-"+phase.name, 0, 3500)
		}
		membership.Store(int32(phase.resolved))
		if phase.name == "quarantine" {
			for _, srv := range backends[20:] {
				srv.Stop() // Real TCP probe failures, without changing DNS membership.
			}
		}
		if phase.name == "recovery" {
			for i := 20; i < len(backends); i++ {
				startBackend(i)
			}
		}
		wait(phase.name+" discovery and health", func() bool {
			text := metrics()
			return laneFloorMetric(text, "otelcol_loadbalancer_num_backends", "") == float64(phase.resolved) &&
				laneFloorMetric(text, "otelcol_loadbalancer_backend_state", `state="eligible"`) == float64(phase.healthy)
		})
		if release != nil {
			// This request recalculates lanes after discovery/health changed while
			// the sink barrier keeps already accepted work outstanding.
			sendBatch(phase.name, -1, 350)
			wait(phase.name+" pending work after topology update", func() bool {
				return laneFloorMetric(metrics(), "otelcol_loadbalancer_central_queue_items", "") > 0
			})
			text := metrics()
			laneFloorMust(t, os.WriteFile(filepath.Join(artifacts, phase.name+"-transition.prom"), []byte(text), 0o600))
			t.Logf("transition=%s observed_resolved=%.0f eligible=%.0f lanes=%.0f pending_items=%.0f", phase.name,
				laneFloorMetric(text, "otelcol_loadbalancer_num_backends", ""),
				laneFloorMetric(text, "otelcol_loadbalancer_backend_state", `state="eligible"`),
				laneFloorMetric(text, "otelcol_loadbalancer_central_queue_effective_lanes", ""),
				laneFloorMetric(text, "otelcol_loadbalancer_central_queue_items", ""))
			release()
		}
		started := time.Now()
		var peakQueue, peakAge float64
		batches := int(phase.duration / (100 * time.Millisecond))
		for batch := range batches {
			sendBatch(phase.name, batch, 350)
			text := metrics()
			queue := laneFloorMetric(text, "otelcol_loadbalancer_central_queue_compressed_bytes", "")
			age := laneFloorMetric(text, "otelcol_loadbalancer_central_queue_oldest_item_age", "")
			if queue < 0 || age < 0 {
				t.Fatal("missing required queue byte/age telemetry")
			}
			peakQueue = max(peakQueue, queue)
			peakAge = max(peakAge, age)
			time.Sleep(100 * time.Millisecond)
		}
		wait(phase.name+" delivery", delivered)
		wait(phase.name+" queue drain", drained)
		text := metrics()
		laneFloorMust(t, os.WriteFile(filepath.Join(artifacts, phase.name+".prom"), []byte(text), 0o600))
		lanes := laneFloorMetric(text, "otelcol_loadbalancer_central_queue_effective_lanes", "")
		ledger.mu.Lock()
		distribution := maps.Clone(ledger.endpoints[phase.name])
		encoded, marshalErr := json.Marshal(distribution)
		ledger.mu.Unlock()
		laneFloorMust(t, marshalErr)
		laneFloorMust(t, os.WriteFile(filepath.Join(artifacts, phase.name+"-delivery.json"), encoded, 0o600))
		t.Logf("phase=%s resolved=%d healthy=%d lanes=%.0f reached=%d records=%d delivered_records_per_second=%.0f peak_queue_bytes=%.0f peak_age_ms=%.0f delivery=%s", phase.name, phase.resolved, phase.healthy, lanes, len(distribution), batches*350, float64(batches*350)/time.Since(started).Seconds(), peakQueue, peakAge, encoded)
		if lanes != float64(phase.lanes) {
			t.Errorf("%s: lanes=%.0f, want %d", phase.name, lanes, phase.lanes)
		}
		if len(distribution) != phase.healthy {
			t.Errorf("%s: reached %d backends, want %d", phase.name, len(distribution), phase.healthy)
		}
		for endpoint := range distribution {
			if endpoint >= phase.healthy {
				t.Errorf("%s: delivered to ineligible backend %d", phase.name, endpoint)
			}
		}
		if peakQueue <= 0 || peakQueue >= 16<<20 || peakAge > 10_000 {
			t.Errorf("%s: unexpected queue pressure: bytes=%.0f age_ms=%.0f", phase.name, peakQueue, peakAge)
		}
		if refused := laneFloorMetric(text, "otelcol_receiver_refused_log_records", ""); refused != 0 {
			t.Errorf("%s: receiver refusal counter must be present and zero, got %.0f", phase.name, refused)
		}
		// The queue rejection series is created only on rejection. Unlike the
		// receiver refusal counter above, its absence is expected on success.
		if rejected := laneFloorMetric(text, "otelcol_loadbalancer_central_queue_rejected_compressed_bytes", ""); rejected > 0 {
			t.Errorf("%s: rejected %.0f compressed bytes", phase.name, rejected)
		}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	for _, id := range expected {
		if ledger.records[id] != 1 {
			t.Fatalf("record %q delivered %d times, want exactly once", id, ledger.records[id])
		}
	}
	if len(ledger.records) != len(expected) {
		t.Fatalf("received %d distinct IDs, want %d", len(ledger.records), len(expected))
	}
	t.Logf("verified %d unique records, no missing or duplicate deliveries", len(expected))
}

type laneFloorLedger struct {
	mu        sync.Mutex
	block     <-chan struct{}
	records   map[string]int
	endpoints map[string]map[int]int
}

type laneFloorSink struct {
	logspb.UnimplementedLogsServiceServer
	ledger   *laneFloorLedger
	endpoint int
}

func (s *laneFloorSink) Export(ctx context.Context, request *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	s.ledger.mu.Lock()
	block := s.ledger.block
	s.ledger.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	select {
	case <-time.After(100 * time.Millisecond): // Exercise asynchronous queueing and draining.
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.ledger.mu.Lock()
	defer s.ledger.mu.Unlock()
	for _, resource := range request.GetResourceLogs() {
		for _, scope := range resource.GetScopeLogs() {
			for _, record := range scope.GetLogRecords() {
				id := record.GetBody().GetStringValue()
				s.ledger.records[id]++
				phase, _, _ := strings.Cut(id, "/")
				if s.ledger.endpoints[phase] == nil {
					s.ledger.endpoints[phase] = map[int]int{}
				}
				s.ledger.endpoints[phase][s.endpoint]++
			}
		}
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}

func startLaneFloorDNS(t *testing.T, membership *atomic.Int32) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:53")
	laneFloorMust(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 4096)
		for {
			n, address, readErr := conn.ReadFrom(buf)
			if readErr != nil {
				return
			}
			var message dnsmessage.Message
			if message.Unpack(buf[:n]) != nil {
				continue
			}
			message.Response = true
			message.Authoritative = true
			for j := range message.Questions {
				question := &message.Questions[j]
				if question.Name.String() != "lane-floor.test." || question.Type != dnsmessage.TypeA {
					continue
				}
				for i := range int(membership.Load()) {
					message.Answers = append(message.Answers, dnsmessage.Resource{
						Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET},
						Body:   &dnsmessage.AResource{A: [4]byte{127, 0, 0, byte(i + 10)}},
					})
				}
			}
			packet, packErr := message.Pack()
			if packErr == nil {
				_, _ = conn.WriteTo(packet, address)
			}
		}
	}()
}

func laneFloorMetric(text, prefix, label string) float64 {
	var sum float64
	found := false
	for line := range strings.SplitSeq(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.HasPrefix(fields[0], prefix) || !strings.Contains(fields[0], label) {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			sum += value
			found = true
		}
	}
	if !found {
		return -1 // Missing telemetry must not look like an empty queue.
	}
	return sum
}

func laneFloorMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

const laneFloorCollectorConfig = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:4317
exporters:
  loadbalancing:
    protocol:
      otlp:
        timeout: 5s
        tls:
          insecure: true
    resolver:
      dns:
        hostname: lane-floor.test
        port: "5317"
        interval: 200ms
        timeout: 1s
    log_routing:
      ignore_trace_id: true
      record_striping_enabled: true
    endpoint_health:
      enabled: true
      # In-flight transport failures may outlive the failed TCP probe. Keep
      # their quarantine below the recovery wait; probes still exclude stopped sinks.
      quarantine_duration: 1s
      max_quarantined_percent: 100
      active_probe:
        enabled: true
        interval: 200ms
        timeout: 100ms
        jitter: 0%
        max_concurrency: 35
        fall: 1
        rise: 1
    central_queue:
      enabled: true
      max_compressed_bytes: 16777216
      target_compressed_bytes: 262144
      max_batch_delay: 100ms
      num_consumers: 8
      active_load_balancer_replicas: 1
      max_lanes: 64
      lane_hysteresis_factor: 2
service:
  telemetry:
    metrics:
      readers:
        - pull:
            exporter:
              prometheus:
                host: 127.0.0.1
                port: 8888
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [loadbalancing]
`
