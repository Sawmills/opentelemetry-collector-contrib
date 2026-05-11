package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestDrainTogglesReadinessAndHealth(t *testing.T) {
	s := &server{started: time.Now()}

	assertServing(t, s, true)

	recorder := httptest.NewRecorder()
	s.handleDrain(recorder, httptest.NewRequest(http.MethodPost, "/drain", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("drain status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	assertServing(t, s, false)

	recorder = httptest.NewRecorder()
	s.handleUndrain(recorder, httptest.NewRequest(http.MethodPost, "/undrain", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("undrain status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	assertServing(t, s, true)
}

func assertServing(t *testing.T, s *server, want bool) {
	t.Helper()

	if got := !s.isUnready(); got != want {
		t.Fatalf("ready = %v, want %v", got, want)
	}

	resp, err := s.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	gotServing := resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	if gotServing != want {
		t.Fatalf("health serving = %v, want %v", gotServing, want)
	}
}
