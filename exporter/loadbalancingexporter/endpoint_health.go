// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type endpointFailureReason string

const (
	endpointFailureTimeout           endpointFailureReason = "timeout"
	endpointFailureUnavailable       endpointFailureReason = "unavailable"
	endpointFailureConnectionRefused endpointFailureReason = "connection_refused"
	endpointFailureConnectionReset   endpointFailureReason = "connection_reset"
	endpointFailureNoRoute           endpointFailureReason = "no_route"
	endpointFailureDNS               endpointFailureReason = "dns"
	endpointFailureUnknownTransport  endpointFailureReason = "unknown_transport"
)

type endpointHealthSettings struct {
	enabled            bool
	quarantineDuration time.Duration
	rerouteOnFailure   bool
	maxRerouteAttempts int
	now                func() time.Time
}

type endpointHealthManager struct {
	mu             sync.Mutex
	settings       endpointHealthSettings
	endpoints      map[string]*endpointHealthState
	failOpenActive bool
}

type endpointHealthState struct {
	endpoint          string
	present           bool
	quarantinedUntil  time.Time
	failureReason     endpointFailureReason
	lastSeenAt        time.Time
	lastFailedAt      time.Time
	lastStateChangeAt time.Time
}

type endpointHealthReconcileResult struct {
	eligible []string
	removed  []string
	failOpen bool
}

type endpointHealthFailureDecision struct {
	endpointLocal bool
	quarantined   bool
	reason        endpointFailureReason
	eligible      []string
	failOpen      bool
}

func newEndpointHealthManager(settings endpointHealthSettings) *endpointHealthManager {
	if settings.now == nil {
		settings.now = time.Now
	}
	if settings.quarantineDuration <= 0 {
		settings.quarantineDuration = defaultEndpointHealthQuarantineDuration
	}
	return &endpointHealthManager{
		settings:  settings,
		endpoints: make(map[string]*endpointHealthState),
	}
}

func endpointHealthSettingsFromConfig(cfg EndpointHealthConfig) endpointHealthSettings {
	quarantineDuration := cfg.QuarantineDuration
	if quarantineDuration <= 0 {
		quarantineDuration = defaultEndpointHealthQuarantineDuration
	}
	return endpointHealthSettings{
		enabled:            cfg.Enabled,
		quarantineDuration: quarantineDuration,
		rerouteOnFailure:   cfg.RerouteOnFailure,
		maxRerouteAttempts: cfg.MaxRerouteAttempts,
		now:                time.Now,
	}
}

func (m *endpointHealthManager) enabled() bool {
	return m != nil && m.settings.enabled
}

func (m *endpointHealthManager) rerouteOnFailure() bool {
	return m != nil && m.settings.enabled && m.settings.rerouteOnFailure
}

func (m *endpointHealthManager) reconcile(resolved []string) endpointHealthReconcileResult {
	if !m.enabled() {
		eligible := append([]string(nil), resolved...)
		return endpointHealthReconcileResult{eligible: eligible}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.settings.now()
	present := make(map[string]struct{}, len(resolved))
	for _, endpoint := range resolved {
		present[endpoint] = struct{}{}
		state := m.stateLocked(endpoint)
		state.lastSeenAt = now
		if !state.present {
			state.present = true
			state.lastStateChangeAt = now
		}
	}

	var removed []string
	for endpoint, state := range m.endpoints {
		if _, ok := present[endpoint]; ok || !state.present {
			continue
		}
		state.present = false
		state.quarantinedUntil = time.Time{}
		state.failureReason = ""
		state.lastFailedAt = time.Time{}
		state.lastStateChangeAt = now
		removed = append(removed, endpoint)
	}
	sort.Strings(removed)

	eligible, failOpen := m.eligibleEndpointsLocked(now)
	return endpointHealthReconcileResult{
		eligible: eligible,
		removed:  removed,
		failOpen: failOpen,
	}
}

func (m *endpointHealthManager) markFailure(endpoint string, err error) endpointHealthFailureDecision {
	if !m.enabled() {
		return endpointHealthFailureDecision{}
	}

	reason, ok := classifyEndpointFailure(err)
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.settings.now()
	decision := endpointHealthFailureDecision{endpointLocal: ok, reason: reason}
	if ok {
		if state, exists := m.endpoints[endpoint]; exists && state.present {
			state.lastFailedAt = now
			if state.quarantinedUntil.IsZero() || !state.quarantinedUntil.After(now) {
				state.quarantinedUntil = now.Add(m.settings.quarantineDuration)
				state.failureReason = reason
				state.lastStateChangeAt = now
				decision.quarantined = true
			}
		}
	}
	decision.eligible, decision.failOpen = m.eligibleEndpointsLocked(now)
	return decision
}

func (m *endpointHealthManager) markSuccess(endpoint string) bool {
	if !m.enabled() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.endpoints[endpoint]
	if !ok || !state.present {
		return false
	}
	if !state.quarantinedUntil.IsZero() || state.failureReason != "" {
		state.quarantinedUntil = time.Time{}
		state.failureReason = ""
		state.lastStateChangeAt = m.settings.now()
		return true
	}
	return false
}

func (m *endpointHealthManager) isPresent(endpoint string) bool {
	if !m.enabled() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.endpoints[endpoint]
	return ok && state.present
}

func (m *endpointHealthManager) failOpen() bool {
	if !m.enabled() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, _ = m.eligibleEndpointsLocked(m.settings.now())
	return m.failOpenActive
}

func (m *endpointHealthManager) eligibleEndpoints() []string {
	if !m.enabled() {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	eligible, _ := m.eligibleEndpointsLocked(m.settings.now())
	return eligible
}

func (m *endpointHealthManager) stateLocked(endpoint string) *endpointHealthState {
	state, ok := m.endpoints[endpoint]
	if !ok {
		state = &endpointHealthState{endpoint: endpoint}
		m.endpoints[endpoint] = state
	}
	return state
}

func (m *endpointHealthManager) eligibleEndpointsLocked(now time.Time) ([]string, bool) {
	var eligible []string
	var present []string
	for _, state := range m.endpoints {
		if !state.present {
			continue
		}
		present = append(present, state.endpoint)
		if !state.quarantinedUntil.IsZero() && !state.quarantinedUntil.After(now) {
			state.quarantinedUntil = time.Time{}
			state.failureReason = ""
			state.lastStateChangeAt = now
		}
		if state.quarantinedUntil.IsZero() {
			eligible = append(eligible, state.endpoint)
		}
	}

	sort.Strings(present)
	sort.Strings(eligible)
	failOpen := len(present) > 0 && len(eligible) == 0
	m.failOpenActive = failOpen
	if failOpen {
		return present, true
	}
	return eligible, false
}

func classifyEndpointFailure(err error) (endpointFailureReason, bool) {
	if err == nil || consumererror.IsPermanent(err) || errors.Is(err, context.Canceled) {
		return "", false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return endpointFailureTimeout, true
	}
	switch status.Code(err) {
	case codes.DeadlineExceeded:
		return endpointFailureTimeout, true
	case codes.Unavailable:
		return endpointFailureUnavailable, true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return endpointFailureTimeout, true
	}

	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return endpointFailureConnectionRefused, true
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return endpointFailureConnectionReset, true
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return endpointFailureNoRoute, true
	}

	errText := strings.ToLower(err.Error())
	switch {
	case strings.Contains(errText, "i/o timeout"):
		return endpointFailureTimeout, true
	case strings.Contains(errText, "connection refused"):
		return endpointFailureConnectionRefused, true
	case strings.Contains(errText, "connection reset"), strings.Contains(errText, "broken pipe"):
		return endpointFailureConnectionReset, true
	case strings.Contains(errText, "no route to host"),
		strings.Contains(errText, "host unreachable"),
		strings.Contains(errText, "network unreachable"):
		return endpointFailureNoRoute, true
	case strings.Contains(errText, "lookup"), strings.Contains(errText, "no such host"):
		return endpointFailureDNS, true
	case strings.Contains(errText, "transport"),
		strings.Contains(errText, "dial tcp"),
		strings.Contains(errText, "read tcp"),
		strings.Contains(errText, "write tcp"):
		return endpointFailureUnknownTransport, true
	default:
		return "", false
	}
}
