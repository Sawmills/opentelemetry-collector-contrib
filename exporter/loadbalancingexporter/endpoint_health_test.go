// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEndpointHealthReconcileMarksStaleAndEligible(t *testing.T) {
	now := time.Unix(100, 0)
	manager := newEndpointHealthManager(endpointHealthSettings{
		enabled:            true,
		quarantineDuration: 30 * time.Second,
		now:                func() time.Time { return now },
	})

	result := manager.reconcile([]string{"endpoint-1", "endpoint-2"})
	require.Equal(t, []string{"endpoint-1", "endpoint-2"}, result.eligible)
	require.Empty(t, result.removed)

	result = manager.reconcile([]string{"endpoint-2"})
	require.Equal(t, []string{"endpoint-2"}, result.eligible)
	require.Equal(t, []string{"endpoint-1"}, result.removed)
	require.False(t, manager.isPresent("endpoint-1"))
	require.True(t, manager.isPresent("endpoint-2"))
}

func TestEndpointHealthQuarantinesOnFirstEndpointLocalFailure(t *testing.T) {
	now := time.Unix(100, 0)
	manager := newEndpointHealthManager(endpointHealthSettings{
		enabled:            true,
		quarantineDuration: 30 * time.Second,
		now:                func() time.Time { return now },
	})
	manager.reconcile([]string{"endpoint-1", "endpoint-2"})

	decision := manager.markFailure("endpoint-1", status.Error(codes.Unavailable, "backend unavailable"))
	require.True(t, decision.endpointLocal)
	require.True(t, decision.quarantined)
	require.Equal(t, endpointFailureUnavailable, decision.reason)
	require.Equal(t, []string{"endpoint-2"}, decision.eligible)

	now = now.Add(31 * time.Second)
	require.Equal(t, []string{"endpoint-1", "endpoint-2"}, manager.eligibleEndpoints())
}

func TestEndpointHealthDoesNotQuarantinePermanentErrors(t *testing.T) {
	now := time.Unix(100, 0)
	manager := newEndpointHealthManager(endpointHealthSettings{
		enabled:            true,
		quarantineDuration: 30 * time.Second,
		now:                func() time.Time { return now },
	})
	manager.reconcile([]string{"endpoint-1", "endpoint-2"})

	decision := manager.markFailure("endpoint-1", consumererror.NewPermanent(errors.New("bad payload")))
	require.False(t, decision.endpointLocal)
	require.False(t, decision.quarantined)
	require.Equal(t, []string{"endpoint-1", "endpoint-2"}, decision.eligible)
}

func TestEndpointHealthFailOpenWhenAllPresentEndpointsQuarantined(t *testing.T) {
	now := time.Unix(100, 0)
	manager := newEndpointHealthManager(endpointHealthSettings{
		enabled:            true,
		quarantineDuration: 30 * time.Second,
		now:                func() time.Time { return now },
	})
	manager.reconcile([]string{"endpoint-1", "endpoint-2"})
	manager.markFailure("endpoint-1", status.Error(codes.Unavailable, "backend unavailable"))

	decision := manager.markFailure("endpoint-2", context.DeadlineExceeded)
	require.True(t, decision.failOpen)
	require.Equal(t, []string{"endpoint-1", "endpoint-2"}, decision.eligible)
	require.True(t, manager.failOpen())
}

func TestEndpointFailureClassification(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason endpointFailureReason
		ok     bool
	}{
		{
			name:   "context deadline exceeded",
			err:    context.DeadlineExceeded,
			reason: endpointFailureTimeout,
			ok:     true,
		},
		{
			name:   "grpc deadline exceeded",
			err:    status.Error(codes.DeadlineExceeded, "deadline exceeded"),
			reason: endpointFailureTimeout,
			ok:     true,
		},
		{
			name:   "grpc unavailable",
			err:    status.Error(codes.Unavailable, "unavailable"),
			reason: endpointFailureUnavailable,
			ok:     true,
		},
		{
			name:   "connection refused",
			err:    &net.OpError{Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)},
			reason: endpointFailureConnectionRefused,
			ok:     true,
		},
		{
			name:   "connection reset fallback",
			err:    errors.New("write tcp: connection reset by peer"),
			reason: endpointFailureConnectionReset,
			ok:     true,
		},
		{
			name:   "no route fallback",
			err:    errors.New("dial tcp: no route to host"),
			reason: endpointFailureNoRoute,
			ok:     true,
		},
		{
			name: "permanent error",
			err:  consumererror.NewPermanent(errors.New("bad data")),
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, ok := classifyEndpointFailure(tt.err)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.reason, reason)
		})
	}
}
