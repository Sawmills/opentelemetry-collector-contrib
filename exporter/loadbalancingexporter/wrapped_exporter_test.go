// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWrappedExporterRejectsForcedConsumeAfterShutdownStarts(t *testing.T) {
	exp := newNopMockExporter()
	require.True(t, exp.forceStartConsume())
	activeConsumeReleased := false
	defer func() {
		if !activeConsumeReleased {
			exp.doneConsume()
		}
	}()

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- exp.Shutdown(t.Context())
	}()

	require.Eventually(t, exp.isStopping, time.Second, 10*time.Millisecond)
	lateConsumeStarted := exp.forceStartConsume()
	if lateConsumeStarted {
		exp.doneConsume()
	}
	require.False(t, lateConsumeStarted)

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the active consume completed")
	default:
	}

	exp.doneConsume()
	activeConsumeReleased = true
	require.NoError(t, <-shutdownDone)
}
