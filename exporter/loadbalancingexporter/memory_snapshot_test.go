// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestMemorySnapshotWaitsForRPCPhases(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	healthy := newMemoryTestBackend(t)
	pressured := newMemoryTestBackend(t)
	client := func(backend *memoryTestBackend) plogotlp.GRPCClient {
		conn, err := grpc.NewClient(backend.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return plogotlp.NewGRPCClient(conn)
	}
	healthyClient, pressuredClient := client(healthy), client(pressured)
	logs := plog.NewLogs()
	record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.Attributes().PutInt("request", 0)
	record.Attributes().PutInt("ordinal", 0)
	request := plogotlp.NewExportRequestFromLogs(logs)
	healthyReturn := newMemoryTestRPCBarrier(t)
	retries := newMemoryTestRPCBarrier(t)
	healthy.returnBarrier.Store(healthyReturn)
	pressured.retryBarrier.Store(retries)
	pressured.refuse.Store(true)
	_, err := pressuredClient.Export(ctx, request)
	require.Equal(t, codes.Unavailable, status.Code(err), "prime the refused identity before retrying")
	healthyResult := make(chan error, 1)
	go func() {
		_, callErr := healthyClient.Export(ctx, request)
		healthyResult <- callErr
	}()
	retryResults := make(chan error, 1)
	retryCtx, cancelRetry := context.WithCancel(ctx)
	defer cancelRetry()
	go func() {
		_, callErr := pressuredClient.Export(retryCtx, request)
		retryResults <- callErr
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.Equal(c, int64(1), healthyReturn.held.Load())
		require.Equal(c, int64(1), retries.held.Load())
	}, time.Second, time.Millisecond)
	require.Equal(t, int64(1), healthy.received.Load(), "the received counter advances before the RPC finishes")
	assert.False(t, memoryTestSnapshotReady(healthy, 1, retries), "an unfinished healthy RPC must prevent the snapshot")
	healthyReturn.release()
	require.NoError(t, <-healthyResult)
	healthy.waitForIdleDelivery(t, 1, time.Second)
	require.True(t, memoryTestSnapshotReady(healthy, 1, retries))
	firstRetry := retries.current.Load()
	require.True(t, retries.holds(firstRetry))
	cancelRetry()
	require.Equal(t, codes.Canceled, status.Code(<-retryResults))
	pressured.waitForIdleDelivery(t, 0, time.Second)
	go func() {
		_, callErr := pressuredClient.Export(ctx, request)
		retryResults <- callErr
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		require.Equal(c, int64(1), retries.held.Load())
	}, time.Second, time.Millisecond)
	assert.False(t, retries.holds(firstRetry), "a replacement retry must invalidate the snapshot entry")
	retries.release()
	require.Equal(t, codes.Unavailable, status.Code(<-retryResults))
	pressured.waitForIdleDelivery(t, 0, time.Second)
	missingRetry := newMemoryTestRPCBarrier(t)
	pressured.retryBarrier.Store(missingRetry)
	require.Zero(t, missingRetry.held.Load())
	assert.False(t, memoryTestSnapshotReady(healthy, 1, missingRetry), "a missing pressured RPC must prevent the snapshot")
}
