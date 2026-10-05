// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver/internal/metadata"
)

// newPubsubTest spins up an in-memory Pub/Sub (pstest) with one topic and one
// subscription, returning a connected client that satisfies SubscriberClient.
// Used by the ack-deadline tests (SAW-12344).
func newPubsubTest(t *testing.T, ctx context.Context) (*pstest.Server, SubscriberClient) {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = srv.GServer.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/my-project/topics/otlp"})
	require.NoError(t, err)
	_, err = srv.GServer.CreateSubscription(ctx, &pubsubpb.Subscription{
		Topic:              "projects/my-project/topics/otlp",
		Name:               "projects/my-project/subscriptions/otlp",
		AckDeadlineSeconds: 10,
	})
	require.NoError(t, err)
	client, err := pubsub.NewSubscriptionAdminClient(ctx, option.WithGRPCConn(conn))
	require.NoError(t, err)
	return srv, client
}

// TestNewHandlerThreadsAckSettings verifies the configurable ack deadline and
// ack batch wait are threaded into the handler (previously hardcoded to 60s/10s).
func TestNewHandlerThreadsAckSettings(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, client := newPubsubTest(t, ctx)
	settings := receivertest.NewNopSettings(metadata.Type)
	telemetryBuilder, _ := metadata.NewTelemetryBuilder(settings.TelemetrySettings)

	h, err := NewHandler(ctx, settings, telemetryBuilder, client, "client-id",
		"projects/my-project/subscriptions/otlp", 123, 2*time.Second,
		func(context.Context, *pubsubpb.ReceivedMessage) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, int32(123), h.streamAckDeadlineSeconds)
	assert.Equal(t, 2*time.Second, h.ackBatchWait)
}

// TestNewHandlerFallsBackToDefaults verifies non-positive values fall back to
// the legacy defaults so direct callers are not surprised.
func TestNewHandlerFallsBackToDefaults(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, client := newPubsubTest(t, ctx)
	settings := receivertest.NewNopSettings(metadata.Type)
	telemetryBuilder, _ := metadata.NewTelemetryBuilder(settings.TelemetrySettings)

	h, err := NewHandler(ctx, settings, telemetryBuilder, client, "client-id",
		"projects/my-project/subscriptions/otlp", 0, 0,
		func(context.Context, *pubsubpb.ReceivedMessage) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, fallbackStreamAckDeadlineSeconds, h.streamAckDeadlineSeconds)
	assert.Equal(t, fallbackAckBatchWait, h.ackBatchWait)
}

// TestHandlerAcksPublishedMessage is an end-to-end check against the pstest fake:
// a published message is consumed and acknowledged (not left to expire/redeliver)
// using the configured ack settings.
func TestHandlerAcksPublishedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv, client := newPubsubTest(t, ctx)
	settings := receivertest.NewNopSettings(metadata.Type)
	telemetryBuilder, _ := metadata.NewTelemetryBuilder(settings.TelemetrySettings)

	var consumed atomic.Int32
	h, err := NewHandler(ctx, settings, telemetryBuilder, client, "client-id",
		"projects/my-project/subscriptions/otlp", 60, 10*time.Millisecond,
		func(context.Context, *pubsubpb.ReceivedMessage) error {
			consumed.Add(1)
			return nil
		})
	require.NoError(t, err)

	srv.Publish("projects/my-project/topics/otlp", []byte("payload"), nil)
	h.RecoverableStream(ctx)
	require.Eventually(t, func() bool { return consumed.Load() >= 1 }, 3*time.Second, 10*time.Millisecond)
	// the message must get acknowledged (not stuck unacked -> redelivered)
	require.Eventually(t, func() bool {
		msgs := srv.Messages()
		return len(msgs) == 1 && msgs[0].Acks >= 1
	}, 3*time.Second, 10*time.Millisecond)
	h.CancelNow()
}

func TestCancelStream(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv := pstest.NewServer()
	defer srv.Close()

	var copts []option.ClientOption
	var dialOpts []grpc.DialOption
	conn, err := grpc.NewClient(srv.Addr, append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, conn.Close()) }()
	copts = append(copts, option.WithGRPCConn(conn))
	_, err = srv.GServer.CreateTopic(ctx, &pubsubpb.Topic{
		Name: "projects/my-project/topics/otlp",
	})
	assert.NoError(t, err)
	_, err = srv.GServer.CreateSubscription(ctx, &pubsubpb.Subscription{
		Topic:              "projects/my-project/topics/otlp",
		Name:               "projects/my-project/subscriptions/otlp",
		AckDeadlineSeconds: 10,
	})
	assert.NoError(t, err)

	settings := receivertest.NewNopSettings(metadata.Type)
	telemetryBuilder, _ := metadata.NewTelemetryBuilder(settings.TelemetrySettings)

	client, err := pubsub.NewSubscriptionAdminClient(ctx, copts...)
	assert.NoError(t, err)

	handler, err := NewHandler(ctx, settings, telemetryBuilder, client, "client-id", "projects/my-project/subscriptions/otlp",
		60, 10*time.Millisecond,
		func(context.Context, *pubsubpb.ReceivedMessage) error {
			return nil
		})
	assert.NoError(t, err)
	srv.Publish("projects/my-project/topics/otlp", []byte{}, map[string]string{
		"ce-type":      "org.opentelemetry.otlp.traces.v1",
		"content-type": "application/protobuf",
	})
	handler.RecoverableStream(ctx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		handler.CancelNow()
	}()
	handler.Wait()
}

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		retry int
		max   time.Duration
	}{
		{
			retry: 0,
			max:   time.Duration(0),
		},
	}
	for i := 1; i <= 11; i++ {
		maxBackoff := min(time.Duration(250.0*math.Pow(2, float64(i-1)))*time.Millisecond, time.Duration(2)*time.Minute)
		tests = append(tests, struct {
			retry int
			max   time.Duration
		}{
			retry: i,
			max:   maxBackoff,
		})
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("retry-%d", tt.retry), func(t *testing.T) {
			for range 10 {
				backoff := exponentialBackoff(tt.retry)
				minBackoffDueToJitter := time.Duration(0.7*float64(tt.max.Milliseconds())) * time.Millisecond
				assert.Condition(t, func() bool { return backoff <= tt.max },
					"exponentialBackoff %s should not go over max %s", backoff.String(), tt.max.String())
				assert.Condition(t, func() bool { return backoff >= minBackoffDueToJitter },
					"exponentialBackoff %s should not go under min (due to jitter) %s", backoff.String(), minBackoffDueToJitter.String())
			}
		})
	}
}
