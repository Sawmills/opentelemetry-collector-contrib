// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver/internal"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver/internal/metadata"
)

const (
	// fallbackStreamAckDeadlineSeconds and fallbackAckBatchWait preserve the receiver's historical
	// behavior when NewHandler is called with non-positive values (e.g. a direct caller that does
	// not thread the config through).
	fallbackStreamAckDeadlineSeconds int32 = 60
	fallbackAckBatchWait                   = 10 * time.Second
)

type StreamHandler struct {
	stream      pubsubpb.Subscriber_StreamingPullClient
	pushMessage func(ctx context.Context, message *pubsubpb.ReceivedMessage) error
	acks        []string
	mutex       sync.Mutex
	client      SubscriberClient

	clientID     string
	subscription string

	cancel context.CancelFunc
	// wait group for the send/receive function
	streamWaitGroup sync.WaitGroup
	// wait group for the handler
	handlerWaitGroup sync.WaitGroup
	settings         receiver.Settings
	telemetryBuilder *metadata.TelemetryBuilder
	// time that acknowledge loop waits before acknowledging messages
	ackBatchWait time.Duration
	// StreamAckDeadlineSeconds requested on the StreamingPull stream
	streamAckDeadlineSeconds int32

	isRunning    atomic.Bool
	retryAttempt int
}

// ack adds the ackID to the list of message to be acknowledged asynchronously
func (handler *StreamHandler) ack(ackID string) {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	handler.acks = append(handler.acks, ackID)
}

func NewHandler(
	ctx context.Context,
	settings receiver.Settings,
	telemetryBuilder *metadata.TelemetryBuilder,
	client SubscriberClient,
	clientID string,
	subscription string,
	ackDeadlineSeconds int32,
	ackBatchWait time.Duration,
	callback func(ctx context.Context, message *pubsubpb.ReceivedMessage) error,
) (*StreamHandler, error) {
	if ackDeadlineSeconds <= 0 {
		ackDeadlineSeconds = fallbackStreamAckDeadlineSeconds
	}
	if ackBatchWait <= 0 {
		ackBatchWait = fallbackAckBatchWait
	}
	handler := StreamHandler{
		settings:                 settings,
		telemetryBuilder:         telemetryBuilder,
		client:                   client,
		clientID:                 clientID,
		subscription:             subscription,
		pushMessage:              callback,
		ackBatchWait:             ackBatchWait,
		streamAckDeadlineSeconds: ackDeadlineSeconds,
	}
	return &handler, handler.initStream(ctx)
}

// initStream creates a new streaming pull stream. When the previous stream was closed, the
// pending acknowledge messages will be acknowledged at stream re-creation.
func (handler *StreamHandler) initStream(ctx context.Context) error {
	var err error
	// Create a stream, but with the receivers context as we don't want to cancel and ongoing operation
	handler.stream, err = handler.client.StreamingPull(ctx)
	if err != nil {
		return err
	}

	request := pubsubpb.StreamingPullRequest{
		Subscription:             handler.subscription,
		StreamAckDeadlineSeconds: handler.streamAckDeadlineSeconds,
		ClientId:                 handler.clientID,
		AckIds:                   handler.acks,
	}
	// SAW-12344 verbose diagnostics: log the exact StreamingPull init parameters so we can
	// confirm the stream is (re)established and with which ack deadline / pending acks.
	handler.settings.Logger.Info("pubsub StreamingPull init request sent",
		zap.String("subscription", handler.subscription),
		zap.String("client_id", handler.clientID),
		zap.Int32("stream_ack_deadline_seconds", handler.streamAckDeadlineSeconds),
		zap.Duration("ack_batch_wait", handler.ackBatchWait),
		zap.Int("carried_over_acks", len(request.AckIds)),
	)
	if err := handler.stream.Send(&request); err != nil {
		handler.settings.Logger.Warn("pubsub StreamingPull init Send failed", zap.Error(err))
		_ = handler.stream.CloseSend()
		return err
	}
	handler.settings.Logger.Info("pubsub StreamingPull stream established", zap.String("subscription", handler.subscription))
	handler.acks = nil
	handler.telemetryBuilder.ReceiverGooglecloudpubsubStreamRestarts.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("otelcol.component.kind", "receiver"),
			attribute.String("otelcol.component.id", handler.settings.ID.String()),
		))
	return nil
}

// firstNStrings returns up to n elements of s, for bounded logging of ack-id lists.
func firstNStrings(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// firstNChars returns up to n runes of s, for bounded logging of a single ack id.
func firstNChars(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// RecoverableStream starts the Pub/Sub stream loop and recovers it if it fails
func (handler *StreamHandler) RecoverableStream(ctx context.Context) {
	handler.handlerWaitGroup.Add(1)
	handler.isRunning.Swap(true)
	var handlerCtx context.Context
	handlerCtx, handler.cancel = context.WithCancel(ctx)
	go handler.recoverableStream(handlerCtx)
}

func (handler *StreamHandler) recoverableStream(ctx context.Context) {
	for handler.isRunning.Load() {
		// Create a new cancelable context for the handler, so we can recover the stream
		var loopCtx context.Context
		loopCtx, cancel := context.WithCancel(ctx)

		handler.settings.Logger.Info("pubsub Starting Streaming Pull loop",
			zap.String("subscription", handler.subscription),
			zap.Int("retry_attempt", handler.retryAttempt))
		handler.streamWaitGroup.Add(2)
		go handler.requestStream(loopCtx, cancel)
		go handler.responseStream(loopCtx, cancel)

		select {
		case <-loopCtx.Done():
			handler.streamWaitGroup.Wait()
		case <-ctx.Done():
			cancel()
			handler.streamWaitGroup.Wait()
		}
		if handler.isRunning.Load() {
			err := handler.initStream(ctx)
			if err != nil {
				handler.settings.Logger.Error("pubsub Failed to recover stream", zap.Int("retry_attempt", handler.retryAttempt+1), zap.Error(err))
				handler.retryAttempt++
			} else {
				handler.retryAttempt = 0
			}
		}
		backoff := exponentialBackoff(handler.retryAttempt)
		handler.settings.Logger.Info("pubsub End of recovery loop, backing off before restart",
			zap.Int("retry_attempt", handler.retryAttempt), zap.Duration("backoff", backoff))
		time.Sleep(backoff)
	}
	handler.settings.Logger.Warn("Shutting down recovery loop.")
	handler.handlerWaitGroup.Done()
}

func (handler *StreamHandler) CancelNow() {
	handler.isRunning.Swap(false)
	if handler.cancel != nil {
		handler.cancel()
		handler.Wait()
	}
}

func (handler *StreamHandler) Wait() {
	handler.handlerWaitGroup.Wait()
}

// acknowledgeMessages will acknowledge the messages, and only clear the outstanding messages when the
// acknowledgement is send successfully
func (handler *StreamHandler) acknowledgeMessages() error {
	handler.mutex.Lock()
	defer handler.mutex.Unlock()
	if len(handler.acks) == 0 {
		return nil
	}
	n := len(handler.acks)
	request := pubsubpb.StreamingPullRequest{
		AckIds: handler.acks,
	}
	err := handler.stream.Send(&request)
	if err == nil {
		handler.settings.Logger.Info("pubsub flushed acks to stream", zap.Int("ack_count", n))
		handler.acks = nil
	} else {
		handler.settings.Logger.Warn("pubsub ack flush Send failed", zap.Int("ack_count", n), zap.Error(err))
	}
	return err
}

// requestStream waits for triggers to acknowledge messages that have been processed by the collector. If
// a stream got restarted, the messages that still needed to be acknowledged are acknowledged at the start
// of the new stream, so we don't need to start with an acknowledgeMessages.
func (handler *StreamHandler) requestStream(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(handler.ackBatchWait)
	var ticks uint64
	for {
		select {
		case <-ctx.Done():
			handler.settings.Logger.Debug("requestStream <-ctx.Done()")
		case <-timer.C:
		}
		ticks++
		// SAW-12344 idle heartbeat: confirms the stream/handler is alive even while Pub/Sub
		// delivers nothing. Logged every ~6 ticks (~30s at the 5s default) to bound volume.
		if ticks%6 == 0 {
			handler.mutex.Lock()
			pending := len(handler.acks)
			handler.mutex.Unlock()
			handler.settings.Logger.Info("pubsub ack-loop heartbeat (stream alive)",
				zap.String("subscription", handler.subscription),
				zap.Uint64("ticks", ticks), zap.Int("pending_acks", pending))
		}
		// whatever happens, we need to acknowledge the messages
		if err := handler.acknowledgeMessages(); err != nil {
			if errors.Is(err, io.EOF) {
				handler.settings.Logger.Warn("EOF reached")
				break
			}
			handler.settings.Logger.Error(fmt.Sprintf("Failed in acknowledge messages with error %v", err))
			break
		}
		// if the context is canceled, we break the loop
		if errors.Is(ctx.Err(), context.Canceled) {
			break
		}
		timer.Reset(handler.ackBatchWait)
	}
	timer.Stop()
	cancel()
	handler.settings.Logger.Debug("Request Stream loop ended.")
	_ = handler.stream.CloseSend()
	handler.streamWaitGroup.Done()
}

func (handler *StreamHandler) responseStream(ctx context.Context, cancel context.CancelFunc) {
	activeStreaming := true
	var recvCount uint64
	for activeStreaming {
		// block until the next message or timeout expires
		resp, err := handler.stream.Recv()
		if err == nil {
			recvCount++
			msgs := resp.GetReceivedMessages()
			// SAW-12344 verbose diagnostics: log every StreamingPull response so we can see whether
			// Pub/Sub is delivering messages, sending empty keep-alives, rejecting our acks
			// (exactly-once), or advertising subscription properties. This is the key RCA signal for
			// "stream healthy but receiving ~0".
			fields := []zap.Field{
				zap.Uint64("recv_seq", recvCount),
				zap.Int("received_messages", len(msgs)),
			}
			if sp := resp.GetSubscriptionProperties(); sp != nil {
				fields = append(fields,
					zap.Bool("exactly_once_delivery", sp.GetExactlyOnceDeliveryEnabled()),
					zap.Bool("message_ordering", sp.GetMessageOrderingEnabled()),
				)
			}
			if ac := resp.GetAcknowledgeConfirmation(); ac != nil {
				fields = append(fields,
					zap.Int("ack_confirmed", len(ac.GetAckIds())),
					zap.Int("ack_invalid", len(ac.GetInvalidAckIds())),
					zap.Int("ack_unordered", len(ac.GetUnorderedAckIds())),
					zap.Int("ack_temp_failed", len(ac.GetTemporaryFailedAckIds())),
					zap.Strings("ack_invalid_ids", firstNStrings(ac.GetInvalidAckIds(), 5)),
					zap.Strings("ack_temp_failed_ids", firstNStrings(ac.GetTemporaryFailedAckIds(), 5)),
				)
			}
			if len(msgs) > 0 {
				first := msgs[0]
				fields = append(fields,
					zap.Int("first_delivery_attempt", int(first.GetDeliveryAttempt())),
					zap.Time("first_publish_time", first.GetMessage().GetPublishTime().AsTime()),
					zap.Int("first_data_bytes", len(first.GetMessage().GetData())),
				)
			}
			handler.settings.Logger.Info("pubsub StreamingPull recv", fields...)

			acked := 0
			for _, message := range msgs {
				// handle all the messages in the response, could be one or more
				err = handler.pushMessage(context.Background(), message)
				if err == nil {
					// When sending a message though the pipeline fails, we ignore the error. We'll let Pubsub
					// handle the flow control.
					handler.ack(message.AckId)
					acked++
				} else {
					handler.settings.Logger.Warn("pubsub pushMessage failed; leaving message unacked",
						zap.String("ack_id_prefix", firstNChars(message.GetAckId(), 12)), zap.Error(err))
				}
			}
			if len(msgs) > 0 {
				handler.settings.Logger.Info("pubsub batch processed", zap.Int("messages", len(msgs)), zap.Int("acked", acked))
			}
		} else {
			s, grpcStatus := status.FromError(err)
			switch {
			case errors.Is(err, io.EOF):
				activeStreaming = false
			case !grpcStatus:
				handler.settings.Logger.Warn("response stream breaking on error",
					zap.Error(err))
				activeStreaming = false
			case s.Code() == codes.Unavailable:
				handler.settings.Logger.Debug("response stream breaking on gRPC s 'Unavailable'")
				activeStreaming = false
			case s.Code() == codes.NotFound:
				handler.settings.Logger.Error("resource doesn't exist, wait 60 seconds, and restarting stream")
				time.Sleep(time.Second * 60)
				activeStreaming = false
			default:
				handler.settings.Logger.Warn("response stream breaking on gRPC s "+s.Message(),
					zap.String("s", s.Message()),
					zap.Error(err))
				activeStreaming = false
			}
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			// Canceling the loop, collector is probably stopping
			handler.settings.Logger.Warn("response stream ctx.Err() == context.Canceled")
			break
		}
	}
	cancel()
	handler.settings.Logger.Debug("Response Stream loop ended.")
	handler.streamWaitGroup.Done()
}

// exponentialBackoff will backoff exponentially with a maximum of 2 minutes
func exponentialBackoff(retryAttempt int) time.Duration {
	if retryAttempt < 1 {
		return 0
	}
	maxDuration := 2 * time.Minute
	backoffMs := 250.0 * math.Pow(2, float64(retryAttempt-1))
	if backoffMs > float64(maxDuration.Milliseconds()) {
		backoffMs = float64(maxDuration.Milliseconds())
	}
	return time.Duration(backoffMs*(0.7+rand.Float64()*0.3)) * time.Millisecond
}
