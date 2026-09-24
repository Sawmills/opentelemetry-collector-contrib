// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3receiver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3receiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awss3receiver/internal/metadatatest"
)

func newNopTelemetryBuilder(t *testing.T) *metadata.TelemetryBuilder {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	return telemetry
}

func newTestMetricsReceiver(t *testing.T, next consumer.Metrics) *awss3Receiver {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverCreateSettings: receivertest.NewNopSettings(metadata.Type)})
	require.NoError(t, err)
	return &awss3Receiver{logger: zap.NewNop(), obsrecv: obsrecv, dataProcessor: &metricsReceiver{consumer: next}}
}

func newDropTestReader(t *testing.T, tt *componenttest.Telemetry, s3Client SingleObjectAPI, sqsAPI sqsClient) *s3SQSNotificationReader {
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	return &s3SQSNotificationReader{
		logger:              zap.NewNop(),
		s3Client:            s3Client,
		sqsClient:           sqsAPI,
		queueURL:            "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
		s3Bucket:            "test-bucket",
		maxNumberOfMessages: 10,
		waitTimeSeconds:     20,
		telemetry:           telemetry,
	}
}

func receiveOnce(mockSQS *mockSQSClient, messages ...types.Message) {
	mockSQS.On("ReceiveMessage", mock.Anything, mock.Anything).Return(
		&sqs.ReceiveMessageOutput{Messages: messages}, nil,
	).Once()
	// Later polls block until the test context ends, as SQS long polling would, instead of spinning.
	mockSQS.On("ReceiveMessage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		<-args.Get(0).(context.Context).Done()
	}).Return(&sqs.ReceiveMessageOutput{Messages: []types.Message{}}, nil)
}

func expectDelete(mockSQS *mockSQSClient, receiptHandle string) {
	mockSQS.On("DeleteMessage", mock.Anything, mock.MatchedBy(func(input *sqs.DeleteMessageInput) bool {
		return *input.ReceiptHandle == receiptHandle
	})).Return(&sqs.DeleteMessageOutput{}, nil).Once()
}

func runReader(t *testing.T, reader *s3SQSNotificationReader, callback s3ObjectCallback) {
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	err := reader.readAll(ctx, "metrics", callback)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func objectCreatedMessage(t *testing.T, receiptHandle, key string) types.Message {
	body, err := json.Marshal(s3EventNotification{Records: []s3EventRecord{{
		EventSource: "aws:s3",
		EventName:   "ObjectCreated:Put",
		S3:          s3Data{Bucket: s3BucketData{Name: "test-bucket"}, Object: s3ObjectData{Key: key}},
	}}})
	require.NoError(t, err)
	return types.Message{Body: aws.String(string(body)), ReceiptHandle: aws.String(receiptHandle)}
}

func TestS3SQSReader_DeletesUnreadableMessages(t *testing.T) {
	snsWithBadPayload, err := json.Marshal(snsMessage{Type: "Notification", Message: "not json"})
	require.NoError(t, err)

	tests := []struct {
		name   string
		body   string
		reason string
	}{
		{name: "not json", body: "plain text", reason: "invalid_json"},
		{name: "json array", body: "[]", reason: "not_s3_notification"},
		{name: "eventbridge event", body: `{"version":"0","detail-type":"Object Created","source":"aws.s3"}`, reason: "not_s3_notification"},
		{name: "sns with bad payload", body: string(snsWithBadPayload), reason: "invalid_sns_message"},
		{name: "sns without s3 records", body: `{"Type":"Notification","Message":"{}"}`, reason: "invalid_sns_message"},
		{
			// The direct parse fills Records before it fails on Event; the SNS payload must not reuse them.
			name:   "sns without s3 records after partial direct parse",
			body:   `{"Records":[{"eventSource":"aws:s3","eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"test-bucket"},"object":{"key":"stale"}}}],"Event":5,"Type":"Notification","Message":"{}"}`,
			reason: "invalid_sns_message",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tt := componenttest.NewTelemetry()
			t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
			mockS3 := new(mockS3ClientSQS)
			mockSQS := new(mockSQSClient)
			receiveOnce(mockSQS, types.Message{
				Body:          aws.String(tc.body),
				MessageId:     aws.String("message-id"),
				ReceiptHandle: aws.String("unreadable"),
			})
			expectDelete(mockSQS, "unreadable")

			runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), func(context.Context, string, []byte) error {
				t.Fatal("callback must not run for an unreadable message")
				return nil
			})

			mockSQS.AssertExpectations(t)
			mockS3.AssertNotCalled(t, "GetObject", mock.Anything, mock.Anything)
			metadatatest.AssertEqualReceiverAwss3SqsMessagesDropped(t, tt,
				[]metricdata.DataPoint[int64]{{Value: 1, Attributes: attribute.NewSet(attribute.String("reason", tc.reason))}},
				metricdatatest.IgnoreTimestamp())
		})
	}
}

func TestS3SQSReader_DoesNotCountFailedDelete(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
	mockSQS := new(mockSQSClient)
	receiveOnce(mockSQS, types.Message{Body: aws.String("plain text"), ReceiptHandle: aws.String("unreadable")})
	mockSQS.On("DeleteMessage", mock.Anything, mock.Anything).Return(nil, errors.New("access denied")).Once()

	runReader(t, newDropTestReader(t, tt, new(mockS3ClientSQS), mockSQS), nil)

	mockSQS.AssertExpectations(t)
	_, err := tt.GetMetric("otelcol_receiver_awss3_sqs_messages_dropped")
	assert.Error(t, err, "a message that stays in the queue must not count as dropped")
}

func TestS3SQSReader_DropsUndecodableObject(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
	mockS3 := new(mockS3ClientSQS)
	mockSQS := new(mockSQSClient)
	receiveOnce(mockSQS, objectCreatedMessage(t, "undecodable", "cwmetrics/old-format"))
	mockS3.On("GetObject", mock.Anything, &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("cwmetrics/old-format"),
	}).Return([]byte("not cloudwatch json"), nil)
	expectDelete(mockSQS, "undecodable")

	runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), func(context.Context, string, []byte) error {
		return &undecodableObjectError{reason: "decode_failed", err: errors.New("invalid character")}
	})

	mockSQS.AssertExpectations(t)
	metadatatest.AssertEqualReceiverAwss3ObjectsDropped(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1, Attributes: attribute.NewSet(attribute.String("reason", "decode_failed"))}},
		metricdatatest.IgnoreTimestamp())
}

func TestS3SQSReader_KeepsMessageOnConsumerError(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
	mockS3 := new(mockS3ClientSQS)
	mockSQS := new(mockSQSClient)
	receiveOnce(mockSQS, objectCreatedMessage(t, "retry", "cwmetrics/good"))
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return([]byte("{}"), nil)

	runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), func(context.Context, string, []byte) error {
		return errors.New("downstream exporter unavailable")
	})

	mockSQS.AssertNotCalled(t, "DeleteMessage", mock.Anything, mock.Anything)
	_, err := tt.GetMetric("otelcol_receiver_awss3_objects_dropped")
	assert.Error(t, err, "a retryable failure must not count as dropped")
}

func TestReceiveBytes_MarksUndecodableContent(t *testing.T) {
	rcvr := newTestMetricsReceiver(t, consumertest.NewNop())

	tests := []struct {
		name   string
		key    string
		data   []byte
		reason string
	}{
		{name: "corrupt gzip", key: "metrics.json.gz", data: []byte("not gzip"), reason: "decompress_failed"},
		{name: "corrupt zstd", key: "metrics.json.zst", data: []byte("not zstd"), reason: "decompress_failed"},
		{name: "invalid otlp json", key: "metrics.json", data: []byte("{not json"), reason: "decode_failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := rcvr.receiveBytes(t.Context(), tc.key, tc.data)
			var undecodable *undecodableObjectError
			require.ErrorAs(t, err, &undecodable)
			assert.Equal(t, tc.reason, undecodable.reason)
		})
	}

	t.Run("consumer error stays retryable", func(t *testing.T) {
		rcvr := newTestMetricsReceiver(t, consumertest.NewErr(errors.New("downstream exporter unavailable")))
		err := rcvr.receiveBytes(t.Context(), "metrics.json", []byte(`{"resourceMetrics":[]}`))
		require.Error(t, err)
		var undecodable *undecodableObjectError
		assert.NotErrorAs(t, err, &undecodable)
	})
}

func TestS3SQSReader_CountsDroppedObjectsOnlyAfterDelete(t *testing.T) {
	twoObjects, err := json.Marshal(s3EventNotification{Records: []s3EventRecord{
		{EventSource: "aws:s3", EventName: "ObjectCreated:Put", S3: s3Data{Bucket: s3BucketData{Name: "test-bucket"}, Object: s3ObjectData{Key: "cwmetrics/old-format"}}},
		{EventSource: "aws:s3", EventName: "ObjectCreated:Put", S3: s3Data{Bucket: s3BucketData{Name: "test-bucket"}, Object: s3ObjectData{Key: "cwmetrics/good"}}},
	}})
	require.NoError(t, err)
	decodeFails := func(_ context.Context, key string, _ []byte) error {
		if key == "cwmetrics/old-format" {
			return &undecodableObjectError{reason: "decode_failed", err: errors.New("invalid character")}
		}
		return nil
	}

	t.Run("delete fails", func(t *testing.T) {
		tt := componenttest.NewTelemetry()
		t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
		mockS3 := new(mockS3ClientSQS)
		mockSQS := new(mockSQSClient)
		receiveOnce(mockSQS, objectCreatedMessage(t, "undecodable", "cwmetrics/old-format"))
		mockS3.On("GetObject", mock.Anything, mock.Anything).Return([]byte("x"), nil)
		mockSQS.On("DeleteMessage", mock.Anything, mock.Anything).Return(nil, errors.New("access denied")).Once()

		runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), decodeFails)

		mockSQS.AssertExpectations(t)
		_, err := tt.GetMetric("otelcol_receiver_awss3_objects_dropped")
		assert.Error(t, err, "an object whose message stays in the queue must not count as dropped")
	})

	t.Run("another record needs a retry", func(t *testing.T) {
		tt := componenttest.NewTelemetry()
		t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
		mockS3 := new(mockS3ClientSQS)
		mockSQS := new(mockSQSClient)
		receiveOnce(mockSQS, types.Message{Body: aws.String(string(twoObjects)), ReceiptHandle: aws.String("mixed")})
		mockS3.On("GetObject", mock.Anything, mock.Anything).Return([]byte("x"), nil)

		runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), func(ctx context.Context, key string, content []byte) error {
			if key == "cwmetrics/good" {
				return errors.New("downstream exporter unavailable")
			}
			return decodeFails(ctx, key, content)
		})

		mockSQS.AssertNotCalled(t, "DeleteMessage", mock.Anything, mock.Anything)
		_, err := tt.GetMetric("otelcol_receiver_awss3_objects_dropped")
		assert.Error(t, err, "an object whose message is retried must not count as dropped")
	})
}

func TestS3SQSReader_DropsUnsupportedFormatObject(t *testing.T) {
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) }) //nolint:usetesting
	mockS3 := new(mockS3ClientSQS)
	mockSQS := new(mockSQSClient)
	receiveOnce(mockSQS, objectCreatedMessage(t, "unsupported", "logs/app.unknown"))
	mockS3.On("GetObject", mock.Anything, mock.Anything).Return([]byte("anything"), nil)
	expectDelete(mockSQS, "unsupported")

	rcvr := newTestMetricsReceiver(t, consumertest.NewNop())
	runReader(t, newDropTestReader(t, tt, mockS3, mockSQS), rcvr.receiveBytes)

	mockSQS.AssertExpectations(t)
	metadatatest.AssertEqualReceiverAwss3ObjectsDropped(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1, Attributes: attribute.NewSet(attribute.String("reason", "unsupported_format"))}},
		metricdatatest.IgnoreTimestamp())
}
