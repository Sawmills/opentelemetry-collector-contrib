// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package googlecloudpubsubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudpubsubreceiver"

import (
	"fmt"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var subscriptionMatcher = regexp.MustCompile(`projects/[a-z][a-z0-9\-]*(:[a-z0-9\-]+)?/subscriptions/`)

const (
	// defaultAckDeadlineSeconds is the StreamingPull ack deadline requested per stream. Pub/Sub
	// allows 10-600s; the receiver defaults to the maximum so acknowledgements are not missed under
	// high volume, which would otherwise trigger redelivery and Pub/Sub delivery throttling.
	defaultAckDeadlineSeconds int32 = 600
	minAckDeadlineSeconds     int32 = 10
	maxAckDeadlineSeconds     int32 = 600
	// defaultAckBatchWait is how long the acknowledge loop batches acks before flushing them.
	defaultAckBatchWait = 5 * time.Second
)

type Config struct {
	// Google Cloud Project ID where the Pubsub client will connect to
	ProjectID string `mapstructure:"project"`
	// User agent that will be used by the Pubsub client to connect to the service
	UserAgent string `mapstructure:"user_agent"`
	// Override of the Pubsub Endpoint, leave empty for the default endpoint
	Endpoint string `mapstructure:"endpoint"`
	// Only has effect if Endpoint is not ""
	Insecure bool `mapstructure:"insecure"`
	// Timeout for all API calls. If not set, defaults to 12 seconds.
	TimeoutSettings exporterhelper.TimeoutConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.

	// The fully qualified resource name of the Pubsub subscription
	Subscription string `mapstructure:"subscription"`
	// Lock down the encoding of the payload, leave empty for attribute based detection
	Encoding string `mapstructure:"encoding"`
	// Lock down the compression of the payload, leave empty for attribute based detection
	Compression string `mapstructure:"compression"`

	// Ignore errors when the configured encoder fails to decoding a PubSub messages
	IgnoreEncodingError bool `mapstructure:"ignore_encoding_error"`

	// The client id that will be used by Pubsub to make load balancing decisions
	ClientID string `mapstructure:"client_id"`

	// AckDeadlineSeconds is the per-stream StreamingPull ack deadline requested from Pub/Sub, in
	// the range 10-600. A high deadline prevents messages from expiring before the collector can
	// acknowledge them under high volume; expiry causes redelivery and Pub/Sub throttles delivery
	// to the subscription. Defaults to 600.
	AckDeadlineSeconds int32 `mapstructure:"ack_deadline_seconds"`
	// AckBatchWait is how long the acknowledge loop batches acks before flushing them to Pub/Sub.
	// Keep it well below AckDeadlineSeconds. Defaults to 5s.
	AckBatchWait time.Duration `mapstructure:"ack_batch_wait"`
}

// resolvedAckDeadlineSeconds returns the configured ack deadline, falling back to the default when
// unset (zero), so a zero-valued Config (e.g. constructed directly) still behaves sensibly.
func (config *Config) resolvedAckDeadlineSeconds() int32 {
	if config.AckDeadlineSeconds == 0 {
		return defaultAckDeadlineSeconds
	}
	return config.AckDeadlineSeconds
}

// resolvedAckBatchWait returns the configured ack batch wait, falling back to the default when unset.
func (config *Config) resolvedAckBatchWait() time.Duration {
	if config.AckBatchWait <= 0 {
		return defaultAckBatchWait
	}
	return config.AckBatchWait
}

func (config *Config) validate() error {
	if !subscriptionMatcher.MatchString(config.Subscription) {
		return fmt.Errorf("subscription '%s' is not a valid format, use 'projects/<project_id>/subscriptions/<name>'", config.Subscription)
	}
	switch config.Compression {
	case "":
	case "gzip":
	default:
		return fmt.Errorf("compression %v is not supported.  supported compression formats include [gzip]", config.Compression)
	}
	if config.AckDeadlineSeconds != 0 && (config.AckDeadlineSeconds < minAckDeadlineSeconds || config.AckDeadlineSeconds > maxAckDeadlineSeconds) {
		return fmt.Errorf("ack_deadline_seconds %d is out of range, must be between %d and %d", config.AckDeadlineSeconds, minAckDeadlineSeconds, maxAckDeadlineSeconds)
	}
	if config.AckBatchWait < 0 {
		return fmt.Errorf("ack_batch_wait %s must not be negative", config.AckBatchWait)
	}
	return nil
}
