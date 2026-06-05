// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsfirehosereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/cwlog"
)

const defaultLogsEncoding = cwlog.TypeStr

// logsConsumer implements the firehoseConsumer
// to use a logs consumer and unmarshaler.
type logsConsumer struct {
	config                 *Config
	settings               receiver.Settings
	commonAttributesLogger *zap.Logger

	// consumer passes the translated logs on to the
	// next consumer.
	consumer consumer.Logs
	// unmarshaler is the configured plog.Unmarshaler
	// to use when processing the records.
	unmarshaler plog.Unmarshaler
}

var _ firehoseConsumer = (*logsConsumer)(nil)

// newLogsReceiver creates a new instance of the receiver
// with a logsConsumer.
func newLogsReceiver(
	config *Config,
	set receiver.Settings,
	nextConsumer consumer.Logs,
) (receiver.Logs, error) {
	c := &logsConsumer{
		config:                 config,
		settings:               set,
		commonAttributesLogger: newCommonAttributesLogger(set.Logger),
		consumer:               nextConsumer,
	}
	return &firehoseReceiver{
		settings: set,
		config:   config,
		consumer: c,
	}, nil
}

// Start sets the consumer's log unmarshaler to either a built-in
// unmarshaler or one loaded from an encoding extension.
func (c *logsConsumer) Start(_ context.Context, host component.Host) error {
	encoding := c.config.Encoding
	if encoding == "" {
		encoding = c.config.RecordType
		if encoding == "" {
			encoding = defaultLogsEncoding
		}
	}
	if encoding == cwlog.TypeStr {
		c.settings.Logger.Warn(
			"The built-in \"cwlogs\" encoding is deprecated and will be removed in a future version. " +
				"Use the \"aws_logs_encoding\" encoding extension with format \"cloudwatch\" instead.",
		)
		c.unmarshaler = cwlog.NewUnmarshaler(c.settings.Logger, c.settings.BuildInfo)
	} else {
		unmarshaler, err := loadEncodingExtension[plog.Unmarshaler](host, encoding, "logs")
		if err != nil {
			return fmt.Errorf("failed to load encoding extension: %w", err)
		}
		c.unmarshaler = unmarshaler
	}
	return nil
}

// Consume uses the configured unmarshaler to deserialize each record,
// with each resulting plog.Logs being sent to the next consumer as
// they are unmarshalled.
func (c *logsConsumer) Consume(ctx context.Context, nextRecord nextRecordFunc, commonAttributes map[string]string) (int, error) {
	warnOnNonMapTarget := true
	for {
		record, err := nextRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nextRecordErrorStatus(err), err
		}
		logs, err := c.unmarshaler.UnmarshalLogs(record)
		if err != nil {
			return http.StatusBadRequest, err
		}

		if commonAttributes != nil {
			for i := 0; i < logs.ResourceLogs().Len(); i++ {
				rm := logs.ResourceLogs().At(i)
				if attachCommonAttributes(rm.Resource().Attributes(), commonAttributes, commonAttributesConfig(c.config), c.commonAttributesLogger, warnOnNonMapTarget) {
					warnOnNonMapTarget = false
				}
			}
		}

		if err := c.consumer.ConsumeLogs(ctx, logs); err != nil {
			if consumererror.IsPermanent(err) {
				return http.StatusBadRequest, err
			}
			return http.StatusServiceUnavailable, err
		}
	}
	return http.StatusOK, nil
}
