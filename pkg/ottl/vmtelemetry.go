// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"errors"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type vmTelemetry struct {
	execCount metric.Int64Counter
	errCount  metric.Int64Counter
	execTime  metric.Float64Histogram
	divCount  metric.Int64Counter
	logger    *zap.Logger
}

var (
	errTypeKey          = attribute.Key("type")
	errTypeGasExhausted = errTypeKey.String("gas_exhausted")
	errTypeStack        = errTypeKey.String("stack")
	errTypeType         = errTypeKey.String("type")
	errTypeOther        = errTypeKey.String("other")
)

func newVMTelemetry(mp metric.MeterProvider, logger *zap.Logger) *vmTelemetry {
	if mp == nil {
		return nil
	}
	if _, ok := mp.(noop.MeterProvider); ok {
		return nil
	}
	meter := mp.Meter("github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm")

	execCount, err := meter.Int64Counter("ottl_vm_execution_count")
	if err != nil {
		logger.Debug("OTTL VM telemetry disabled: exec counter", zap.Error(err))
		return nil
	}
	errCount, err := meter.Int64Counter("ottl_vm_error_count")
	if err != nil {
		logger.Debug("OTTL VM telemetry disabled: error counter", zap.Error(err))
		return nil
	}
	execTime, err := meter.Float64Histogram("ottl_vm_execution_time", metric.WithUnit("ns"))
	if err != nil {
		logger.Debug("OTTL VM telemetry disabled: latency histogram", zap.Error(err))
		return nil
	}
	divCount, err := meter.Int64Counter("ottl_vm_shadow_divergence_total")
	if err != nil {
		logger.Debug("OTTL VM telemetry disabled: divergence counter", zap.Error(err))
		return nil
	}

	return &vmTelemetry{
		execCount: execCount,
		errCount:  errCount,
		execTime:  execTime,
		divCount:  divCount,
		logger:    logger,
	}
}

func (m *vmTelemetry) record(ctx context.Context, dur time.Duration, err error) {
	if m == nil {
		return
	}
	m.execCount.Add(ctx, 1)
	m.execTime.Record(ctx, float64(dur.Nanoseconds()))
	if err != nil {
		m.errCount.Add(ctx, 1, metric.WithAttributes(vmErrorAttr(err)))
	}
}

func vmErrorAttr(err error) attribute.KeyValue {
	switch {
	case errors.Is(err, vm.ErrGasExhausted):
		return errTypeGasExhausted
	case errors.Is(err, vm.ErrStackOverflow), errors.Is(err, vm.ErrStackUnderflow):
		return errTypeStack
	case errors.Is(err, vm.ErrTypeMismatch):
		return errTypeType
	default:
		return errTypeOther
	}
}

func (m *vmTelemetry) recordDivergence(ctx context.Context, err bool) {
	if m == nil {
		return
	}
	if err {
		m.divCount.Add(ctx, 1, metric.WithAttributes(attribute.String("type", "error")))
		return
	}
	m.divCount.Add(ctx, 1, metric.WithAttributes(attribute.String("type", "result")))
}
