// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/vm"
	"go.uber.org/zap"
)

type shadowMetrics struct {
	divergenceTotal atomic.Uint64
}

var globalShadowMetrics = &shadowMetrics{}

// ShadowDivergenceTotal returns the total number of divergences detected in shadow mode.
func ShadowDivergenceTotal() uint64 {
	return globalShadowMetrics.divergenceTotal.Load()
}

// ResetShadowMetrics resets the shadow mode metrics. Primarily for testing.
func ResetShadowMetrics() {
	globalShadowMetrics.divergenceTotal.Store(0)
}

type shadowBoolExpr[K any] struct {
	interpreter BoolExpr[K]
	vmExpr      BoolExpr[K]
	origText    string
	logger      *zap.Logger
}

func (s *shadowBoolExpr[K]) Eval(ctx context.Context, tCtx K) (bool, error) {
	interpResult, interpErr := s.interpreter.Eval(ctx, tCtx)
	vmResult, vmErr := s.vmExpr.Eval(ctx, tCtx)

	s.compareAndLog(interpResult, interpErr, vmResult, vmErr)

	return interpResult, interpErr
}

func (s *shadowBoolExpr[K]) compareAndLog(interpResult bool, interpErr error, vmResult bool, vmErr error) {
	if !errorsEquivalent(interpErr, vmErr) {
		globalShadowMetrics.divergenceTotal.Add(1)
		s.logger.Warn("OTTL shadow mode: error divergence",
			zap.String("statement", s.origText),
			zap.Error(interpErr),
			zap.NamedError("vm_error", vmErr),
		)
		return
	}

	if interpErr != nil {
		return
	}

	if interpResult != vmResult {
		globalShadowMetrics.divergenceTotal.Add(1)
		s.logger.Warn("OTTL shadow mode: result divergence",
			zap.String("statement", s.origText),
			zap.Bool("interpreter_result", interpResult),
			zap.Bool("vm_result", vmResult),
		)
	}
}

type shadowExpr[K any] struct {
	interpreter Expr[K]
	vmExpr      Expr[K]
	origText    string
	logger      *zap.Logger
}

func (s *shadowExpr[K]) Eval(ctx context.Context, tCtx K) (any, error) {
	interpResult, interpErr := s.interpreter.Eval(ctx, tCtx)
	vmResult, vmErr := s.vmExpr.Eval(ctx, tCtx)

	s.compareAndLog(interpResult, interpErr, vmResult, vmErr)

	return interpResult, interpErr
}

func (s *shadowExpr[K]) compareAndLog(interpResult any, interpErr error, vmResult any, vmErr error) {
	if !errorsEquivalent(interpErr, vmErr) {
		globalShadowMetrics.divergenceTotal.Add(1)
		s.logger.Warn("OTTL shadow mode: error divergence",
			zap.String("statement", s.origText),
			zap.Error(interpErr),
			zap.NamedError("vm_error", vmErr),
		)
		return
	}

	if interpErr != nil {
		return
	}

	if !reflect.DeepEqual(interpResult, vmResult) {
		globalShadowMetrics.divergenceTotal.Add(1)
		s.logger.Warn("OTTL shadow mode: result divergence",
			zap.String("statement", s.origText),
			zap.Any("interpreter_result", interpResult),
			zap.Any("vm_result", vmResult),
		)
	}
}

// errorsEquivalent checks if two errors are equivalent for shadow mode comparison.
// ErrGasExhausted from VM is treated as acceptable when interpreter succeeds.
func errorsEquivalent(interpErr, vmErr error) bool {
	if interpErr == nil && vmErr == nil {
		return true
	}

	if interpErr == nil && errors.Is(vmErr, vm.ErrGasExhausted) {
		return true
	}

	if interpErr != nil && vmErr != nil {
		return errorsHaveSameType(interpErr, vmErr)
	}

	return false
}

func errorsHaveSameType(a, b error) bool {
	aType := reflect.TypeOf(errors.Unwrap(a))
	bType := reflect.TypeOf(errors.Unwrap(b))

	if aType == nil {
		aType = reflect.TypeOf(a)
	}
	if bType == nil {
		bType = reflect.TypeOf(b)
	}

	return aType == bType
}

func wrapWithShadow[K any](interpreter, vmExpr BoolExpr[K], origText string, logger *zap.Logger) BoolExpr[K] {
	shadow := &shadowBoolExpr[K]{
		interpreter: interpreter,
		vmExpr:      vmExpr,
		origText:    origText,
		logger:      logger,
	}
	return BoolExpr[K]{shadow.Eval}
}

func wrapWithShadowExpr[K any](interpreter, vmExpr Expr[K], origText string, logger *zap.Logger) Expr[K] {
	shadow := &shadowExpr[K]{
		interpreter: interpreter,
		vmExpr:      vmExpr,
		origText:    origText,
		logger:      logger,
	}
	return Expr[K]{exprFunc: shadow.Eval}
}
