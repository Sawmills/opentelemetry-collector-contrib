// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestShadowMode_NoDivergence(t *testing.T) {
	ResetShadowMetrics()

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) { return nil, nil },
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: logger},
		WithVMEnabled[any](),
		WithShadowMode[any](),
	)
	require.NoError(t, err)

	cond, err := p.ParseCondition("1 + 1 == 2")
	require.NoError(t, err)

	result, err := cond.Eval(context.Background(), nil)
	require.NoError(t, err)
	assert.True(t, result)

	assert.Equal(t, uint64(0), ShadowDivergenceTotal())
	assert.Equal(t, 0, logs.Len())
}

func TestShadowMode_BooleanLogic(t *testing.T) {
	ResetShadowMetrics()

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) { return nil, nil },
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: logger},
		WithVMEnabled[any](),
		WithShadowMode[any](),
	)
	require.NoError(t, err)

	testCases := []struct {
		condition string
		expected  bool
	}{
		{"true and true", true},
		{"true and false", false},
		{"true or false", true},
		{"false or false", false},
		{"not true", false},
		{"not false", true},
		{"1 < 2", true},
		{"2 > 1", true},
		{"1 == 1", true},
		{"1 != 2", true},
	}

	for _, tc := range testCases {
		t.Run(tc.condition, func(t *testing.T) {
			cond, err := p.ParseCondition(tc.condition)
			require.NoError(t, err)

			result, err := cond.Eval(context.Background(), nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}

	assert.Equal(t, uint64(0), ShadowDivergenceTotal())
	assert.Equal(t, 0, logs.Len())
}

func TestShadowMode_Arithmetic(t *testing.T) {
	ResetShadowMetrics()

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) { return nil, nil },
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: logger},
		WithVMEnabled[any](),
		WithShadowMode[any](),
	)
	require.NoError(t, err)

	testCases := []struct {
		condition string
		expected  bool
	}{
		{"1 + 2 * 3 == 7", true},
		{"(1 + 2) * 3 == 9", true},
		{"10 - 5 == 5", true},
		{"6 / 2 == 3", true},
		{"1.5 * 2.0 == 3.0", true},
	}

	for _, tc := range testCases {
		t.Run(tc.condition, func(t *testing.T) {
			cond, err := p.ParseCondition(tc.condition)
			require.NoError(t, err)

			result, err := cond.Eval(context.Background(), nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}

	assert.Equal(t, uint64(0), ShadowDivergenceTotal())
	assert.Equal(t, 0, logs.Len())
}

func TestShadowMode_StringComparison(t *testing.T) {
	ResetShadowMetrics()

	core, logs := observer.New(zap.WarnLevel)
	logger := zap.New(core)

	p, err := NewParser[any](
		map[string]Factory[any]{},
		func(Path[any]) (GetSetter[any], error) {
			return StandardGetSetter[any]{
				Getter: func(context.Context, any) (any, error) { return nil, nil },
				Setter: func(context.Context, any, any) error { return nil },
			}, nil
		},
		component.TelemetrySettings{Logger: logger},
		WithVMEnabled[any](),
		WithShadowMode[any](),
	)
	require.NoError(t, err)

	testCases := []struct {
		condition string
		expected  bool
	}{
		{`"hello" == "hello"`, true},
		{`"hello" != "world"`, true},
		{`"abc" < "abd"`, true},
		{`"z" > "a"`, true},
	}

	for _, tc := range testCases {
		t.Run(tc.condition, func(t *testing.T) {
			cond, err := p.ParseCondition(tc.condition)
			require.NoError(t, err)

			result, err := cond.Eval(context.Background(), nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}

	assert.Equal(t, uint64(0), ShadowDivergenceTotal())
	assert.Equal(t, 0, logs.Len())
}

func TestShadowMetrics_Reset(t *testing.T) {
	globalShadowMetrics.divergenceTotal.Store(42)
	assert.Equal(t, uint64(42), ShadowDivergenceTotal())

	ResetShadowMetrics()
	assert.Equal(t, uint64(0), ShadowDivergenceTotal())
}

func TestErrorsEquivalent(t *testing.T) {
	assert.True(t, errorsEquivalent(nil, nil))

	assert.True(t, errorsEquivalent(
		nil,
		wrapError(context.DeadlineExceeded),
	) == false)
}

func wrapError(err error) error {
	return err
}
