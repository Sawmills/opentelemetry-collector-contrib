// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package common // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor/internal/common"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/expr"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor/internal/metadata"
)

type LogsConsumer interface {
	Context() ContextID
	ConsumeLogs(ctx context.Context, ld plog.Logs, cache *pcommon.Map) error
}

// metricsTrackingStatementSequence wraps ottl.StatementSequence to track individual statement execution results
type metricsTrackingStatementSequence[K any] struct {
	statements        []*ottl.Statement[K]
	errorMode         ottl.ErrorMode
	telemetrySettings component.TelemetrySettings
	telemetryBuilder  *metadata.TelemetryBuilder
	shouldTrack       bool
}

// Execute executes all statements and tracks metrics for each individual statement execution
func (s *metricsTrackingStatementSequence[K]) Execute(ctx context.Context, tCtx K) error {
	for _, statement := range s.statements {
		_, _, err := statement.Execute(ctx, tCtx)
		if err != nil {
			// Track failed transformation if metrics tracking is enabled
			if s.shouldTrack {
				s.telemetryBuilder.ProcessorTransformLogsFailed.Add(ctx, 1)
			}

			// Handle the error according to the error mode (similar to OTTL's internal logic)
			if s.errorMode == ottl.PropagateError {
				return fmt.Errorf("failed to execute statement: %w", err)
			}
			if s.errorMode == ottl.IgnoreError {
				// Log the error and continue, just like OTTL does internally
				s.telemetrySettings.Logger.Warn("failed to execute statement", zap.Error(err))
			}
			// For SilentError mode, we just continue without logging
		} else {
			// Track successful transformation if metrics tracking is enabled
			if s.shouldTrack {
				s.telemetryBuilder.ProcessorTransformLogsTransformed.Add(ctx, 1)
			}
		}
	}
	return nil
}

type logStatements struct {
	sequence metricsTrackingStatementSequence[ottllog.TransformContext]
	expr.BoolExpr[ottllog.TransformContext]
}

func (l logStatements) Context() ContextID {
	return Log
}

func (l logStatements) ConsumeLogs(ctx context.Context, ld plog.Logs, cache *pcommon.Map) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rlogs := ld.ResourceLogs().At(i)
		for j := 0; j < rlogs.ScopeLogs().Len(); j++ {
			slogs := rlogs.ScopeLogs().At(j)
			logs := slogs.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				tCtx := ottllog.NewTransformContext(logs.At(k), slogs.Scope(), rlogs.Resource(), slogs, rlogs, ottllog.WithCache(cache))
				condition, err := l.Eval(ctx, tCtx)
				if err != nil {
					return err
				}
				if condition {
					err := l.sequence.Execute(ctx, tCtx)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

type LogParserCollection ottl.ParserCollection[LogsConsumer]

type LogParserCollectionOption ottl.ParserCollectionOption[LogsConsumer]

// logTelemetryConfig holds telemetry configuration for log statement conversion
type logTelemetryConfig struct {
	telemetryBuilder *metadata.TelemetryBuilder
	errorMode        ottl.ErrorMode
}

func WithLogParser(functions map[string]ottl.Factory[ottllog.TransformContext], telemetryBuilder *metadata.TelemetryBuilder, errorMode ottl.ErrorMode) LogParserCollectionOption {
	return func(pc *ottl.ParserCollection[LogsConsumer]) error {
		logParser, err := ottllog.NewParser(functions, pc.Settings, ottllog.EnablePathContextNames())
		if err != nil {
			return err
		}

		// Create statement converter with telemetry configuration
		config := logTelemetryConfig{
			telemetryBuilder: telemetryBuilder,
			errorMode:        errorMode,
		}

		converter := func(pc *ottl.ParserCollection[LogsConsumer], statements ottl.StatementsGetter, parsedStatements []*ottl.Statement[ottllog.TransformContext]) (LogsConsumer, error) {
			return convertLogStatements(pc, statements, parsedStatements, config)
		}

		return ottl.WithParserCollectionContext(ottllog.ContextName, &logParser, ottl.WithStatementConverter(converter))(pc)
	}
}

func convertLogStatements(pc *ottl.ParserCollection[LogsConsumer], statements ottl.StatementsGetter, parsedStatements []*ottl.Statement[ottllog.TransformContext], telemetryConfig logTelemetryConfig) (LogsConsumer, error) {
	contextStatements, err := toContextStatements(statements)
	if err != nil {
		return nil, err
	}
	errorMode := pc.ErrorMode
	if contextStatements.ErrorMode != "" {
		errorMode = contextStatements.ErrorMode
	}
	// Use the provided default error mode if pc.ErrorMode is not set
	if errorMode == "" {
		errorMode = telemetryConfig.errorMode
	}
	var parserOptions []ottl.Option[ottllog.TransformContext]
	if contextStatements.Context == "" {
		parserOptions = append(parserOptions, ottllog.EnablePathContextNames())
	}
	globalExpr, errGlobalBoolExpr := parseGlobalExpr(filterottl.NewBoolExprForLogWithOptions, contextStatements.Conditions, errorMode, pc.Settings, filterottl.StandardLogFuncs(), parserOptions)
	if errGlobalBoolExpr != nil {
		return nil, errGlobalBoolExpr
	}

	// Determine if we should track metrics (only for IgnoreError/SilentError modes)
	shouldTrack := telemetryConfig.telemetryBuilder != nil && (errorMode == ottl.IgnoreError || errorMode == ottl.SilentError)

	// Create custom statement sequence with metrics tracking
	sequence := metricsTrackingStatementSequence[ottllog.TransformContext]{
		statements:        parsedStatements,
		errorMode:         errorMode,
		telemetrySettings: pc.Settings,
		telemetryBuilder:  telemetryConfig.telemetryBuilder,
		shouldTrack:       shouldTrack,
	}

	return logStatements{sequence, globalExpr}, nil
}

func WithLogErrorMode(errorMode ottl.ErrorMode) LogParserCollectionOption {
	return LogParserCollectionOption(ottl.WithParserCollectionErrorMode[LogsConsumer](errorMode))
}

func NewLogParserCollection(settings component.TelemetrySettings, options ...LogParserCollectionOption) (*LogParserCollection, error) {
	pcOptions := []ottl.ParserCollectionOption[LogsConsumer]{
		withCommonContextParsers[LogsConsumer](),
		ottl.EnableParserCollectionModifiedPathsLogging[LogsConsumer](true),
	}

	for _, option := range options {
		pcOptions = append(pcOptions, ottl.ParserCollectionOption[LogsConsumer](option))
	}

	pc, err := ottl.NewParserCollection(settings, pcOptions...)
	if err != nil {
		return nil, err
	}

	lpc := LogParserCollection(*pc)
	return &lpc, nil
}

func (lpc *LogParserCollection) ParseContextStatements(contextStatements ContextStatements) (LogsConsumer, error) {
	pc := ottl.ParserCollection[LogsConsumer](*lpc)
	if contextStatements.Context != "" {
		return pc.ParseStatementsWithContext(string(contextStatements.Context), contextStatements, true)
	}
	return pc.ParseStatements(contextStatements)
}
