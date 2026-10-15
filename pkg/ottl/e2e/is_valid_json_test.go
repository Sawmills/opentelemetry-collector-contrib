// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

func TestIsValidJSONGuardPreservesMalformedBodyWithoutExecutionError(t *testing.T) {
	body := `{not json}`

	record, err := executeGuardedJSON(t.Context(), body)

	require.NoError(t, err)
	require.Equal(t, body, record.Body().Str())
}

func TestIsValidJSONGuardPreservesValidParsedFields(t *testing.T) {
	body := `{"message":"synthetic","nested":{"enabled":true}}`
	want := map[string]any{"message": "synthetic", "nested": map[string]any{"enabled": true}}

	record, err := executeGuardedJSON(t.Context(), body)

	require.NoError(t, err)
	require.Equal(t, want, record.Body().AsRaw())
}

func TestIsValidJSONGuardPreservesExtraClosingBraceWithoutExecutionError(t *testing.T) {
	body := `{"message":"synthetic"}}`

	record, err := executeGuardedJSON(t.Context(), body)

	require.NoError(t, err)
	require.Equal(t, body, record.Body().Str())
}

func TestIsValidJSONGuardPreservesExtraClosingBracketWithoutExecutionError(t *testing.T) {
	body := `{"message":"synthetic"}]`

	record, err := executeGuardedJSON(t.Context(), body)

	require.NoError(t, err)
	require.Equal(t, body, record.Body().Str())
}

func TestTryParseJSONPreservesMalformedBodyWithoutExecutionError(t *testing.T) {
	body := `{not json}`
	record, err := executeObjectTryParseJSON(t.Context(), body)
	require.NoError(t, err)
	require.Equal(t, body, record.Body().Str())
}

func TestTryParseJSONPreservesValidParsedFields(t *testing.T) {
	record, err := executeObjectTryParseJSON(t.Context(), `{"message":"synthetic","nested":{"enabled":true}}`)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"message": "synthetic", "nested": map[string]any{"enabled": true}}, record.Body().AsRaw())
}

func TestTryParseJSONObjectStatementPreservesRejectedAndNonObjectBodies(t *testing.T) {
	for _, body := range []string{`{"message":"synthetic"}}`, `{"message":"synthetic"}]`, `{"number":1e10000}`, `{"message":`, `plain text`, `null`, `[1,2]`} {
		record, err := executeObjectTryParseJSON(t.Context(), body)
		require.NoError(t, err)
		require.Equal(t, body, record.Body().Str())
	}
}

func TestTryParseJSONParsesValidContainersWithoutShapeGuard(t *testing.T) {
	for _, sample := range []struct {
		body string
		want any
	}{
		{" \n{\"message\":\"synthetic\"}", map[string]any{"message": "synthetic"}},
		{` ["synthetic",{"enabled":true}]`, []any{"synthetic", map[string]any{"enabled": true}}},
	} {
		record, err := executeJSONStatement(t.Context(), sample.body, `set(body, TryParseJSON(body))`)
		require.NoError(t, err)
		require.Equal(t, sample.want, record.Body().AsRaw())
	}
}

func executeGuardedJSON(ctx context.Context, body string) (plog.LogRecord, error) {
	return executeJSONStatement(ctx, body, `set(body, ParseJSON(body)) where IsValidJSON(body) and IsMatch(body, "^\\{")`)
}

func executeObjectTryParseJSON(ctx context.Context, body string) (plog.LogRecord, error) {
	return executeJSONStatement(ctx, body, `set(body, TryParseJSON(body)) where IsMatch(body, "^\\{")`)
}

func executeJSONStatement(ctx context.Context, body, statement string) (plog.LogRecord, error) {
	settings := componenttest.NewNopTelemetrySettings()
	parser, err := ottllog.NewParser(ottlfuncs.StandardFuncs[*ottllog.TransformContext](), settings)
	if err != nil {
		return plog.LogRecord{}, err
	}
	statements, err := parser.ParseStatements([]string{statement})
	if err != nil {
		return plog.LogRecord{}, err
	}
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	record := scopeLogs.LogRecords().AppendEmpty()
	record.Body().SetStr(body)
	transformContext := ottllog.NewTransformContextPtr(resourceLogs, scopeLogs, record)
	defer transformContext.Close()
	sequence := ottl.NewStatementSequence(statements, settings)
	return record, sequence.Execute(ctx, transformContext)
}
