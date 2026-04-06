# SAW-6662 Track A Snowflake Schema Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a backward-compatible `schema: snowflake` mode to `parquet_log_encoding`, keep `awss3exporter` generic, and define the Track A Snowflake storage/view contract for the bakeoff.

**Architecture:** Extend the existing parquet encoding extension with a schema selector and a new Snowflake adapter while preserving the current Datadog path as the default. Keep raw parquet storage explicit and logs-only, then add Snowflake SQL artifacts that turn raw JSON text columns into the supported public secure view.

**Tech Stack:** Go, OpenTelemetry Collector contrib, `xitongsys/parquet-go`, Snowflake SQL, OCB builder wiring, Go test, Make.

---

### Task 1: Add schema config without breaking existing configs

**Files:**

- Modify: `extension/parquetlogencodingextension/config.go`
- Modify: `extension/parquetlogencodingextension/config_test.go`
- Test: `extension/parquetlogencodingextension/config_test.go`

**Step 1: Write the failing config tests**

Add tests for:

- omitted `schema` defaults to `datadog`
- `schema: snowflake` validates
- invalid schema fails

```go
func TestConfigValidateSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		shouldError bool
	}{
		{name: "default_datadog", schema: "datadog"},
		{name: "snowflake", schema: "snowflake"},
		{name: "invalid", schema: "custom", shouldError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := CreateDefaultConfig().(*Config)
			cfg.Schema = tt.schema
			err := cfg.Validate()
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run TestConfigValidateSchema
```

Expected: FAIL because `Config` has no `Schema` field and validation does not recognize it.

**Step 3: Write the minimal implementation**

Add:

- `Schema string \`mapstructure:"schema"\``
- default constant `defaultSchema = "datadog"`
- validation allowing only `datadog` and `snowflake`

```go
const defaultSchema = "datadog"

type Config struct {
	MaxFileSizeBytes   int64  `mapstructure:"max_file_size_bytes"`
	NumberOfGoRoutines int64  `mapstructure:"number_of_go_routines"`
	RowGroupSizeBytes  int64  `mapstructure:"row_group_size_bytes"`
	PageSizeBytes      int64  `mapstructure:"page_size_bytes"`
	CompressionCodec   string `mapstructure:"compression_codec"`
	Schema             string `mapstructure:"schema"`
}
```

**Step 4: Run tests to verify they pass**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run 'TestConfigValidateSchema|TestConfigValidateCompressionCodec|TestConfigValidateRowGroupSizeWithinMaxFileSize'
```

Expected: PASS

**Step 5: Commit**

```bash
git add extension/parquetlogencodingextension/config.go extension/parquetlogencodingextension/config_test.go
git commit -m "feat: add parquet schema selection config"
```

### Task 2: Add adapter selection while keeping Datadog as default

**Files:**

- Modify: `extension/parquetlogencodingextension/extension.go`
- Modify: `extension/parquetlogencodingextension/extension_test.go`
- Test: `extension/parquetlogencodingextension/extension_test.go`

**Step 1: Write the failing selection tests**

Add tests that:

- omitted schema still uses Datadog behavior
- `schema: snowflake` selects a different adapter path

```go
func TestNewParquetLogExtension_DefaultSchemaUsesDatadog(t *testing.T) {
	ext := newTestParquetExtension(t, &Config{
		MaxFileSizeBytes:   defaultMaxFileSizeBytes,
		NumberOfGoRoutines: defaultNumberOfGoRoutines,
	})
	require.IsType(t, &datadog.ParquetLog{}, ext.adapter.Schema())
}
```

```go
func TestNewParquetLogExtension_SnowflakeSchemaUsesSnowflakeAdapter(t *testing.T) {
	ext := newTestParquetExtension(t, &Config{
		MaxFileSizeBytes:   defaultMaxFileSizeBytes,
		NumberOfGoRoutines: defaultNumberOfGoRoutines,
		Schema:             "snowflake",
	})
	require.IsType(t, &snowflake.ParquetLog{}, ext.adapter.Schema())
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run 'TestNewParquetLogExtension_DefaultSchemaUsesDatadog|TestNewParquetLogExtension_SnowflakeSchemaUsesSnowflakeAdapter'
```

Expected: FAIL because the extension always constructs the Datadog adapter.

**Step 3: Write the minimal implementation**

Add a schema switch in `NewParquetLogExtension`:

```go
func newParquetAdapter(schema string, params extension.Settings) (adapters.ParquetAdapter, error) {
	switch strings.ToLower(schema) {
	case "", defaultSchema:
		return datadog.NewDatadogParquetAdapter(params)
	case "snowflake":
		return snowflake.NewSnowflakeParquetAdapter(params)
	default:
		return nil, fmt.Errorf("unsupported parquet schema: %s", schema)
	}
}
```

**Step 4: Run tests to verify they pass**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run 'TestNewParquetLogExtension_DefaultSchemaUsesDatadog|TestNewParquetLogExtension_SnowflakeSchemaUsesSnowflakeAdapter'
```

Expected: PASS

**Step 5: Commit**

```bash
git add extension/parquetlogencodingextension/extension.go extension/parquetlogencodingextension/extension_test.go
git commit -m "feat: route parquet extension by schema"
```

### Task 3: Implement the Snowflake adapter and its raw storage contract

**Files:**

- Create: `extension/parquetlogencodingextension/adapters/snowflake/adapter.go`
- Create: `extension/parquetlogencodingextension/adapters/snowflake/adapter_test.go`
- Modify: `extension/parquetlogencodingextension/adapters/adapter.go`
- Test: `extension/parquetlogencodingextension/adapters/snowflake/adapter_test.go`

**Step 1: Write the failing adapter tests**

Cover:

- top-level scalar fields
- `message_text` extraction order
- `body_json_text` only for JSON-object bodies
- canonical JSON text fields
- `schema_version = snowflake_v1`
- hot/cold bucket behavior

```go
func TestConvertToParquet_JSONBodyProducesSnowflakeRow(t *testing.T) {
	adapter, err := NewSnowflakeParquetAdapter(extensiontest.NewNopSettings(component.MustNewType("parquet_log_encoding")))
	require.NoError(t, err)

	rows, err := adapter.ConvertToParquet(context.Background(), newSnowflakeLogs())
	require.NoError(t, err)
	require.Len(t, rows, 1)

	row := rows[0].(ParquetLog)
	require.Equal(t, "snowflake_v1", row.SchemaVersion)
	require.Equal(t, "error", row.Status)
	require.NotEmpty(t, row.MessageText)
	require.NotEmpty(t, row.BodyJSONText)
	require.NotEmpty(t, row.AttributesHotText)
	require.NotEmpty(t, row.TagsHotText)
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run TestConvertToParquet_JSONBodyProducesSnowflakeRow
```

Expected: FAIL because the Snowflake adapter does not exist.

**Step 3: Write the minimal implementation**

Implement:

- `ParquetLog` struct for raw storage columns
- Datadog-compatible extraction for `service`, `host`, `source`
- OTLP-first `trace_id` / `span_id` with fallback
- `message_text` extraction order:
  - `message`
  - `msg`
  - `log`
  - `error.message`
  - fallback body serialization
- `body_json_text`
- `attributes_hot_text`, `attributes_cold_text`, `tags_hot_text`, `tags_cold_text`
- canonical JSON serializer with recursive key sorting
- precision-safe numeric string fallback

```go
type ParquetLog struct {
	TS                 int64  `parquet:"name=ts, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Service            string `parquet:"name=service, type=BYTE_ARRAY, logicaltype=STRING"`
	Status             string `parquet:"name=status, type=BYTE_ARRAY, logicaltype=STRING"`
	MessageText        string `parquet:"name=message_text, type=BYTE_ARRAY, logicaltype=STRING"`
	Host               string `parquet:"name=host, type=BYTE_ARRAY, logicaltype=STRING"`
	Source             string `parquet:"name=source, type=BYTE_ARRAY, logicaltype=STRING"`
	TraceID            string `parquet:"name=trace_id, type=BYTE_ARRAY, logicaltype=STRING"`
	SpanID             string `parquet:"name=span_id, type=BYTE_ARRAY, logicaltype=STRING"`
	SchemaVersion      string `parquet:"name=schema_version, type=BYTE_ARRAY, logicaltype=STRING"`
	BodyJSONText       string `parquet:"name=body_json_text, type=BYTE_ARRAY, logicaltype=STRING"`
	AttributesHotText  string `parquet:"name=attributes_hot_text, type=BYTE_ARRAY, logicaltype=STRING"`
	AttributesColdText string `parquet:"name=attributes_cold_text, type=BYTE_ARRAY, logicaltype=STRING"`
	TagsHotText        string `parquet:"name=tags_hot_text, type=BYTE_ARRAY, logicaltype=STRING"`
	TagsColdText       string `parquet:"name=tags_cold_text, type=BYTE_ARRAY, logicaltype=STRING"`
}
```

**Step 4: Run tests to verify they pass**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run 'TestConvertToParquet_JSONBodyProducesSnowflakeRow|TestConvertToParquet_NonJSONObjectBodyLeavesBodyJSONNull|TestConvertToParquet_EmptyHotListsProduceEmptyObjects'
```

Expected: PASS

**Step 5: Commit**

```bash
git add extension/parquetlogencodingextension/adapters/adapter.go extension/parquetlogencodingextension/adapters/snowflake/adapter.go extension/parquetlogencodingextension/adapters/snowflake/adapter_test.go
git commit -m "feat: add snowflake parquet adapter"
```

### Task 4: Add extension readback and logs-only validation tests

**Files:**

- Modify: `extension/parquetlogencodingextension/extension_test.go`
- Create: `extension/parquetlogencodingextension/snowflake_readback_test.go`
- Test: `extension/parquetlogencodingextension/snowflake_readback_test.go`

**Step 1: Write the failing parquet readback tests**

Add a test that marshals one Snowflake row and verifies raw parquet column names and basic values after readback.

```go
func TestMarshalLogsSnowflakeSchema_Readback(t *testing.T) {
	ext := newTestParquetExtension(t, &Config{
		MaxFileSizeBytes:   defaultMaxFileSizeBytes,
		NumberOfGoRoutines: defaultNumberOfGoRoutines,
		Schema:             "snowflake",
	})

	buf, err := ext.MarshalLogs(newSnowflakeLogs())
	require.NoError(t, err)

	validateParquetFile(t, buf, 1, func(t *testing.T, table arrow.Table) {
		expected := []string{
			"ts", "service", "status", "message_text", "host", "source",
			"trace_id", "span_id", "schema_version",
			"body_json_text", "attributes_hot_text", "attributes_cold_text",
			"tags_hot_text", "tags_cold_text",
		}
		for _, name := range expected {
			require.NotEmpty(t, table.Schema().FieldIndices(name))
		}
	})
}
```

**Step 2: Run test to verify it fails**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./... -run TestMarshalLogsSnowflakeSchema_Readback
```

Expected: FAIL until the adapter and schema wiring are complete.

**Step 3: Write minimal implementation / test helpers**

Add:

- `newTestParquetExtension(t, cfg)`
- `newSnowflakeLogs()` fixture builder
- readback assertions for required columns and `schema_version`

If signal validation belongs in the extension config or factory, add tests for:

- logs accepted
- metrics/traces rejected for `schema: snowflake`

**Step 4: Run tests to verify they pass**

Run:

```bash
cd extension/parquetlogencodingextension
go test ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add extension/parquetlogencodingextension/extension_test.go extension/parquetlogencodingextension/snowflake_readback_test.go
git commit -m "test: verify snowflake parquet raw schema"
```

### Task 5: Wire the extension into the contrib binary and document usage

**Files:**

- Modify: `cmd/otelcontribcol/builder-config.yaml`
- Modify: `extension/parquetlogencodingextension/README.md`
- Modify: `exporter/awss3exporter/README.md`
- Test: build via `make genotelcontribcol`

**Step 1: Write the failing integration expectation**

Add a lightweight docs/build task note in the README changes and confirm the builder config does not currently include the extension.

No new Go test is required here; the failing check is the binary generation/build step.

**Step 2: Run build command to verify current state**

Run:

```bash
cd /path/to/opentelemetry-collector-contrib
make genotelcontribcol
```

Expected: either the extension is absent from generated components or the new schema path is not available in the generated local binary config surface.

**Step 3: Write the minimal implementation**

Add the extension module to `cmd/otelcontribcol/builder-config.yaml`:

```yaml
extensions:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension v0.149.0
```

Document:

- `schema: snowflake`
- logs-only restriction
- hot-key config location on the extension
- use with `awss3exporter` via `encoding: parquet_log_encoding/...`

**Step 4: Run build/docs checks**

Run:

```bash
cd /path/to/opentelemetry-collector-contrib
make genotelcontribcol
cd extension/parquetlogencodingextension
go test ./...
```

Expected: PASS

**Step 5: Commit**

```bash
git add cmd/otelcontribcol/builder-config.yaml extension/parquetlogencodingextension/README.md exporter/awss3exporter/README.md
git commit -m "docs: wire snowflake parquet encoding into contrib build"
```

### Task 6: Add Snowflake SQL artifacts and benchmark fixtures

**Files:**

- Create: `docs/sql/saw-6662-track-a-raw-external-table.sql`
- Create: `docs/sql/saw-6662-track-a-public-secure-view.sql`
- Create: `docs/sql/saw-6662-track-a-benchmark-queries.sql`
- Modify: `docs/superpowers/specs/2026-04-06-saw-6662-track-a-snowflake-schema-design.md` only if implementation reveals a spec mismatch

**Step 1: Write the failing artifact expectation**

Create benchmark fixture comments/tests in the plan execution branch by treating the absence of these SQL artifacts as the failing state.

Primary requirement:

- raw external table uses raw `*_text` names
- public secure view exposes:
  - `body_json` as parsed `VARIANT`
  - `attributes_hot`, `attributes_cold`, `tags_hot`, `tags_cold` as parsed `VARIANT`
  - curated columns `env`, `version`, `customer_id`, `transaction_id`

**Step 2: Verify files do not exist yet**

Run:

```bash
cd /path/to/opentelemetry-collector-contrib
ls docs/sql
```

Expected: the Track A SQL artifacts do not exist yet.

**Step 3: Write the minimal implementation**

Author SQL files with:

- raw external table DDL
- secure view DDL with `TRY_PARSE_JSON` / `PARSE_JSON` into `VARIANT`
- curated-field precedence using plain, readable SQL
- benchmark fixtures including:
  - exact BigPanda reference queries as comments/reference
  - normalized public-view benchmark queries as the primary examples

Example secure-view fragment:

```sql
CREATE OR REPLACE SECURE VIEW bp_logs_public AS
SELECT
  ts,
  service,
  status,
  message_text,
  host,
  source,
  trace_id,
  span_id,
  schema_version,
  TRY_PARSE_JSON(body_json_text) AS body_json,
  TRY_PARSE_JSON(attributes_hot_text) AS attributes_hot,
  TRY_PARSE_JSON(attributes_cold_text) AS attributes_cold,
  TRY_PARSE_JSON(tags_hot_text) AS tags_hot,
  TRY_PARSE_JSON(tags_cold_text) AS tags_cold
FROM bp_logs_raw;
```

**Step 4: Review the SQL artifacts manually**

Run:

```bash
cd /path/to/opentelemetry-collector-contrib
sed -n '1,220p' docs/sql/saw-6662-track-a-raw-external-table.sql
sed -n '1,260p' docs/sql/saw-6662-track-a-public-secure-view.sql
sed -n '1,260p' docs/sql/saw-6662-track-a-benchmark-queries.sql
```

Expected: SQL matches the approved design and keeps benchmark queries plain.

**Step 5: Commit**

```bash
git add docs/sql/saw-6662-track-a-raw-external-table.sql docs/sql/saw-6662-track-a-public-secure-view.sql docs/sql/saw-6662-track-a-benchmark-queries.sql
git commit -m "docs: add track A snowflake sql artifacts"
```

### Task 7: Final verification and handoff

**Files:**

- Review: `extension/parquetlogencodingextension/...`
- Review: `cmd/otelcontribcol/builder-config.yaml`
- Review: `exporter/awss3exporter/README.md`
- Review: `docs/sql/...`

**Step 1: Run extension tests**

```bash
cd extension/parquetlogencodingextension
go test ./...
```

Expected: PASS

**Step 2: Run local contrib generation**

```bash
cd /path/to/opentelemetry-collector-contrib
make genotelcontribcol
```

Expected: PASS

**Step 3: Inspect git state**

```bash
cd /path/to/opentelemetry-collector-contrib
git status --short
git log --oneline --decorate -n 10
```

Expected: only intended files changed; commit history reflects small TDD-first steps.

**Step 4: Write final implementation summary**

Summarize:

- backward compatibility preserved
- new `schema: snowflake` path added
- logs-only behavior enforced
- Snowflake SQL artifacts included
- benchmark surface aligned with the approved spec

**Step 5: Commit any final doc/test-only cleanup**

```bash
git add -A
git commit -m "chore: finalize track A snowflake bakeoff wiring"
```
