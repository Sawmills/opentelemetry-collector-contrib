# SAW-6662 Track A: Snowflake Schema in `parquet_log_encoding`

## Summary

This design adds an additive `snowflake` schema mode to `parquet_log_encoding` in `sawmills-collector-contrib` for the Track A bakeoff path: plain S3 Parquet plus Snowflake external table plus secure view.

The design is intentionally hybrid:

- generic architecture inside contrib
- BigPanda-first schema and benchmark corpus
- backward compatible default behavior

Existing `parquet_log_encoding` configs must continue to work unchanged. The new behavior activates only when `schema: snowflake` is selected.

## Goals

- Preserve backward compatibility with existing `parquet_log_encoding` and `awss3exporter` configs.
- Add a Snowflake-oriented log schema that improves query ergonomics and benchmark performance for BigPanda query shapes.
- Keep `awss3exporter` generic and transport-focused.
- Make the Snowflake secure view the primary supported query surface for Track A.
- Keep the design small enough for a fair bakeoff against the existing Datadog-shaped parquet path.

## Non-Goals

- No breaking changes to existing Datadog-shaped parquet behavior.
- No traces or metrics support for `schema: snowflake` in v1.
- No configurable arbitrary top-level column generation in v1.
- No configurable curated secure-view columns in v1.
- No alternate default path partition strategy in v1.
- No requirement for a debug/raw-text Snowflake view in the first bakeoff.

## Architecture

Track A extends the existing `parquet_log_encoding` extension rather than creating a new extension.

### Component ownership

- `parquetlogencodingextension`
  - owns schema selection
  - owns row shape
  - owns hot-key config
  - owns buffering, flush behavior, and parquet telemetry
- `awss3exporter`
  - remains generic
  - uploads whatever encoding extension emits
  - does not learn Snowflake-specific semantics

### Config shape

The extension adds:

- `schema: datadog | snowflake`
- `attributes_hot_keys`
- `tags_hot_keys`

Rules:

- `schema` is optional
- omitted `schema` means `datadog`
- omitted hot-key lists use built-in defaults
- empty hot-key lists mean no hot keys
- non-empty hot-key lists replace the defaults completely

### Backward compatibility

Backward compatibility is required:

- existing `parquet_log_encoding/...` configs continue to work unchanged
- existing `awss3exporter` configs using `encoding: parquet_log_encoding/...` and `encoding_file_extension: parquet` continue to work unchanged
- existing Datadog-shaped parquet output remains behaviorally compatible when `schema` is omitted

The new Snowflake behavior is strictly additive and only activates with `schema: snowflake`.

## Track A Storage Contract

For `schema: snowflake`, parquet storage is intentionally different from the Datadog-shaped schema.

### Scalar storage columns

These names remain the same in storage and in the public secure view:

- `ts`
- `service`
- `status`
- `message_text`
- `host`
- `source`
- `trace_id`
- `span_id`
- `schema_version`

Semantics:

- `ts` is required and typed as a timestamp
- `service`, `status`, `message_text`, `host`, `source`, `trace_id`, `span_id` are nullable
- `status` is normalized to lower-case
- `schema_version` is required and always `snowflake_v1`

### Raw JSON-text storage columns

These names are storage-truth names and are used directly in parquet and the raw Snowflake external table:

- `body_json_text`
- `attributes_hot_text`
- `attributes_cold_text`
- `tags_hot_text`
- `tags_cold_text`

Semantics:

- `body_json_text` is nullable
- `attributes_hot_text`, `attributes_cold_text`, `tags_hot_text`, `tags_cold_text` are always non-null
- empty hot/cold buckets are stored as canonical `{}` rather than null
- non-JSON-object bodies store `body_json_text = null`

## Field Semantics

### Timestamp

- `ts` comes only from the canonical OTLP log timestamp
- no fallback to attributes or body timestamps in v1
- missing or invalid `ts` drops the row and emits a metric

### Service, host, source

`service`, `host`, and `source` preserve existing Datadog-style extraction behavior as much as possible in order to minimize migration risk and keep comparisons fair.

### Trace and span IDs

Precedence:

1. canonical OTLP IDs
2. known Datadog/log attribute fallbacks
3. null

### `message_text`

`message_text` should be usable in customer queries whenever possible.

Extraction order:

1. top-level `message`
2. top-level `msg`
3. top-level `log`
4. nested `error.message`
5. original body string
6. compact JSON-style serialization of non-object/non-string OTLP body values

`message_text` is only null when the body is truly absent or unusable.

No separate `message_text_raw` field is included in v1.

### `body_json_text`

`body_json_text` is only populated when the original body is already a JSON object by v1 rules.

Rules:

- parse the object body
- canonicalize it before writing
- sort keys recursively at all nesting levels
- preserve normal numeric values as numeric types where safely representable
- fallback to strings for precision-sensitive numeric edge cases only

`body_json_text` stores canonical compact JSON text, not the original raw formatting.

### Hot and cold buckets

Promoted top-level fields remain preserved in attrs/tags JSON for lossless parity.

Hot-key behavior:

- two separate configurable lists:
  - `attributes_hot_keys`
  - `tags_hot_keys`
- configurable lists affect hot/cold membership only
- configurable lists do not generate arbitrary top-level columns

## Snowflake Contracts

Track A includes both a raw external table and a public secure view.

### Raw external table

The raw external table is an implementation detail, not the primary supported query surface.

Its purpose is to reflect storage truth:

- scalar columns with query-oriented names
- raw JSON text columns with `*_text` names

### Public secure view

The secure view is the supported query surface.

It exposes:

- scalar columns:
  - `ts`
  - `service`
  - `status`
  - `message_text`
  - `host`
  - `source`
  - `trace_id`
  - `span_id`
  - `schema_version`
- parsed `VARIANT` fields:
  - `body_json`
  - `attributes_hot`
  - `attributes_cold`
  - `tags_hot`
  - `tags_cold`
- curated nullable columns:
  - `env`
  - `version`
  - `customer_id`
  - `transaction_id`

The public view does not expose raw `*_text` debug columns.

### Optional debug/internal view

The design defines an optional internal/debug view that may expose:

- raw `*_text` columns
- raw-table aligned inspection fields

This view is not required for the first bakeoff.

## Curated Column Rules

Curated secure-view columns are fixed and opinionated in v1. They are not configurable.

Curated set:

- `env`
- `version`
- `customer_id`
- `transaction_id`

Mapping style:

- fixed, opinionated source mappings
- not generated from hot-key config

Precedence:

1. top-level field if one exists
2. `attributes_hot`
3. `tags_hot`
4. `attributes_cold`
5. `tags_cold`

Missing curated values remain null.

If conflicting values exist across sources:

- the first value by precedence wins
- a conflict metric is emitted

Conflict metrics are limited to these curated fields in v1.

## Error Handling and Coercion

### Hard failures

Fail at config/startup:

- unsupported `schema`
- use of `schema: snowflake` for metrics
- use of `schema: snowflake` for traces

### Row-drop rule

Drop row only for unrecoverable timestamp failure:

- missing or invalid OTLP timestamp for `ts`

### Recoverable conversion issues

Keep the row when possible.

Rules:

- preserve the original raw value as a string when possible
- use null only when there is no faithful raw representation
- precision-sensitive numeric values fall back to strings
- `body_json_text` becomes null when the body is not a valid JSON object by v1 rules

## Metrics

Use a single bounded metric family with `reason` labels only in v1.

Representative reasons:

- `precision_string_fallback`
- `json_parse_fallback`
- `curated_field_conflict`
- `row_drop_invalid_ts`

No `field` label in v1.

## Partitioning and Upload Path

Track A keeps the existing default time-based path layout for the bakeoff.

Rationale:

- isolates schema/query-surface changes from path-layout changes
- keeps comparison against current Datadog-shaped parquet fair
- avoids mixing multiple tuning axes in the first bakeoff

Path partitioning is effectively fixed for the bakeoff contract even if the exporter remains generally configurable.

`service` is not part of the default path partitioning in v1.

## Benchmark Contract

Primary success is judged on secure-view performance against the real customer query corpus.

### Primary benchmark

- public secure view
- real BigPanda query corpus
- normalized query style intended for end users

### Secondary diagnostics

- raw external-table behavior may be inspected for debugging
- raw-table performance is not the primary success metric

### Benchmark corpus

The benchmark suite includes:

- verbatim customer queries as reference fixtures
- normalized secure-view versions as the primary benchmark SQL
- a small number of derived variants to avoid overfitting

Recommended mix:

- 2 exact customer queries
- 3 to 5 derived variants

Primary benchmark SQL should remain plain:

- avoid `COALESCE` where possible
- avoid casts where possible
- avoid reparsing JSON in the query when the view can absorb it

## Expected Impact by Query Shape

This design is explicitly aligned to the BigPanda query shapes gathered during SAW-6662 discovery.

### 1. Time range + service + message substring

Example shape:

- filter by `ts`
- filter by `service`
- filter by substring search over `message_text`

Expected impact:

- `ts` and `service` become typed top-level columns rather than being inferred from generic payloads
- `message_text` becomes a dedicated query field rather than a giant serialized envelope
- Snowflake can narrow the candidate set using time and service filters before applying substring search

Important limitation:

- Track A is not a true full-text index
- the main gain for this query shape is pruning before text search, not making raw `%...%` search intrinsically cheap

### 2. Time bucket + status + service aggregation

Example shape:

- `DATE_TRUNC(..., ts)`
- filter by `status`
- filter by `service`
- aggregate `COUNT(*)`

Expected impact:

- this is the query shape most directly improved by the new schema
- `ts`, `status`, and `service` are typed top-level columns
- `status` is normalized at write time
- aggregation queries can avoid pulling large JSON/text payloads into the plan unless needed

### 3. JSON-property lookup from the log body

Example shape:

- filter by `ts`
- filter by `service`
- filter by a property inside the structured log body

Expected impact:

- `body_json_text` is stored when the original body is a JSON object
- the secure view exposes parsed `body_json` as `VARIANT`
- customer queries do not need to reparse a giant message string in-query

### 4. Curated troubleshooting filters

Example shape:

- filter by fields such as `env`, `version`, `customer_id`, `transaction_id`

Expected impact:

- these columns are exposed directly in the secure view
- precedence is deterministic and documented
- conflicts are observable through metrics

### Overall expectation

The design is expected to help BigPanda most on:

1. `status` / `service` / `time` aggregations
2. time-window + service point lookups
3. body-JSON property queries

It is expected to help free-text message search primarily by reducing the number of rows and files that must be searched first, not by introducing a text-search-specific indexing strategy.

## Scope

### In scope

- additive `schema: snowflake` mode in `parquetlogencodingextension`
- new Snowflake adapter
- backward-compatible default `datadog` path
- hot-key config on the extension
- contrib builder/distribution wiring so the feature is usable in the contrib binary
- Snowflake raw external table contract
- Snowflake public secure view contract
- benchmark SQL fixtures for the public view

### Out of scope for Track A v1

- traces/metrics support for `schema: snowflake`
- configurable arbitrary top-level column generation
- configurable curated secure-view columns
- alternate default path partitioning
- required debug/internal view

## Testing Strategy

Required test layers:

### Config tests

- omitted `schema` preserves datadog default behavior
- `schema: snowflake` accepted
- logs-only validation for `schema: snowflake`
- hot-key omitted vs empty vs explicit replacement semantics

### Adapter tests

- `message_text` extraction rules
- `body_json_text` population rules
- top-level field extraction
- lossless parity retention
- canonical JSON output with recursive key sorting
- precision-safe numeric fallback

### Extension and parquet readback tests

- raw storage column names and types
- required vs nullable expectations
- `schema_version = snowflake_v1`

### Regression tests

- existing Datadog-shaped parquet path remains behaviorally compatible

### Snowflake artifact tests and fixtures

- external table DDL fixture
- public secure view SQL fixture
- benchmark query fixtures against the public view

## Recommended Implementation Shape

Recommended approach:

- keep `parquet_log_encoding`
- add `schema: datadog | snowflake`
- add a new `snowflake` adapter
- keep shared buffering/flush/telemetry behavior
- keep `awss3exporter` generic

This is the smallest change that:

- preserves current configs
- fits the contrib architecture
- keeps the design plausibly upstreamable
- gives BigPanda a meaningful Track A bakeoff path

## Decision Summary

Locked design choices:

- hybrid design target: generic core, BigPanda-first schema/tests
- `schema` option lives inside `parquetlogencodingextension`
- `schema` defaults to `datadog`
- `snowflake` is logs-only and fails fast for metrics/traces
- fixed top-level columns plus lossless parity in JSON buckets
- hot-key configurability affects hot/cold membership only
- public contract uses a secure view
- raw external table is implementation detail
- optional debug/internal view, not required in the first bakeoff
