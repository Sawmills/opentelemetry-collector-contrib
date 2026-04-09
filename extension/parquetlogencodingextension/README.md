# Parquet Log Encoding Extension

Buffers OTLP logs and encodes them into parquet payloads for downstream exporters such as `awss3exporter`.

## Supported schemas

- `datadog` (default): compatibility-oriented Datadog-shaped parquet rows.
- `snowflake`: logs-only Snowflake-oriented parquet rows for queryable S3 export.

Backward compatibility is preserved:

- existing `parquet_log_encoding` configs continue to work unchanged
- omitted `schema` defaults to `datadog`

## Configuration

```yaml
extensions:
  parquet_log_encoding:
    schema: snowflake
    compression_codec: snappy
    max_file_size_bytes: 104857600
    row_group_size_bytes: 104857600
    page_size_bytes: 1048576
```

Supported fields:

- `schema`: `datadog` or `snowflake`
- `compression_codec`: `snappy`, `zstd`, `gzip`, `uncompressed`
- `max_file_size_bytes`
- `row_group_size_bytes`
- `page_size_bytes`
- `number_of_go_routines`

## `snowflake` schema notes

- logs only; metrics and traces are rejected for this schema path
- top-level columns:
  - `ts`
  - `service`
  - `status`
  - `message_text`
  - `host`
  - `source`
  - `trace_id`
  - `span_id`
  - `schema_version`
- JSON payload columns:
  - `body_json_text`
  - `attributes_hot_text`
  - `attributes_cold_text`
  - `tags_hot_text`
  - `tags_cold_text`
- current built-in hot buckets:
  - attributes: none by default
  - tags: `env`, `version`
- cold attributes are bounded before archival:
  - excludes `k8s.node.uid`, `k8s.pod.ip`, `k8s.pod.uid`
  - caps archived cold-attribute keys at `128`
  - truncates archived string values at `4096` bytes

The `snowflake` storage contract is intended to pair with a Snowflake external table and secure view. The parquet files keep raw JSON text payloads; the Snowflake view is expected to parse those fields into query-friendly `VARIANT` columns.
