-- SAW-6662 Track A raw external table
-- Storage-truth contract for parquet_log_encoding schema=snowflake.
-- Replace database, schema, stage, and file format names for the target environment.

create or replace external table RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__RAW (
  ts timestamp_ntz as to_timestamp_ntz(value:ts::number / 1000),
  service string as value:service::string,
  status string as value:status::string,
  message_text string as value:message_text::string,
  host string as value:host::string,
  source string as value:source::string,
  trace_id string as value:trace_id::string,
  span_id string as value:span_id::string,
  schema_version string as value:schema_version::string,
  body_json_text string as value:body_json_text::string,
  attributes_hot_text string as value:attributes_hot_text::string,
  attributes_cold_text string as value:attributes_cold_text::string,
  tags_hot_text string as value:tags_hot_text::string,
  tags_cold_text string as value:tags_cold_text::string
)
with location = @RAW_PLATFORM.SAWMILLS_BIGPANDA_TRACK_A_STAGE/logs/
auto_refresh = false
file_format = (
  type = parquet
);
