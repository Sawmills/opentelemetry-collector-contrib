-- SAW-6662 Track A public secure view
-- Supported query surface for the Snowflake bakeoff.

create or replace secure view RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS as
with parsed as (
  select
    ts,
    service,
    status,
    message_text,
    host,
    source,
    trace_id,
    span_id,
    schema_version,
    try_parse_json(body_json_text) as body_json,
    try_parse_json(attributes_hot_text) as attributes_hot,
    try_parse_json(attributes_cold_text) as attributes_cold,
    try_parse_json(tags_hot_text) as tags_hot,
    try_parse_json(tags_cold_text) as tags_cold
  from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__RAW
)
select
  ts,
  service,
  status,
  message_text,
  host,
  source,
  trace_id,
  span_id,
  schema_version,
  body_json,
  attributes_hot,
  attributes_cold,
  tags_hot,
  tags_cold,
  coalesce(
    attributes_hot:env::string,
    tags_hot:env::string,
    attributes_cold:env::string,
    tags_cold:env::string
  ) as env,
  coalesce(
    attributes_hot:version::string,
    tags_hot:version::string,
    attributes_cold:version::string,
    tags_cold:version::string
  ) as version,
  coalesce(
    attributes_hot:"customer.id"::string,
    tags_hot:"customer.id"::string,
    attributes_cold:"customer.id"::string,
    tags_cold:"customer.id"::string
  ) as customer_id,
  coalesce(
    attributes_hot:"transaction.id"::string,
    tags_hot:"transaction.id"::string,
    attributes_cold:"transaction.id"::string,
    tags_cold:"transaction.id"::string
  ) as transaction_id
from parsed;
