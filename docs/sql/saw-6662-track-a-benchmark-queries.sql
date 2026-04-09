-- SAW-6662 Track A benchmark corpus
-- Keep the exact customer queries for traceability, then benchmark the normalized
-- public-view variants as the primary query surface.

-- Reference customer query 1: time range + service + message substring
-- select * from DWH.RAW_PLATFORM.BP_S3__2_0_0__SAWMILLS_LOGS
-- where
--   EXISTS (
--       SELECT 1
--       FROM LATERAL FLATTEN(input => ARRAY_CONSTRUCT('processing command', 'enriched')) f
--       WHERE message ILIKE '%' || f.value || '%'
--   )
--   AND SERVICE IN ('alert-filtering-pipeline', 'envy')
--   AND DATE between '2026-01-22 18:18:20' and '2026-01-22 18:18:21';

-- Reference customer query 2: time bucket + status + service aggregation
-- select DATE_TRUNC('MINUTE', DATE) AS minute,
--   COUNT(*) AS errors
-- from DWH.RAW_PLATFORM.BP_S3__2_0_0__SAWMILLS_LOGS
-- where
--   lower(status) = 'error'
--   AND EXISTS (
--       SELECT 1
--       FROM LATERAL FLATTEN(input => ARRAY_CONSTRUCT('Error occurred, while serving request')) f
--       WHERE message ILIKE '%' || f.value || '%'
--   )
--   AND SERVICE IN ('alert-filtering-pipeline', 'envy')
--   AND DATE between '2026-01-22 18:18:20' and '2026-01-22 18:18:26'
-- group by 1
-- limit 100;

-- Normalized benchmark query 1: point lookup on the public secure view
select *
from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS
where service in ('alert-filtering-pipeline', 'envy')
  and ts between '2026-01-22 18:18:20'::timestamp_ntz and '2026-01-22 18:18:21'::timestamp_ntz
  and (
    message_text ilike '%processing command%'
    or message_text ilike '%enriched%'
  );

-- Normalized benchmark query 2: aggregation on the public secure view
select date_trunc('minute', ts) as minute,
  count(*) as errors
from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS
where service in ('alert-filtering-pipeline', 'envy')
  and status = 'error'
  and ts between '2026-01-22 18:18:20'::timestamp_ntz and '2026-01-22 18:18:26'::timestamp_ntz
  and message_text ilike '%Error occurred, while serving request%'
group by 1
limit 100;

-- Derived benchmark query 3: same aggregation shape without message search
select date_trunc('minute', ts) as minute,
  count(*) as errors
from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS
where service in ('alert-filtering-pipeline', 'envy')
  and status = 'error'
  and ts between '2026-01-22 18:18:20'::timestamp_ntz and '2026-01-22 18:23:20'::timestamp_ntz
group by 1
order by 1;

-- Derived benchmark query 4: JSON-property lookup through parsed body_json
select ts,
  service,
  status,
  message_text
from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS
where service in ('alert-filtering-pipeline', 'envy')
  and ts between '2026-01-22 18:18:20'::timestamp_ntz and '2026-01-22 18:18:26'::timestamp_ntz
  and body_json:request_id::string = 'req-123';

-- Derived benchmark query 5: curated-column troubleshooting filter
select ts,
  service,
  env,
  version,
  customer_id,
  transaction_id,
  message_text
from RAW_PLATFORM.BP_S3__SAW_6662_TRACK_A__LOGS
where ts between '2026-01-22 18:18:20'::timestamp_ntz and '2026-01-22 18:23:20'::timestamp_ntz
  and env = 'prod'
  and customer_id = '12345'
order by ts
limit 200;
