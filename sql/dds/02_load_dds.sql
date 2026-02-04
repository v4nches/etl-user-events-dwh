BEGIN;

-- 1) dim_users: делаем upsert по user_id
INSERT INTO dds.dim_users (user_id, email, country, created_at, updated_at, source, load_ts)
SELECT
  user_id,
  email,
  country,
  created_at,
  updated_at,
  source,
  load_ts
FROM stg.users
ON CONFLICT (user_id) DO UPDATE SET
  email      = EXCLUDED.email,
  country    = EXCLUDED.country,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at,
  source     = EXCLUDED.source,
  load_ts    = EXCLUDED.load_ts;

-- 2) fact_events: upsert/ignore по event_id (события считаем неизменяемыми)
INSERT INTO dds.fact_events (event_id, user_id, event_type, event_ts, source, load_ts)
SELECT
  event_id,
  user_id,
  event_type,
  event_ts,
  source,
  load_ts
FROM stg.events
ON CONFLICT (event_id) DO NOTHING;

COMMIT;
