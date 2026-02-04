-- Загрузка из Raw слоя
-- (позже сделаем инкрементальную загрузку)

TRUNCATE TABLE stg.events;
TRUNCATE TABLE stg.users;

-- USERS: очистка строк, нормализация
INSERT INTO stg.users (user_id, email, country, created_at, updated_at, source, load_ts)
SELECT
    u.user_id,
    NULLIF(BTRIM(u.email), '') AS email,
    NULLIF(BTRIM(u.country), '') AS country,
    u.created_at,
    u.updated_at,
    'kaggle_retailrocket' AS source,
    now() AS load_ts
FROM raw.users u;

-- EVENTS: нормализация event_type, отбор только валидных строк
INSERT INTO stg.events (event_id, user_id, event_type, event_ts, source, load_ts)
SELECT
    e.event_id,
    e.user_id,
    LOWER(BTRIM(e.event_type)) AS event_type,
    e.event_ts,
    'kaggle_retailrocket' AS source,
    now() AS load_ts
FROM raw.events e
WHERE e.event_id IS NOT NULL
  AND e.user_id IS NOT NULL
  AND e.event_ts IS NOT NULL
  AND e.event_type IS NOT NULL;
