-- Сколько строк в stg
SELECT 'stg.users' AS table_name, COUNT(*) AS cnt FROM stg.users
UNION ALL
SELECT 'stg.events' AS table_name, COUNT(*) AS cnt FROM stg.events;

-- Проверка: NULL там, где нельзя
SELECT
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
  SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_type,
  SUM(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END) AS null_event_ts
FROM stg.events;

-- Orphan events: события без пользователя (в stg.users)
SELECT COUNT(*) AS orphan_events
FROM stg.events e
LEFT JOIN stg.users u ON u.user_id = e.user_id
WHERE u.user_id IS NULL;

-- Распределение по типам событий
SELECT event_type, COUNT(*) AS cnt
FROM stg.events
GROUP BY 1
ORDER BY cnt DESC;
