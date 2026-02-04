-- 1) Сколько строк в DDS
SELECT
  (SELECT COUNT(*) FROM dds.dim_users)  AS dds_users,
  (SELECT COUNT(*) FROM dds.fact_events) AS dds_events;

-- 2) Orphan events в DDS (на всякий случай)
SELECT COUNT(*) AS orphan_events_dds
FROM dds.fact_events e
LEFT JOIN dds.dim_users u ON u.user_id = e.user_id
WHERE u.user_id IS NULL;

-- 3) Распределение типов
SELECT event_type, COUNT(*) AS cnt
FROM dds.fact_events
GROUP BY 1
ORDER BY cnt DESC;
