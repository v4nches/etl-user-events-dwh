CREATE SCHEMA IF NOT EXISTS dds;

-- Измерение пользователей
CREATE TABLE IF NOT EXISTS dds.dim_users (
  user_id    BIGINT PRIMARY KEY,
  email      TEXT,
  country    TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  source     TEXT NOT NULL,
  load_ts    TIMESTAMP NOT NULL
);

-- Факт событий
CREATE TABLE IF NOT EXISTS dds.fact_events (
  event_id   BIGINT PRIMARY KEY,
  user_id    BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  event_ts   TIMESTAMP NOT NULL,
  source     TEXT NOT NULL,
  load_ts    TIMESTAMP NOT NULL
);

-- Индексы под типовые запросы
CREATE INDEX IF NOT EXISTS idx_fact_events_user_id ON dds.fact_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_event_ts ON dds.fact_events(event_ts);
CREATE INDEX IF NOT EXISTS idx_fact_events_event_type ON dds.fact_events(event_type);
