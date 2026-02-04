CREATE SCHEMA IF NOT EXISTS stg;

-- Пользователи (приведённые)
CREATE TABLE IF NOT EXISTS stg.users (
    user_id     BIGINT PRIMARY KEY,
    email       TEXT,
    country     TEXT,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    source      TEXT NOT NULL DEFAULT 'kaggle_retailrocket',
    load_ts     TIMESTAMP NOT NULL DEFAULT now()
);

-- События (приведённые)
CREATE TABLE IF NOT EXISTS stg.events (
    event_id    BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    event_type  TEXT NOT NULL,
    event_ts    TIMESTAMP NOT NULL,
    source      TEXT NOT NULL DEFAULT 'kaggle_retailrocket',
    load_ts     TIMESTAMP NOT NULL DEFAULT now()
);

-- Индексы под типовые запросы
CREATE INDEX IF NOT EXISTS idx_stg_events_user_id ON stg.events(user_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_event_ts ON stg.events(event_ts);
