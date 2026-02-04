import os
import sys
import time
from dataclasses import dataclass

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Пути проекта
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

USERS_CSV = os.path.join(PROJECT_ROOT, "data", "users.csv")
EVENTS_CSV = os.path.join(PROJECT_ROOT, "data", "events.csv")


# Загрузка .env (без зависимостей)
def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        print(f"Файл .env не найден: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))

# Конфигурация БД
@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

def get_pg_config() -> PgConfig:
    load_dotenv(ENV_PATH)

    def required(name: str) -> str:
        value = os.getenv(name)
        if value is None:
            raise RuntimeError(
                f"Отсутствует переменная окружения {name} (проверь .env)"
            )
        return value

    return PgConfig(
        host=required("PGHOST"),
        port=int(required("PGPORT")),
        dbname=required("PGDATABASE"),
        user=required("PGUSER"),
        password=os.getenv("PGPASSWORD", ""),  # ← пустая строка допустима
    )


def connect(cfg: PgConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )

# SQL операции
def upsert_users(cur, rows):
    sql = """
        INSERT INTO raw.users (user_id, email, country, created_at, updated_at)
        VALUES %s
        ON CONFLICT (user_id) DO UPDATE SET
            email = EXCLUDED.email,
            country = EXCLUDED.country,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at;
    """
    execute_values(cur, sql, rows, page_size=5000)


def upsert_events(cur, rows):
    sql = """
        INSERT INTO raw.events (event_id, user_id, event_type, event_ts)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING;
    """
    execute_values(cur, sql, rows, page_size=5000)


# Логирование
def log(message: str) -> None:
    print(message, flush=True)


# Основной процесс
def main():
    start_time = time.time()

    cfg = get_pg_config()

    log("=== Загрузка RAW-данных начата ===")
    log(f"Подключение: {cfg.user}@{cfg.host}:{cfg.port}/{cfg.dbname}")
    log(f"Файл пользователей: {USERS_CSV}")
    log(f"Файл событий:       {EVENTS_CSV}")

    users_chunk_size = 200_000
    events_chunk_size = 200_000

    total_users = 0
    total_events = 0

    if not os.path.exists(USERS_CSV):
        raise FileNotFoundError(f"Файл users.csv не найден: {USERS_CSV}")

    if not os.path.exists(EVENTS_CSV):
        raise FileNotFoundError(f"Файл events.csv не найден: {EVENTS_CSV}")

    with connect(cfg) as conn:
        conn.autocommit = False

        with conn.cursor() as cur:
            # ---------- USERS ----------
            log("\n--- Загрузка пользователей ---")
            for i, chunk in enumerate(
                pd.read_csv(
                    USERS_CSV,
                    chunksize=users_chunk_size,
                    parse_dates=["created_at", "updated_at"],
                )
            ):
                chunk["user_id"] = chunk["user_id"].astype("int64")

                rows = list(
                    chunk[["user_id", "email", "country", "created_at", "updated_at"]]
                    .itertuples(index=False, name=None)
                )

                upsert_users(cur, rows)
                conn.commit()

                total_users += len(rows)
                log(f"Пользователи: чанк {i + 1}, строк = {len(rows)}, всего = {total_users}")

            # ---------- EVENTS ----------
            log("\n--- Загрузка событий ---")
            for i, chunk in enumerate(
                pd.read_csv(
                    EVENTS_CSV,
                    chunksize=events_chunk_size,
                    parse_dates=["event_ts"],
                )
            ):
                chunk["event_id"] = chunk["event_id"].astype("int64")
                chunk["user_id"] = chunk["user_id"].astype("int64")

                rows = list(
                    chunk[["event_id", "user_id", "event_type", "event_ts"]]
                    .itertuples(index=False, name=None)
                )

                upsert_events(cur, rows)
                conn.commit()

                total_events += len(rows)
                log(f"События: чанк {i + 1}, строк = {len(rows)}, всего = {total_events}")

    elapsed = time.time() - start_time

    log("\n=== Загрузка RAW-данных завершена ===")
    log(f"Всего пользователей загружено: {total_users}")
    log(f"Всего событий загружено:       {total_events}")
    log(f"Время выполнения: {elapsed:.1f} сек")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nОшибка при выполнении загрузки RAW:", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        raise
