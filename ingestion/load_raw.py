import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

#  (вынесен в .env) 
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "user_events_dwh"
PG_USER = "ivan"      
PG_PASSWORD = ""      

#  пути 
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
USERS_CSV = os.path.join(PROJECT_ROOT, "data", "users.csv")
EVENTS_CSV = os.path.join(PROJECT_ROOT, "data", "events.csv")


def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def read_users(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")
    df = pd.read_csv(path)

    required = ["user_id", "email", "country", "created_at", "updated_at"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"users.csv: не хватает колонок: {missing}")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="raise")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="raise")
    return df[required]


def read_events(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")
    df = pd.read_csv(path)

    required = ["event_id", "user_id", "event_type", "event_ts"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"events.csv: не хватает колонок: {missing}")

    df["event_ts"] = pd.to_datetime(df["event_ts"], errors="raise")
    return df[required]


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
    execute_values(cur, sql, rows, page_size=1000)


def upsert_events(cur, rows):
    sql = """
        INSERT INTO raw.events (event_id, user_id, event_type, event_ts)
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            event_type = EXCLUDED.event_type,
            event_ts = EXCLUDED.event_ts;
    """
    execute_values(cur, sql, rows, page_size=1000)


def main():
    print("=== RAW ingestion started ===")
    print("Users:", USERS_CSV)
    print("Events:", EVENTS_CSV)

    users_df = read_users(USERS_CSV)
    events_df = read_events(EVENTS_CSV)

    users_rows = list(users_df.itertuples(index=False, name=None))
    events_rows = list(events_df.itertuples(index=False, name=None))

    try:
        conn = connect()
    except Exception as e:
        raise RuntimeError("Не удалось подключиться к PostgreSQL. Проверь PG_* вверху файла.") from e

    try:
        with conn:
            with conn.cursor() as cur:
                upsert_users(cur, users_rows)
                upsert_events(cur, events_rows)
        print(f"OK: users={len(users_rows)}, events={len(events_rows)} загружены в {PG_DB}")
    finally:
        conn.close()

    print("=== RAW ingestion finished ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(f"ERROR: {err}")
        sys.exit(1)
