import os
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

KAGGLE_EVENTS = os.path.join(PROJECT_ROOT, "data", "kaggle_raw", "events.csv")
USERS_OUT = os.path.join(PROJECT_ROOT, "data", "users.csv")
EVENTS_OUT = os.path.join(PROJECT_ROOT, "data", "events.csv")


def main():
    print("=== Запуск подготовки данных Kaggle ===")
    print(f"Источник данных: {KAGGLE_EVENTS}")

    # Проверка наличия исходного файла
    if not os.path.exists(KAGGLE_EVENTS):
        raise FileNotFoundError(
            f"Файл с событиями Kaggle не найден: {KAGGLE_EVENTS}"
        )

    print("Чтение файла events.csv ...")

    try:
        df = pd.read_csv(KAGGLE_EVENTS)
    except Exception as e:
        raise RuntimeError(
            f"Ошибка при чтении CSV-файла {KAGGLE_EVENTS}: {e}"
        )

    expected_columns = {"timestamp", "visitorid", "event", "itemid", "transactionid"}
    missing_columns = expected_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Файл events.csv не содержит обязательные колонки: {sorted(missing_columns)}"
        )

    print("Преобразование временной метки timestamp → event_ts ...")

    try:
        df["event_ts"] = pd.to_datetime(df["timestamp"], unit="ms", errors="raise")
    except Exception as e:
        raise ValueError(
            f"Ошибка при преобразовании timestamp в дату/время: {e}"
        )

    print("Формирование таблицы событий (events) ...")

    events_df = df[["visitorid", "event", "event_ts"]].copy()
    events_df.rename(
        columns={
            "visitorid": "user_id",
            "event": "event_type",
        },
        inplace=True,
    )

    # Генерация surrogate key для событий
    events_df.insert(0, "event_id", range(1, len(events_df) + 1))

    print("Формирование таблицы пользователей (users) ...")

    users_df = (
        events_df.groupby("user_id", as_index=False)
        .agg(
            created_at=("event_ts", "min"),
            updated_at=("event_ts", "max"),
        )
    )

    users_df["email"] = None
    users_df["country"] = None
    users_df = users_df[
        ["user_id", "email", "country", "created_at", "updated_at"]
    ]

    print("Сохранение подготовленных данных ...")

    os.makedirs(os.path.join(PROJECT_ROOT, "data"), exist_ok=True)

    try:
        users_df.to_csv(USERS_OUT, index=False)
        events_df.to_csv(EVENTS_OUT, index=False)
    except Exception as e:
        raise RuntimeError(
            f"Ошибка при сохранении CSV-файлов: {e}"
        )

    print(f"Пользователи сохранены: {len(users_df)} строк → {USERS_OUT}")
    print(f"События сохранены:     {len(events_df)} строк → {EVENTS_OUT}")
    print("=== Подготовка данных Kaggle завершена успешно ===")


if __name__ == "__main__":
    main()
