# etl-user-events-dwh
ETL pipeline and DWH for user events analytics

---

# ETL User Events DWH

Учебный ETL-проект по построению хранилища данных (DWH) для пользовательских событий
на основе PostgreSQL и SQL.

Проект демонстрирует полный цикл обработки данных:
RAW → STG → DDS с проверками качества данных (DQ checks).

---

## Architecture

**Источник данных (CSV / Kaggle)**  
→ **RAW** — загрузка сырых данных  
→ **STG** — очистка и приведение данных  
→ **DDS** — витрины (dimensions / facts)  
→ **DQ checks** — контроль качества данных


---

## Project structure

```text
etl-user-events-dwh/
├─ ingestion/
│  └─ load_raw.py
├─ sql/
│  ├─ staging/
│  │  ├─ 01_create_stg.sql
│  │  ├─ 02_load_stg.sql
│  │  └─ 03_checks.sql
│  └─ dds/
│     ├─ 01_create_dds.sql
│     ├─ 02_load_dds.sql
│     └─ 03_checks_dds.sql
├─ .env.example
├─ .gitignore
└─ README.md

