# etl-user-events-dwh
ETL pipeline and DWH for user events analytics

# ETL User Events DWH

Проект по построению ETL-пайплайна и хранилища данных
для аналитики пользовательских событий.

## Архитектура
RAW → DWH → Data Mart

## Стек технологий
Python, PostgreSQL, SQL

## Структура проекта
- `ingestion/` — скрипты Python для загрузки данных
- `sql/` — SQL-скрипты для staging, DWH и marts
- `tests/` — проверки качества данных
- `diagrams/` — схемы и диаграммы

## Цель проекта
Демонстрация навыков Data Engineer:
- проектирование DWH
- инкрементальная загрузка данных
- построение витрин и проверок качества данных
