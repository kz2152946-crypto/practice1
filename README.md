# Практическая №1 — локальный Lakehouse, ELT по медальону и бенчмарк форматов/сжатия

**Студентка:** Зобова Ксения Павловна, МИСИС, БИВТ-24-СП-3
**Курс:** «Основы теории информации» (VK Education)
**Вариант 2:** TPC-H Q10 «Returned Item Reporting» — топ-20 клиентов по выручке от возвращённых позиций за 4-й квартал 1993
**Масштаб:** `sf1` (lineitem — 6 001 215 строк)

Заполненная форма отчёта — в [`отчет.md`](отчет.md). Здесь — что я сделала, как это устроено и как повторить.

## Что сделано

1. Подняла стек **MinIO (форк silo) + PostgreSQL + Lakekeeper + Trino** отдельными `docker run`, как в гайде 1.
2. Сделала три слоя, каждый в своём бакете / warehouse / каталоге Trino:
   - **Bronze** — сырые `customer`, `orders`, `lineitem`, `nation` из `tpch.sf1` + `loaded_at`, `source`;
   - **Silver** — очищенные и обогащённые `dim_customer` (клиент + страна) и `fct_order_line` (строка заказа + дата заказа + выручка строки);
   - **Gold** — витрина `top20_returned_customers_q4_1993`, которая отвечает на мой запрос.
3. Проверила, что Gold совпадает с эталонным Q10 напрямую по `tpch.sf1` (EXCEPT в обе стороны — 0 строк).
4. Бенчмарк: пересоздала Bronze в 8 вариантах (PARQUET/ORC × NONE/SNAPPY/GZIP/ZSTD), замерила размер, время записи, время Q10 и Q1 (cold/warm).
5. Schema evolution на Silver (ADD / RENAME / widen типа) и time travel с восстановлением из снапшота.
6. Подключила **Apache Spark** напрямую к Lakekeeper и прочитала Bronze; бонус — 10 параллельных потоков и сравнение Trino vs Spark.

## Архитектура

```
                    SQL (CLI / DBeaver)
                          │
                          ▼
   ┌──────────────┐  REST  ┌───────────────┐  JDBC  ┌──────────────┐
   │ Trino 476    │ ─────► │  Lakekeeper   │ ─────► │ PostgreSQL 17│
   │ :8080        │        │  (Iceberg     │        │ :5433        │
   │ каталоги:    │        │  REST Catalog)│        │ (метаданные  │
   │ bronze silver│        │  :8181        │        │  каталога)   │
   │ gold bench   │        └───────┬───────┘        └──────────────┘
   │ tpch         │                │ warehouse → бакет
   └──────┬───────┘                ▼
          │ читает/пишет  ┌──────────────────────────────────────┐
          └────────────►  │ MinIO (pgsty/silo) :9000 / :9001      │
   ┌──────────────┐       │ бакеты: bronze | silver | gold | bench│
   │ Spark 3.5.3  │ ────► │ data/*.parquet|orc + metadata/*.json  │
   │ (Iceberg REST│       └──────────────────────────────────────┘
   │  напрямую)   │
   └──────────────┘

tpch.sf1 ──CTAS──► bronze.lakehouse.* ──ELT──► silver.lakehouse.* ──агрегат──► gold.lakehouse.*
```

Порт Postgres — **5433**, а не 5432: на моём ноутбуке 5432 уже занят локальным Postgres (так советует гайд 1, §8). Lakekeeper ходит к нему через `host.docker.internal:5433`.

## Как воспроизвести

Нужен Docker Desktop (у меня 8 ГБ памяти на Docker) и ~3 ГБ свободного места.

```bash
git clone <этот репозиторий> practice1 && cd practice1

# 1. стек: MinIO, Postgres, Lakekeeper, бакеты, 4 warehouse, Trino; в конце SHOW CATALOGS
bash scripts/01_stack.sh

# 2. слои
docker exec -i trino trino < sql/01_bronze.sql
docker exec -i trino trino --output-format ALIGNED < sql/02_silver.sql   # + проверки качества
docker exec -i trino trino --output-format ALIGNED < sql/03_gold.sql     # + сверка с эталоном Q10
docker exec -i trino trino --output-format CSV_HEADER < sql/00_layer_stats.sql > results/layers_stats.csv

# 3. бенчмарк (≈20 минут, сам перезапускает trino и minio для cold-замеров)
python3 scripts/benchmark.py

# 4. эволюция схемы и time travel
docker exec -i trino trino --output-format ALIGNED < sql/05_schema_evolution.sql
bash scripts/schema_evolution_metadata.sh     # ID колонок из metadata.json
bash scripts/time_travel.sh

# 5. Spark поверх Bronze (+ бонус: 10 потоков)
bash scripts/03_spark.sh
bash scripts/04_parallel.sh
```

Остановить, не теряя данных: `docker stop trino lakekeeper lakehouse-pg lakehouse-minio`.
Удалить совсем: `docker rm -f trino lakekeeper lakehouse-pg lakehouse-minio && docker volume rm p1-minio-data p1-pg-data`.

## Структура репозитория

```
practice1/
├── README.md                 — этот файл
├── отчет.md                  — заполненная форма отчёта
├── catalog/                  — каталоги Trino (монтируются в /etc/trino/catalog)
│   ├── bronze.properties  silver.properties  gold.properties  bench.properties
│   └── tpch.properties       — генератор TPC-H (connector.name=tpch)
├── scripts/
│   ├── 01_stack.sh           — все docker run стека с комментариями
│   ├── create-warehouse-{bronze,silver,gold,bench}.json — регистрация warehouse в Lakekeeper
│   ├── benchmark.py          — бенчмарк 2 формата × 4 кодека
│   ├── schema_evolution_metadata.sh — схемы с ID колонок из metadata.json
│   ├── time_travel.sh        — сценарий "авария → восстановление"
│   ├── 03_spark.sh           — Spark поверх Bronze
│   └── 04_parallel.sh        — 10 потоков Trino и Spark, Trino vs Spark
├── sql/
│   ├── 00_layer_stats.sql    — строки / файлы / МБ / формат / кодек по всем таблицам
│   ├── 01_bronze.sql  02_silver.sql  03_gold.sql
│   ├── 04_benchmark.sql      — SQL бенчмарка для одного варианта (скрипт гоняет все 8)
│   ├── 05_schema_evolution.sql
│   └── 06_time_travel.sql
├── spark/
│   ├── conf/                 — spark-defaults.conf (каталог bronze → Lakekeeper), log4j2
│   ├── bronze_spark.sql      — запросы Spark к Bronze
│   ├── tpch_run.py           — драйвер потоков из laba2 (+ параметры каталога)
│   └── queries_q10/q10.sql   — мой запрос для драйвера
└── results/                  — сырые выводы и csv всех замеров
```

В git не кладу данные, jar'ы (`spark/jars/`, качаются скриптом) и логи. Пароли в конфигах — только демо `minioadmin` / `postgres` из гайда.
