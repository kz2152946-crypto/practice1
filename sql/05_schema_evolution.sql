-- Schema evolution на Silver (копия dim_customer, чтобы не ломать сборку Gold).
-- Запуск: docker exec -i trino trino --output-format ALIGNED < sql/05_schema_evolution.sql

DROP TABLE IF EXISTS silver.lakehouse.dim_customer_evo;
CREATE TABLE silver.lakehouse.dim_customer_evo
WITH (format = 'PARQUET')
AS SELECT customer_key, customer_name, acct_balance, market_segment, nation_name
   FROM silver.lakehouse.dim_customer;

-- 0. Состояние ДО: схема, файлы, снапшоты
DESCRIBE silver.lakehouse.dim_customer_evo;
SELECT file_path, record_count, file_size_in_bytes FROM silver.lakehouse."dim_customer_evo$files";
SELECT snapshot_id, operation, committed_at FROM silver.lakehouse."dim_customer_evo$snapshots" ORDER BY committed_at;

-- 1. ADD COLUMN: новая колонка, старые строки её не имеют
ALTER TABLE silver.lakehouse.dim_customer_evo ADD COLUMN loyalty_tier varchar;

-- 2. RENAME COLUMN: меняется только имя в метаданных, ID колонки тот же
ALTER TABLE silver.lakehouse.dim_customer_evo RENAME COLUMN market_segment TO segment;

-- 3. Widen: decimal(12,2) -> decimal(16,2) (Iceberg разрешает увеличивать precision)
ALTER TABLE silver.lakehouse.dim_customer_evo ALTER COLUMN acct_balance SET DATA TYPE decimal(16,2);

DESCRIBE silver.lakehouse.dim_customer_evo;

-- 4. Файлы и снапшоты ПОСЛЕ ALTER: файлы те же, новых снапшотов нет (меняется только metadata.json)
SELECT file_path, record_count, file_size_in_bytes FROM silver.lakehouse."dim_customer_evo$files";
SELECT snapshot_id, operation, committed_at FROM silver.lakehouse."dim_customer_evo$snapshots" ORDER BY committed_at;
SELECT timestamp, latest_snapshot_id, latest_schema_id, file
FROM silver.lakehouse."dim_customer_evo$metadata_log_entries" ORDER BY timestamp;

-- 5. Новые строки пишутся уже по новой схеме
INSERT INTO silver.lakehouse.dim_customer_evo
VALUES (9999991, 'Customer#NEW', 12345678901234.56, 'BUILDING', 'RUSSIA', 'GOLD');

-- старые строки: loyalty_tier = NULL, переименованная колонка читается со старыми значениями
SELECT customer_key, customer_name, acct_balance, segment, loyalty_tier
FROM silver.lakehouse.dim_customer_evo
WHERE customer_key IN (1, 2, 9999991) ORDER BY customer_key;

SELECT count(*) AS rows_total, count(loyalty_tier) AS rows_with_tier, count(segment) AS rows_with_segment
FROM silver.lakehouse.dim_customer_evo;

-- старый файл остался, добавился один новый (append)
SELECT file_path, record_count FROM silver.lakehouse."dim_customer_evo$files";
SELECT snapshot_id, operation, committed_at FROM silver.lakehouse."dim_customer_evo$snapshots" ORDER BY committed_at;

-- 6. Чтение первого снапшота: Trino читает его по схеме того снапшота (старые имена колонок).
--    FOR VERSION AS OF принимает только литерал —
--    id подставляет scripts/schema_evolution_metadata.sh):
-- SELECT * FROM silver.lakehouse.dim_customer_evo FOR VERSION AS OF <id первого снапшота>;
