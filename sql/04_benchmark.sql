-- Бенчмарк форматов и кодеков на Bronze (гайд 3). Пример для одного варианта: parquet_zstd.
-- Все 8 вариантов (PARQUET/ORC x NONE/SNAPPY/GZIP/ZSTD) прогоняет scripts/benchmark.py
-- теми же командами: меняется только кодек в SET SESSION, format в WITH и имя схемы.

CREATE SCHEMA IF NOT EXISTS bench.parquet_zstd;

-- 1. Запись (время: time docker exec trino trino --execute "...")
SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.parquet_zstd.lineitem WITH (format = 'PARQUET')
AS SELECT * FROM tpch.sf1.lineitem;

SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.parquet_zstd.orders WITH (format = 'PARQUET', partitioning = ARRAY['year(orderdate)'])
AS SELECT * FROM tpch.sf1.orders;

SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.parquet_zstd.customer WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.customer;
SET SESSION bench.compression_codec = 'ZSTD';
CREATE TABLE bench.parquet_zstd.nation   WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf1.nation;

-- 2. Размер и кодек
SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes FROM bench.parquet_zstd."lineitem$files";
SELECT key, value FROM bench.parquet_zstd."lineitem$properties"
WHERE key LIKE 'write.%.compression-codec' OR key = 'write.format.default';

-- 3. Мой запрос (Q10). cold — сразу после docker restart trino lakehouse-minio, warm — повторы
SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, n.name AS nation, c.address, c.phone, c.comment
FROM bench.parquet_zstd.customer c
JOIN bench.parquet_zstd.orders o ON c.custkey = o.custkey
JOIN bench.parquet_zstd.lineitem l ON l.orderkey = o.orderkey
JOIN bench.parquet_zstd.nation n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC LIMIT 20;

-- 4. Агрегация Q1 по всей lineitem (полный скан — кодек виден сильнее всего)
SELECT returnflag, linestatus, count(*) AS cnt, sum(quantity) AS sum_qty,
       sum(extendedprice) AS sum_price, sum(discount) AS sum_disc,
       sum(extendedprice * (1 - discount)) AS sum_charge
FROM bench.parquet_zstd.lineitem GROUP BY returnflag, linestatus;

-- 5. Время запросов из Trino (скрипт помечает каждый запрос через --source)
SELECT source, date_diff('millisecond', started, "end") AS exec_ms, query
FROM system.runtime.queries
WHERE source LIKE 'q10_%' AND state = 'FINISHED'
ORDER BY created DESC;
