-- Bronze: сырые таблицы tpch.sf1 "как есть" + служебные поля loaded_at, source.
-- Формат PARQUET, кодек ZSTD (задаётся сессией), orders партиционирую по году заказа.
-- Запуск: docker exec -i trino trino < sql/01_bronze.sql

CREATE SCHEMA IF NOT EXISTS bronze.lakehouse;
CREATE SCHEMA IF NOT EXISTS silver.lakehouse;
CREATE SCHEMA IF NOT EXISTS gold.lakehouse;

SET SESSION bronze.compression_codec = 'ZSTD';

DROP TABLE IF EXISTS bronze.lakehouse.nation;
CREATE TABLE bronze.lakehouse.nation
WITH (format = 'PARQUET')
AS SELECT n.*,
          CAST(current_timestamp AS timestamp(6) with time zone) AS loaded_at,
          'tpch.sf1.nation' AS source
   FROM tpch.sf1.nation n;

DROP TABLE IF EXISTS bronze.lakehouse.customer;
CREATE TABLE bronze.lakehouse.customer
WITH (format = 'PARQUET')
AS SELECT c.*,
          CAST(current_timestamp AS timestamp(6) with time zone) AS loaded_at,
          'tpch.sf1.customer' AS source
   FROM tpch.sf1.customer c;

DROP TABLE IF EXISTS bronze.lakehouse.orders;
CREATE TABLE bronze.lakehouse.orders
WITH (format = 'PARQUET', partitioning = ARRAY['year(orderdate)'])
AS SELECT o.*,
          CAST(current_timestamp AS timestamp(6) with time zone) AS loaded_at,
          'tpch.sf1.orders' AS source
   FROM tpch.sf1.orders o;

DROP TABLE IF EXISTS bronze.lakehouse.lineitem;
CREATE TABLE bronze.lakehouse.lineitem
WITH (format = 'PARQUET')
AS SELECT l.*,
          CAST(current_timestamp AS timestamp(6) with time zone) AS loaded_at,
          'tpch.sf1.lineitem' AS source
   FROM tpch.sf1.lineitem l;
