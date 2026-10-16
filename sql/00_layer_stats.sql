-- Характеристики таблиц всех слоёв: строки, файлы, МБ (по "$files"), формат и кодек (по "$properties").
-- Запуск: docker exec -i trino trino --output-format CSV_HEADER < sql/00_layer_stats.sql > results/layers_stats.csv
WITH t(layer, tbl, rows_cnt, files, bytes, fmt, codec, part) AS (
  SELECT 'bronze','nation',(SELECT count(*) FROM bronze.lakehouse.nation),(SELECT count(*) FROM bronze.lakehouse."nation$files"),(SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."nation$files"),
         (SELECT value FROM bronze.lakehouse."nation$properties" WHERE key='write.format.default'),(SELECT value FROM bronze.lakehouse."nation$properties" WHERE key LIKE 'write.%.compression-codec'),'нет'
  UNION ALL SELECT 'bronze','customer',(SELECT count(*) FROM bronze.lakehouse.customer),(SELECT count(*) FROM bronze.lakehouse."customer$files"),(SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."customer$files"),
         (SELECT value FROM bronze.lakehouse."customer$properties" WHERE key='write.format.default'),(SELECT value FROM bronze.lakehouse."customer$properties" WHERE key LIKE 'write.%.compression-codec'),'нет'
  UNION ALL SELECT 'bronze','orders',(SELECT count(*) FROM bronze.lakehouse.orders),(SELECT count(*) FROM bronze.lakehouse."orders$files"),(SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."orders$files"),
         (SELECT value FROM bronze.lakehouse."orders$properties" WHERE key='write.format.default'),(SELECT value FROM bronze.lakehouse."orders$properties" WHERE key LIKE 'write.%.compression-codec'),'year(orderdate)'
  UNION ALL SELECT 'bronze','lineitem',(SELECT count(*) FROM bronze.lakehouse.lineitem),(SELECT count(*) FROM bronze.lakehouse."lineitem$files"),(SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."lineitem$files"),
         (SELECT value FROM bronze.lakehouse."lineitem$properties" WHERE key='write.format.default'),(SELECT value FROM bronze.lakehouse."lineitem$properties" WHERE key LIKE 'write.%.compression-codec'),'нет'
  UNION ALL SELECT 'silver','dim_customer',(SELECT count(*) FROM silver.lakehouse.dim_customer),(SELECT count(*) FROM silver.lakehouse."dim_customer$files"),(SELECT sum(file_size_in_bytes) FROM silver.lakehouse."dim_customer$files"),
         (SELECT value FROM silver.lakehouse."dim_customer$properties" WHERE key='write.format.default'),(SELECT value FROM silver.lakehouse."dim_customer$properties" WHERE key LIKE 'write.%.compression-codec'),'нет'
  UNION ALL SELECT 'silver','fct_order_line',(SELECT count(*) FROM silver.lakehouse.fct_order_line),(SELECT count(*) FROM silver.lakehouse."fct_order_line$files"),(SELECT sum(file_size_in_bytes) FROM silver.lakehouse."fct_order_line$files"),
         (SELECT value FROM silver.lakehouse."fct_order_line$properties" WHERE key='write.format.default'),(SELECT value FROM silver.lakehouse."fct_order_line$properties" WHERE key LIKE 'write.%.compression-codec'),'month(order_date)'
  UNION ALL SELECT 'gold','top20_returned_customers_q4_1993',(SELECT count(*) FROM gold.lakehouse.top20_returned_customers_q4_1993),(SELECT count(*) FROM gold.lakehouse."top20_returned_customers_q4_1993$files"),(SELECT sum(file_size_in_bytes) FROM gold.lakehouse."top20_returned_customers_q4_1993$files"),
         (SELECT value FROM gold.lakehouse."top20_returned_customers_q4_1993$properties" WHERE key='write.format.default'),(SELECT value FROM gold.lakehouse."top20_returned_customers_q4_1993$properties" WHERE key LIKE 'write.%.compression-codec'),'нет'
)
SELECT layer, tbl AS table_name, rows_cnt AS rows, files, bytes, round(bytes / 1048576e0, 3) AS mb, fmt AS format, codec, part AS partitioning FROM t;
