-- Silver: очистка, нормализация типов/названий, обогащение.
-- Запуск: docker exec -i trino trino --output-format ALIGNED < sql/02_silver.sql

-- ---------- 1. Проверки качества Bronze (до преобразования) ----------

-- NULL в ключах и важных полях
SELECT 'customer' AS tbl,
       count_if(custkey IS NULL) AS null_keys,
       count_if(nationkey IS NULL) AS null_fk,
       count(*) - count(DISTINCT custkey) AS dup_keys
FROM bronze.lakehouse.customer
UNION ALL
SELECT 'orders', count_if(orderkey IS NULL), count_if(custkey IS NULL OR orderdate IS NULL),
       count(*) - count(DISTINCT orderkey)
FROM bronze.lakehouse.orders
UNION ALL
SELECT 'lineitem', count_if(orderkey IS NULL OR linenumber IS NULL),
       count_if(extendedprice IS NULL OR discount IS NULL OR returnflag IS NULL),
       count(*) - count(DISTINCT (orderkey, linenumber))
FROM bronze.lakehouse.lineitem
UNION ALL
SELECT 'nation', count_if(nationkey IS NULL), count_if(name IS NULL),
       count(*) - count(DISTINCT nationkey)
FROM bronze.lakehouse.nation;

-- допустимые значения
SELECT returnflag, count(*) AS cnt FROM bronze.lakehouse.lineitem GROUP BY 1 ORDER BY 1;

SELECT count_if(discount NOT BETWEEN 0 AND 1)  AS bad_discount,
       count_if(extendedprice <= 0)            AS bad_price,
       count_if(quantity <= 0)                 AS bad_qty
FROM bronze.lakehouse.lineitem;

-- лишние пробелы в строках и "сироты" в связях
SELECT count_if(address <> trim(address)) AS untrimmed_address,
       count_if(comment <> trim(comment)) AS untrimmed_comment,
       count_if(name <> trim(name) OR phone <> trim(phone)) AS untrimmed_name_phone,
       count_if(nationkey NOT IN (SELECT nationkey FROM bronze.lakehouse.nation))     AS orphan_nation
FROM bronze.lakehouse.customer;

SELECT count(*) AS orphan_lines
FROM bronze.lakehouse.lineitem l
LEFT JOIN bronze.lakehouse.orders o ON o.orderkey = l.orderkey
WHERE o.orderkey IS NULL;

-- ---------- 2. Silver-таблицы ----------

SET SESSION silver.compression_codec = 'ZSTD';

-- Клиент + страна. trim строк, деньги в decimal, единые имена колонок,
-- дедуп по custkey (берём последнюю загрузку), отсев строк без ключа/страны.
DROP TABLE IF EXISTS silver.lakehouse.dim_customer;
CREATE TABLE silver.lakehouse.dim_customer
WITH (format = 'PARQUET')
AS
WITH c AS (
  SELECT *, row_number() OVER (PARTITION BY custkey ORDER BY loaded_at DESC) AS rn
  FROM bronze.lakehouse.customer
  WHERE custkey IS NOT NULL
)
SELECT c.custkey                               AS customer_key,
       trim(c.name)                            AS customer_name,
       trim(c.address)                         AS address,
       trim(c.phone)                           AS phone,
       CAST(c.acctbal AS decimal(12,2))        AS acct_balance,
       upper(trim(c.mktsegment))               AS market_segment,
       trim(c.comment)                         AS comment,
       n.nationkey                             AS nation_key,
       upper(trim(n.name))                     AS nation_name,
       current_timestamp                       AS processed_at
FROM c
JOIN bronze.lakehouse.nation n ON n.nationkey = c.nationkey
WHERE c.rn = 1;

-- Строки заказов, обогащённые датой заказа и клиентом, с выручкой строки.
-- Отсеиваю: строки без заказа, невалидные returnflag/discount/price, дубли (orderkey, linenumber).
DROP TABLE IF EXISTS silver.lakehouse.fct_order_line;
CREATE TABLE silver.lakehouse.fct_order_line
WITH (format = 'PARQUET', partitioning = ARRAY['month(order_date)'])
AS
WITH l AS (
  SELECT *, row_number() OVER (PARTITION BY orderkey, linenumber ORDER BY loaded_at DESC) AS rn
  FROM bronze.lakehouse.lineitem
  WHERE orderkey IS NOT NULL
    AND returnflag IN ('R', 'A', 'N')
    AND discount BETWEEN 0 AND 1
    AND extendedprice > 0
)
SELECT l.orderkey                                           AS order_key,
       CAST(l.linenumber AS integer)                        AS line_number,
       o.custkey                                            AS customer_key,
       o.orderdate                                          AS order_date,
       upper(trim(o.orderstatus))                           AS order_status,
       upper(trim(l.returnflag))                            AS return_flag,
       upper(trim(l.linestatus))                            AS line_status,
       CAST(l.quantity AS decimal(12,2))                    AS quantity,
       CAST(l.extendedprice AS decimal(12,2))               AS extended_price,
       CAST(l.discount AS decimal(4,2))                     AS discount,
       CAST(l.tax AS decimal(4,2))                          AS tax,
       CAST(CAST(l.extendedprice AS decimal(12,2))
         * (1 - CAST(l.discount AS decimal(4,2))) AS decimal(18,4)) AS net_revenue,
       l.shipdate                                           AS ship_date,
       current_timestamp                                    AS processed_at
FROM l
JOIN bronze.lakehouse.orders o ON o.orderkey = l.orderkey
WHERE l.rn = 1
  AND o.custkey IS NOT NULL
  AND o.orderdate IS NOT NULL;

-- ---------- 3. Проверки после ----------
SELECT 'dim_customer' AS tbl, count(*) AS rows FROM silver.lakehouse.dim_customer
UNION ALL SELECT 'fct_order_line', count(*) FROM silver.lakehouse.fct_order_line;

SELECT typeof(net_revenue) AS revenue_type, count(*) AS rows_q4_1993_r
FROM silver.lakehouse.fct_order_line
WHERE order_date >= DATE '1993-10-01' AND order_date < DATE '1994-01-01' AND return_flag = 'R'
GROUP BY 1;
