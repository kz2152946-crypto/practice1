-- Gold: витрина под вариант 2 (TPC-H Q10 "Returned Item Reporting") —
-- топ-20 клиентов по выручке от возвращённых позиций за Q4 1993.
-- Запуск: docker exec -i trino trino --output-format ALIGNED < sql/03_gold.sql

SET SESSION gold.compression_codec = 'ZSTD';

DROP TABLE IF EXISTS gold.lakehouse.top20_returned_customers_q4_1993;
CREATE TABLE gold.lakehouse.top20_returned_customers_q4_1993
WITH (format = 'PARQUET')
AS
SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC) AS revenue_rank,
       c.customer_key,
       c.customer_name,
       CAST(sum(f.net_revenue) AS decimal(18,4))         AS revenue,
       c.acct_balance,
       c.nation_name,
       c.address,
       c.phone,
       c.comment
FROM silver.lakehouse.fct_order_line f
JOIN silver.lakehouse.dim_customer c ON c.customer_key = f.customer_key
WHERE f.order_date >= DATE '1993-10-01'
  AND f.order_date <  DATE '1994-01-01'
  AND f.return_flag = 'R'
GROUP BY c.customer_key, c.customer_name, c.acct_balance, c.nation_name,
         c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;

SELECT * FROM gold.lakehouse.top20_returned_customers_q4_1993 ORDER BY revenue_rank;

-- ---------- Проверка: Gold == эталонный Q10 напрямую по tpch.sf1 ----------
-- Эталон приводим к тем же типам/очистке, что в Silver:
-- revenue double -> decimal (до копеек), acctbal -> decimal(12,2), trim(address/comment).
CREATE OR REPLACE VIEW gold.lakehouse.v_q10_reference AS
SELECT c.custkey                                                   AS customer_key,
       c.name                                                      AS customer_name,
       sum(l.extendedprice * (1 - l.discount))                     AS revenue_double,
       CAST(c.acctbal AS decimal(12,2))                            AS acct_balance,
       n.name                                                      AS nation_name,
       trim(c.address)                                             AS address,
       c.phone,
       trim(c.comment)                                             AS comment
FROM tpch.sf1.customer AS c
JOIN tpch.sf1.orders   AS o ON c.custkey = o.custkey
JOIN tpch.sf1.lineitem AS l ON l.orderkey = o.orderkey
JOIN tpch.sf1.nation   AS n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01'
  AND o.orderdate <  DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue_double DESC
LIMIT 20;

-- Gold EXCEPT эталон (должно быть 0 строк)
SELECT 'gold_minus_ref' AS check_name, count(*) AS diff_rows FROM (
  SELECT customer_key, customer_name, CAST(revenue AS decimal(18,2)), acct_balance,
         nation_name, address, phone, comment
  FROM gold.lakehouse.top20_returned_customers_q4_1993
  EXCEPT
  SELECT customer_key, customer_name, CAST(round(revenue_double, 2) AS decimal(18,2)), acct_balance,
         nation_name, address, phone, comment
  FROM gold.lakehouse.v_q10_reference
)
UNION ALL
-- эталон EXCEPT Gold (должно быть 0 строк)
SELECT 'ref_minus_gold', count(*) FROM (
  SELECT customer_key, customer_name, CAST(round(revenue_double, 2) AS decimal(18,2)), acct_balance,
         nation_name, address, phone, comment
  FROM gold.lakehouse.v_q10_reference
  EXCEPT
  SELECT customer_key, customer_name, CAST(revenue AS decimal(18,2)), acct_balance,
         nation_name, address, phone, comment
  FROM gold.lakehouse.top20_returned_customers_q4_1993
);

-- насколько расходятся точная decimal-сумма и double-сумма эталона
SELECT max(abs(CAST(g.revenue AS double) - r.revenue_double)) AS max_abs_diff
FROM gold.lakehouse.top20_returned_customers_q4_1993 g
JOIN gold.lakehouse.v_q10_reference r USING (customer_key);
