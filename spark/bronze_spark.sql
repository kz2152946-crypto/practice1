-- Spark SQL поверх Bronze: Spark сам ходит в Lakekeeper за метаданными и читает parquet из MinIO.
-- Запуск: см. scripts/03_spark.sh
SHOW NAMESPACES IN bronze;
SHOW TABLES IN bronze.lakehouse;
DESCRIBE TABLE bronze.lakehouse.lineitem;

SELECT 'nation' AS tbl, count(*) AS cnt FROM bronze.lakehouse.nation
UNION ALL SELECT 'customer', count(*) FROM bronze.lakehouse.customer
UNION ALL SELECT 'orders',   count(*) FROM bronze.lakehouse.orders
UNION ALL SELECT 'lineitem', count(*) FROM bronze.lakehouse.lineitem;

-- метаданные Iceberg глазами Spark: снапшоты и файлы
SELECT snapshot_id, operation, committed_at FROM bronze.lakehouse.lineitem.snapshots;
SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes FROM bronze.lakehouse.lineitem.files;

-- простой запрос: возвраты по годам заказа
SELECT year(o.orderdate) AS order_year,
       count(*) AS returned_lines,
       round(sum(l.extendedprice * (1 - l.discount)), 2) AS returned_revenue
FROM bronze.lakehouse.lineitem l
JOIN bronze.lakehouse.orders o ON o.orderkey = l.orderkey
WHERE l.returnflag = 'R'
GROUP BY year(o.orderdate)
ORDER BY order_year;

-- мой вариант (Q10) на Bronze — должен совпасть с Gold
SELECT c.custkey, c.name, round(sum(l.extendedprice * (1 - l.discount)), 4) AS revenue,
       c.acctbal, n.name AS nation
FROM bronze.lakehouse.customer c
JOIN bronze.lakehouse.orders o ON c.custkey = o.custkey
JOIN bronze.lakehouse.lineitem l ON l.orderkey = o.orderkey
JOIN bronze.lakehouse.nation n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;
