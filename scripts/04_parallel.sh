#!/usr/bin/env bash
# Бонус: мой Q10 по Bronze в 1 и 10 потоков на Trino и на Spark (гайд 3, §11.1, §13.10).
# Каждый поток выполняет запрос 3 раза подряд. Вывод: results/parallel_*.txt, results/spark_q10_streams*.csv
set -euo pipefail
cd "$(dirname "$0")/.."
TIMEFORMAT=%R
Q=$(sed -e 's/FROM customer c/FROM bronze.lakehouse.customer c/' -e 's/JOIN orders o/JOIN bronze.lakehouse.orders o/' \
        -e 's/JOIN lineitem l/JOIN bronze.lakehouse.lineitem l/' -e 's/JOIN nation n/JOIN bronze.lakehouse.nation n/' \
        spark/queries_q10/q10_run1.sql)

run3() { for r in 1 2 3; do docker exec trino trino --source "par_$1_$r" --execute "$Q" >/dev/null 2>&1; done; }

{
  echo "== Trino, 1 поток x 3 запроса"
  time run3 s1
  echo "== Trino, 10 потоков x 3 запроса"
  time ( for i in $(seq 1 10); do run3 "s10_$i" & done; wait )
  echo "== exec_ms по запросам (system.runtime.queries)"
  docker exec trino trino --output-format ALIGNED --execute \
    "SELECT CASE WHEN source LIKE 'par_s1_%' THEN '1 поток' ELSE '10 потоков' END AS mode,
            count(*) AS queries, min(date_diff('millisecond', started, \"end\")) AS min_ms,
            approx_percentile(date_diff('millisecond', started, \"end\"), 0.5) AS median_ms,
            max(date_diff('millisecond', started, \"end\")) AS max_ms
     FROM system.runtime.queries WHERE source LIKE 'par_%' AND state = 'FINISHED'
     GROUP BY 1 ORDER BY 1" 2>/dev/null
} 2>&1 | tee results/parallel_trino.txt

# Spark: тот же запрос, драйвер tpch_run.py из laba2
for s in 1 10; do
  docker run --rm \
    --add-host host.docker.internal:host-gateway \
    -v "$(pwd)/spark/conf:/opt/spark/conf" \
    -v "$(pwd)/spark/jars:/opt/spark/iceberg-jars" \
    -v "$(pwd)/spark:/opt/spark/work" \
    apache/spark:3.5.3 \
    /opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/work/tpch_run.py \
      --catalog bronze --database lakehouse \
      --queries /opt/spark/work/queries_q10 --streams "$s" \
      --out "/opt/spark/work/spark_q10_streams$s.csv" 2>&1 | grep -v -e WARN -e '^\s*$' | tee "results/parallel_spark_streams$s.txt"
  mv "spark/spark_q10_streams$s.csv" results/
done
