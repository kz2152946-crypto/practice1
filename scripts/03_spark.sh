#!/usr/bin/env bash
# Spark 3.5.3 в Docker читает Bronze напрямую через Lakekeeper REST + MinIO (гайд 3, §11.4).
# Jar'ы Iceberg один раз качаются в spark/jars (в git не кладу).
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p spark/jars
[ -f spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar ] || curl -fsSL -o spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
[ -f spark/jars/iceberg-aws-bundle-1.7.1.jar ] || curl -fsSL -o spark/jars/iceberg-aws-bundle-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

# разовый прогон spark/bronze_spark.sql, вывод в results/spark_bronze.txt
docker run --rm \
  --add-host host.docker.internal:host-gateway \
  -v "$(pwd)/spark/conf:/opt/spark/conf" \
  -v "$(pwd)/spark/jars:/opt/spark/iceberg-jars" \
  -v "$(pwd)/spark:/opt/spark/work" \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-sql -f /opt/spark/work/bronze_spark.sql \
  2>&1 | tee results/spark_bronze.txt
