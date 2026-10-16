#!/usr/bin/env bash
# Подъём стека MinIO(silo) -> Postgres -> Lakekeeper -> Trino по гайду 1 (docker run, без compose).
# Запускать из корня репозитория: bash scripts/01_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Postgres на хосте проброшен на 5433, потому что 5432 у меня занят локальным Postgres (гайд 1, §8)
PG_URL='postgresql://postgres:postgres@host.docker.internal:5433/postgres'
LK_IMAGE='quay.io/lakekeeper/catalog:latest-main'

# 1. MinIO — объектное хранилище (форк silo, quay.io/minio отдаёт 401). 9000 — S3 API, 9001 — консоль
docker run -d --name lakehouse-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v p1-minio-data:/data \
  pgsty/silo:latest server /data --console-address ":9001"

# 2. Postgres — тут Lakekeeper хранит метаданные каталога
docker run -d --name lakehouse-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -v p1-pg-data:/var/lib/postgresql/data \
  postgres:17

until docker exec lakehouse-pg pg_isready -q; do sleep 1; done

# 3. Миграции БД Lakekeeper (одноразово)
docker run --rm \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG_URL" \
  -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG_URL" \
  "$LK_IMAGE" migrate

# 4. Lakekeeper — Iceberg REST Catalog на 8181
docker run -d --name lakekeeper \
  -e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!' \
  -e "LAKEKEEPER__PG_DATABASE_URL_READ=$PG_URL" \
  -e "LAKEKEEPER__PG_DATABASE_URL_WRITE=$PG_URL" \
  -p 8181:8181 \
  "$LK_IMAGE" serve

until curl -sf http://localhost:8181/health >/dev/null; do sleep 1; done

# 5. Бакеты в MinIO: по одному на слой + bench для экспериментов
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway \
  pgsty/mc:latest -c "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && \
    for b in bronze silver gold bench; do mc mb --ignore-existing m/\$b; done"

# 6. Принять Terms of Use и зарегистрировать 4 warehouse (bronze/silver/gold/bench)
curl -s -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" -d '{"accept-terms-of-use": true}' || true
for w in bronze silver gold bench; do
  curl -s -X POST http://localhost:8181/management/v1/warehouse \
    -H "Content-Type: application/json" -d @"scripts/create-warehouse-$w.json"
  echo
done

# 7. Trino — SQL-движок, каталоги берёт из папки catalog/ (bronze, silver, gold, bench, tpch)
docker run -d --name trino \
  -p 8080:8080 \
  -v "$(pwd)/catalog:/etc/trino/catalog" \
  trinodb/trino:476

echo "ждём Trino..."
until docker exec trino trino --execute "SELECT 1" >/dev/null 2>&1; do sleep 3; done
docker exec trino trino --execute "SHOW CATALOGS"
