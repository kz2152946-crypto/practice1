#!/usr/bin/env bash
# Схемы с ID колонок из последнего metadata.json таблицы dim_customer_evo (в MinIO)
# и чтение первого снапшота после эволюции схемы.
set -euo pipefail
q() { docker exec trino trino --output-format TSV --execute "$1" 2>/dev/null; }

META=$(q "SELECT file FROM silver.lakehouse.\"dim_customer_evo\$metadata_log_entries\" ORDER BY timestamp DESC LIMIT 1")
echo "metadata: $META"
docker run --rm --entrypoint /bin/sh --add-host host.docker.internal:host-gateway pgsty/mc:latest -c \
  "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin >/dev/null && mc cat m/${META#s3://}" \
  | gunzip | python3 -c '
import json, sys
m = json.load(sys.stdin)
print("current-schema-id:", m["current-schema-id"])
for s in m["schemas"]:
    cols = ", ".join("%s:%s:%s" % (f["id"], f["name"], f["type"]) for f in s["fields"])
    print("schema %s: %s" % (s["schema-id"], cols))
'

FIRST=$(q "SELECT snapshot_id FROM silver.lakehouse.\"dim_customer_evo\$snapshots\" ORDER BY committed_at LIMIT 1")
echo "первый снапшот: $FIRST — Trino читает его по схеме того снапшота (schema 0):"
docker exec trino trino --output-format ALIGNED --execute \
  "SELECT count(*) AS rows, min(market_segment) AS min_market_segment, typeof(min(acct_balance)) AS balance_type
   FROM silver.lakehouse.dim_customer_evo FOR VERSION AS OF $FIRST" 2>&1 | grep -v -e jline -e "dumb terminal"
echo "то же для текущего снапшота: колонки segment и loyalty_tier есть, market_segment нет"
docker exec trino trino --output-format ALIGNED --execute \
  "SELECT count(*) AS rows, count(loyalty_tier) AS with_tier, typeof(min(acct_balance)) AS balance_type FROM silver.lakehouse.dim_customer_evo" 2>/dev/null
