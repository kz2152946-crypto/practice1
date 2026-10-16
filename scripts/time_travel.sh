#!/usr/bin/env bash
# Time travel на silver.lakehouse.dim_customer: запоминаю текущий снапшот и время,
# подставляю их в sql/06_time_travel.sql и выполняю.
set -euo pipefail
cd "$(dirname "$0")/.."
q() { docker exec trino trino --output-format TSV --execute "$1" 2>/dev/null; }

GOOD=$(q "SELECT snapshot_id FROM silver.lakehouse.\"dim_customer\$history\" WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1")
sleep 2
TS=$(q "SELECT format_datetime(current_timestamp AT TIME ZONE 'UTC', 'yyyy-MM-dd HH:mm:ss.SSS') || ' UTC'")
sleep 2
echo "GOOD=$GOOD TS=$TS"
sed -e "s/\${GOOD}/$GOOD/g" -e "s/\${TS}/$TS/g" sql/06_time_travel.sql \
  | docker exec -i trino trino --output-format ALIGNED 2>&1 | grep -v -e jline -e "dumb terminal"
