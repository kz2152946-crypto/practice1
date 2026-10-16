#!/usr/bin/env python3
"""
Бенчмарк форматов и кодеков на Bronze (гайд 3): 2 формата x 4 кодека.

Для каждого варианта создаётся схема bench.<format>_<codec> с таблицами
lineitem, orders, customer, nation из tpch.sf1 (кодек через SET SESSION).
Меряю:
  - время записи (CTAS lineitem и orders, 2 прогона),
  - размер по "$files",
  - время Q10 (мой вариант) и Q1 (агрегация по lineitem): cold после
    docker restart trino lakehouse-minio, затем 3 warm-прогона.
Время: wall (как `time docker exec ...`) и exec_ms из system.runtime.queries.

Запуск из корня репо: python3 scripts/benchmark.py
Результат: results/benchmark.csv (сырые замеры), results/benchmark_summary.csv
"""
import csv
import subprocess
import time
from statistics import median

SF = "sf1"
FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "GZIP", "ZSTD"]
WRITE_RUNS = 2
WARM_RUNS = 3

Q10 = """SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, n.name AS nation, c.address, c.phone, c.comment
FROM bench.{s}.customer c
JOIN bench.{s}.orders o ON c.custkey = o.custkey
JOIN bench.{s}.lineitem l ON l.orderkey = o.orderkey
JOIN bench.{s}.nation n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC LIMIT 20"""

Q1 = """SELECT returnflag, linestatus, count(*) AS cnt, sum(quantity) AS sum_qty,
       sum(extendedprice) AS sum_price, sum(discount) AS sum_disc,
       sum(extendedprice * (1 - discount)) AS sum_charge
FROM bench.{s}.lineitem GROUP BY returnflag, linestatus"""

PARTITIONING = {"orders": ", partitioning = ARRAY['year(orderdate)']"}


def trino(sql, source="bench", fmt="CSV"):
    """docker exec trino trino --execute ...; возвращает (wall_s, stdout)."""
    t0 = time.perf_counter()
    p = subprocess.run(
        ["docker", "exec", "trino", "trino", "--source", source,
         "--output-format", fmt, "--execute", sql],
        capture_output=True, text=True)
    wall = time.perf_counter() - t0
    if p.returncode != 0:
        raise RuntimeError(f"{sql[:80]}...\n{p.stderr}")
    return wall, p.stdout.strip()


def wait_trino():
    while True:
        try:
            trino("SELECT 1", source="ping")
            return
        except RuntimeError:
            time.sleep(3)


def restart_stack():
    subprocess.run(["docker", "restart", "trino", "lakehouse-minio"],
                   check=True, capture_output=True)
    wait_trino()


def exec_ms(source):
    """Время выполнения последнего запроса с данным --source из system.runtime.queries."""
    _, out = trino(
        "SELECT date_diff('millisecond', started, \"end\") FROM system.runtime.queries "
        f"WHERE source = '{source}' AND state = 'FINISHED' ORDER BY created DESC LIMIT 1",
        source="meta")
    return int(out.strip('"')) if out else None


def main():
    rows = []  # variant, format, codec, metric, object, run, value, unit

    def add(v, f, c, metric, obj, run, value, unit):
        rows.append([v, f, c, metric, obj, run, value, unit])
        print(f"{v:15} {metric:10} {obj:9} {run:6} {value} {unit}", flush=True)

    wait_trino()
    variants = [(f, c) for f in FORMATS for c in CODECS]

    # 1. Запись + размер
    for f, c in variants:
        s = f"{f.lower()}_{c.lower()}"
        trino(f"CREATE SCHEMA IF NOT EXISTS bench.{s}")
        for tbl in ["nation", "customer", "orders", "lineitem"]:
            runs = WRITE_RUNS if tbl in ("orders", "lineitem") else 1
            for r in range(1, runs + 1):
                trino(f"DROP TABLE IF EXISTS bench.{s}.{tbl}")
                src = f"write_{s}_{tbl}_{r}"
                wall, _ = trino(
                    f"SET SESSION bench.compression_codec = '{c}'; "
                    f"CREATE TABLE bench.{s}.{tbl} WITH (format = '{f}'{PARTITIONING.get(tbl, '')}) "
                    f"AS SELECT * FROM tpch.{SF}.{tbl}", source=src)
                if tbl in ("orders", "lineitem"):
                    add(s, f, c, "write_wall", tbl, r, round(wall, 3), "s")
                    add(s, f, c, "write_exec", tbl, r, exec_ms(src), "ms")
            _, out = trino(f'SELECT count(*), sum(file_size_in_bytes) FROM bench.{s}."{tbl}$files"')
            files, size = [int(x.strip('"')) for x in out.split(",")]
            add(s, f, c, "files", tbl, "", files, "count")
            add(s, f, c, "size", tbl, "", size, "bytes")
        _, out = trino(f"SELECT value FROM bench.{s}.\"lineitem$properties\" "
                       "WHERE key LIKE 'write.%.compression-codec'")
        add(s, f, c, "codec_prop", "lineitem", "", out.strip('"'), "")

    # 2. Запросы: cold (после рестарта) + warm
    for qname, qsql in [("q10", Q10), ("q1", Q1)]:
        for f, c in variants:
            s = f"{f.lower()}_{c.lower()}"
            restart_stack()
            for run in ["cold"] + [f"warm{i}" for i in range(1, WARM_RUNS + 1)]:
                src = f"{qname}_{s}_{run}"
                wall, _ = trino(qsql.format(s=s), source=src)
                add(s, f, c, f"{qname}_wall", qname, run, round(wall, 3), "s")
                add(s, f, c, f"{qname}_exec", qname, run, exec_ms(src), "ms")

    with open("results/benchmark.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["variant", "format", "codec", "metric", "object", "run", "value", "unit"])
        w.writerows(rows)

    # 3. Сводка (warm = медиана тёплых прогонов, запись = лучший из прогонов)
    def vals(v, metric, obj=None, run=None):
        return [r[6] for r in rows if r[0] == v and r[3] == metric
                and (obj is None or r[4] == obj)
                and (run is None or r[5] == run or (run == "warm" and str(r[5]).startswith("warm")))]

    with open("results/benchmark_summary.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["variant", "format", "codec", "lineitem_mb", "orders_mb", "bronze_total_mb",
                    "lineitem_files", "write_lineitem_s", "write_orders_s",
                    "q10_cold_s", "q10_warm_s", "q10_cold_exec_ms", "q10_warm_exec_ms",
                    "q1_cold_s", "q1_warm_s"])
        for f, c in variants:
            v = f"{f.lower()}_{c.lower()}"
            w.writerow([
                v, f, c,
                round(vals(v, "size", "lineitem")[0] / 2**20, 2),
                round(vals(v, "size", "orders")[0] / 2**20, 2),
                round(sum(vals(v, "size")) / 2**20, 2),
                vals(v, "files", "lineitem")[0],
                round(min(vals(v, "write_wall", "lineitem")), 2),
                round(min(vals(v, "write_wall", "orders")), 2),
                vals(v, "q10_wall", run="cold")[0],
                round(median(vals(v, "q10_wall", run="warm")), 3),
                vals(v, "q10_exec", run="cold")[0],
                median(vals(v, "q10_exec", run="warm")),
                vals(v, "q1_wall", run="cold")[0],
                round(median(vals(v, "q1_wall", run="warm")), 3),
            ])
    print("готово: results/benchmark.csv, results/benchmark_summary.csv")


if __name__ == "__main__":
    main()
