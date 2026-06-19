#!/usr/bin/env python3
"""Generate randomized differential tests for the logical Yannakakis rewrite.

The generated cases compare the extension plan against the same query with the
extension optimizer disabled. They also require the extension-side query to fire
the logical tree rewrite marker, so a passing run proves both correctness and
coverage of the intended rule.
"""

from __future__ import annotations

import argparse
import csv
import random
import subprocess
import sys
from pathlib import Path


def sql_int(expr: str) -> str:
    return f"CAST({expr} AS INTEGER)"


def edge_expr(i_expr: str, domain_a: int, domain_b: int, cast_a: str = "INTEGER", cast_b: str = "INTEGER") -> tuple[str, str]:
    return (
        f"CAST(({i_expr}) % {domain_a} AS {cast_a})",
        f"CAST(floor(({i_expr}) / {domain_a}) % {domain_b} AS {cast_b})",
    )


def make_case(case_id: int, rng: random.Random) -> str:
    rows = rng.choice([30000, 36000, 42000, 48000, 54000, 60000])
    composite = rng.choice([False, True])
    grouped = rng.choice([False, True])
    suffix = f"yrd_{case_id}"

    if composite:
        d0a = rng.choice([120, 150, 200, 240, 300])
        d0b = rng.choice([10, 12, 15, 20])
        d1a = rng.choice([160, 200, 240, 300, 400])
        d1b = rng.choice([10, 12, 15, 20])
        # Keep each side around 5-40 duplicates/key so the logical rewrite fires
        # but the native baseline remains practical.
        while rows // (d0a * d0b) < 10:
            d0a = max(60, d0a // 2)
        while rows // (d1a * d1b) < 10:
            d1a = max(80, d1a // 2)
        t0_a, t0_b = edge_expr("i", d0a, d0b, "SMALLINT", "TINYINT")
        t1_a, t1_b = edge_expr("i", d0a, d0b, "INTEGER", "BIGINT")
        t1_c, t1_d = edge_expr("i", d1a, d1b, "SMALLINT", "INTEGER")
        t2_c, t2_d = edge_expr("i", d1a, d1b, "BIGINT", "SMALLINT")
        join01 = f"{suffix}_t0.a = {suffix}_t1.a AND {suffix}_t0.b = {suffix}_t1.b"
        join12 = f"{suffix}_t1.c = {suffix}_t2.c AND {suffix}_t1.d = {suffix}_t2.d"
        create0 = f"""
CREATE TABLE {suffix}_t0 AS
SELECT {t0_a} AS a,
       {t0_b} AS b,
       {sql_int(f"(i * {rng.randrange(5, 31)} + {rng.randrange(0, 11)}) % 9")} AS g0,
       CASE WHEN i % {rng.choice([11, 13, 17])} = 0 THEN NULL
            ELSE {sql_int(f"(i * {rng.randrange(17, 53)} + {rng.randrange(0, 29)}) % 1000")} END AS v0
FROM range({rows}) t(i);
"""
        create1 = f"""
CREATE TABLE {suffix}_t1 AS
SELECT {t1_a} AS a,
       {t1_b} AS b,
       {t1_c} AS c,
       {t1_d} AS d,
       {sql_int(f"(i * {rng.randrange(19, 61)} + {rng.randrange(0, 31)}) % 2048")} AS v1
FROM range({rows}) t(i);
"""
        create2 = f"""
CREATE TABLE {suffix}_t2 AS
SELECT {t2_c} AS c,
       {t2_d} AS d,
       {sql_int(f"(i * {rng.randrange(7, 29)} + {rng.randrange(0, 13)}) % 7")} AS g2,
       {sql_int(f"(i * {rng.randrange(23, 67)} + {rng.randrange(0, 37)}) % 997")} AS v2,
       (i % {rng.choice([3, 5, 7])} = 0) AS flag
FROM range({rows}) t(i);
"""
    else:
        d0 = rng.choice([300, 400, 500, 600, 800, 1000])
        d1 = rng.choice([100, 150, 200, 300, 400])
        join01 = f"{suffix}_t0.b = {suffix}_t1.b"
        join12 = f"{suffix}_t1.c = {suffix}_t2.c"
        create0 = f"""
CREATE TABLE {suffix}_t0 AS
SELECT i::INTEGER AS b,
       {sql_int(f"(i * {rng.randrange(5, 31)} + {rng.randrange(0, 11)}) % 9")} AS g0,
       CASE WHEN i % {rng.choice([11, 13, 17])} = 0 THEN NULL
            ELSE {sql_int(f"(i * {rng.randrange(17, 53)} + {rng.randrange(0, 29)}) % 1000")} END AS v0
FROM range({rows}) t(i);
"""
        create1 = f"""
CREATE TABLE {suffix}_t1 AS
SELECT i::INTEGER AS b,
       {sql_int(f"i % {d1}")} AS c,
       {sql_int(f"(i * {rng.randrange(19, 61)} + {rng.randrange(0, 31)}) % 2048")} AS v1
FROM range({rows}) t(i);
"""
        create2 = f"""
CREATE TABLE {suffix}_t2 AS
SELECT {sql_int(f"i % {d1}")} AS c,
       {sql_int(f"(i * {rng.randrange(7, 29)} + {rng.randrange(0, 13)}) % 7")} AS g2,
       {sql_int(f"(i * {rng.randrange(23, 67)} + {rng.randrange(0, 37)}) % 997")} AS v2,
       (i % {rng.choice([3, 5, 7])} = 0) AS flag
FROM range({rows}) t(i);
"""
        # Keep d0 deliberately unused in the first edge: t0.b/t1.b is 1:1, while
        # the second edge provides the blowup. Varying rows/d1 still changes the
        # generated native result shape across cases.
        _ = d0

    group_cols = f"{suffix}_t0.g0, {suffix}_t2.g2"
    select_group = f"{group_cols}," if grouped else ""
    group_by = f"GROUP BY {group_cols}" if grouped else ""
    query = f"""
SELECT {select_group}
       COUNT(*) AS c_all,
       COUNT({suffix}_t0.v0) AS c_v0,
       SUM({suffix}_t0.v0) AS s_v0,
       SUM({suffix}_t1.v1) AS s_v1,
       MIN({suffix}_t2.v2) AS min_v2,
       MAX({suffix}_t2.v2) AS max_v2,
       BOOL_OR({suffix}_t2.flag) AS any_flag,
       BIT_OR({suffix}_t1.v1) AS any_bits
FROM {suffix}_t0
JOIN {suffix}_t1 ON {join01}
JOIN {suffix}_t2 ON {join12}
{group_by}
"""
    return f"""
{create0}
{create1}
{create2}
SELECT aggjoin_reset_rewrite_marker();
CREATE TEMP TABLE {suffix}_ext AS {query};
PRAGMA disabled_optimizers='extension';
CREATE TEMP TABLE {suffix}_native AS {query};
PRAGMA disabled_optimizers='';
SELECT '{suffix}' AS case_name,
       aggjoin_last_rewrite() AS marker,
       (SELECT COUNT(*) FROM (
           (SELECT * FROM {suffix}_ext EXCEPT ALL SELECT * FROM {suffix}_native)
           UNION ALL
           (SELECT * FROM {suffix}_native EXCEPT ALL SELECT * FROM {suffix}_ext)
       ) diff) AS diff_count;
DROP TABLE {suffix}_native;
DROP TABLE {suffix}_ext;
DROP TABLE {suffix}_t0;
DROP TABLE {suffix}_t1;
DROP TABLE {suffix}_t2;
"""


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", default=str(root / "build/Release/duckdb"))
    parser.add_argument("--seed", type=int, default=20260619)
    parser.add_argument("--cases", type=int, default=8)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    sql = [
        "PRAGMA threads=1;",
        "SELECT aggjoin_set_operator_enabled(false);",
        "SELECT aggjoin_set_logical_rewrites_enabled(true);",
    ]
    sql.extend(make_case(i, rng) for i in range(args.cases))
    sql.append("SELECT aggjoin_set_operator_enabled(true);")
    sql.append("SELECT aggjoin_set_logical_rewrites_enabled(true);")

    proc = subprocess.run(
        [args.duckdb, "-csv", "-noheader"],
        input="\n".join(sql),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout)
        sys.stderr.write(proc.stderr)
        return proc.returncode

    rows = list(csv.reader(proc.stdout.splitlines()))
    failures = []
    for row in rows:
        if len(row) != 3 or not row[0].startswith("yrd_"):
            continue
        case_name, marker, diff_count = row
        if marker != "agg_propagation" or diff_count != "0":
            failures.append((case_name, marker, diff_count))
    case_rows = [row for row in rows if len(row) == 3 and row[0].startswith("yrd_")]
    if len(case_rows) != args.cases:
        failures.append(("missing-output", f"rows={len(case_rows)}", f"cases={args.cases}"))
    if failures:
        for case_name, marker, diff_count in failures:
            print(f"FAIL {case_name}: marker={marker} diff_count={diff_count}", file=sys.stderr)
        return 1
    print(f"ok: {args.cases} randomized Yannakakis differential cases passed (seed={args.seed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
