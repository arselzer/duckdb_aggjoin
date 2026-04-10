-- Benchmark: probe-side non-numeric + build-side numeric aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_probe_nonnumeric_mixed.sql

.timer on

.print === Probe-side grouped-by-key, DATE MIN+MAX + build SUM, 100K keys, 1M rows ===
CREATE TABLE r_pnm1 AS
SELECT i % 100000 AS x,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pnm1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_pnm1.x,
           MIN(r_pnm1.d),
           MAX(r_pnm1.d),
           SUM(s_pnm1.sv)
    FROM r_pnm1 JOIN s_pnm1 ON r_pnm1.x = s_pnm1.x
    GROUP BY r_pnm1.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_pnm1.x,
           MIN(r_pnm1.d),
           MAX(r_pnm1.d),
           SUM(s_pnm1.sv)
    FROM r_pnm1 JOIN s_pnm1 ON r_pnm1.x = s_pnm1.x
    GROUP BY r_pnm1.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pnm1;
DROP TABLE s_pnm1;

.print
.print === Probe-side grouped-by-key, VARCHAR MIN+MAX + build SUM, 100K keys, 1M rows ===
CREATE TABLE r_pnm2 AS
SELECT i % 100000 AS x,
       'v' || LPAD(CAST(i % 10 AS VARCHAR), 2, '0') AS label
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pnm2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_pnm2.x,
           MIN(r_pnm2.label),
           MAX(r_pnm2.label),
           SUM(s_pnm2.sv)
    FROM r_pnm2 JOIN s_pnm2 ON r_pnm2.x = s_pnm2.x
    GROUP BY r_pnm2.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_pnm2.x,
           MIN(r_pnm2.label),
           MAX(r_pnm2.label),
           SUM(s_pnm2.sv)
    FROM r_pnm2 JOIN s_pnm2 ON r_pnm2.x = s_pnm2.x
    GROUP BY r_pnm2.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pnm2;
DROP TABLE s_pnm2;

.print
.print === Probe-side ungrouped, DATE MIN+MAX + build SUM, 100K keys, 1M rows ===
CREATE TABLE r_pnm3 AS
SELECT i % 100000 AS x,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pnm3 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_pnm3.d),
           MAX(r_pnm3.d),
           SUM(s_pnm3.sv)
    FROM r_pnm3 JOIN s_pnm3 ON r_pnm3.x = s_pnm3.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_pnm3.d),
           MAX(r_pnm3.d),
           SUM(s_pnm3.sv)
    FROM r_pnm3 JOIN s_pnm3 ON r_pnm3.x = s_pnm3.x
) TO '/tmp/aggjoin_probe_nonnumeric_mixed_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pnm3;
DROP TABLE s_pnm3;
