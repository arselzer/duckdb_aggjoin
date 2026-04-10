-- Benchmark: build-side mixed numeric + non-numeric aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_build_side_mixed.sql

.timer on

.print === Build-side grouped-by-key, SUM+VARCHAR MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_bm1 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_bm1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y,
       'v' || LPAD(CAST(i % 10 AS VARCHAR), 2, '0') AS label
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_bm1.x,
           SUM(s_bm1.y),
           MIN(s_bm1.label),
           MAX(s_bm1.label)
    FROM r_bm1 JOIN s_bm1 ON r_bm1.x = s_bm1.x
    GROUP BY r_bm1.x
) TO '/tmp/aggjoin_build_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_bm1.x,
           SUM(s_bm1.y),
           MIN(s_bm1.label),
           MAX(s_bm1.label)
    FROM r_bm1 JOIN s_bm1 ON r_bm1.x = s_bm1.x
    GROUP BY r_bm1.x
) TO '/tmp/aggjoin_build_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_bm1;
DROP TABLE s_bm1;

.print
.print === Build-side ungrouped, SUM+DATE MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_bm2 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_bm2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT SUM(s_bm2.y),
           MIN(s_bm2.d),
           MAX(s_bm2.d)
    FROM r_bm2 JOIN s_bm2 ON r_bm2.x = s_bm2.x
) TO '/tmp/aggjoin_build_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(s_bm2.y),
           MIN(s_bm2.d),
           MAX(s_bm2.d)
    FROM r_bm2 JOIN s_bm2 ON r_bm2.x = s_bm2.x
) TO '/tmp/aggjoin_build_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_bm2;
DROP TABLE s_bm2;
