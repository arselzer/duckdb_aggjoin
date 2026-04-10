-- Benchmark: build-side non-numeric MIN/MAX shapes
-- Run: build/Release/duckdb < benchmarks/bench_build_side_nonnumeric.sql

.timer on

.print === Build-side grouped-by-key, VARCHAR MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_bn1 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_bn1 AS
SELECT i % 100000 AS x,
       'v' || LPAD(CAST(i % 10 AS VARCHAR), 2, '0') AS label
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_bn1.x,
           MIN(s_bn1.label),
           MAX(s_bn1.label)
    FROM r_bn1 JOIN s_bn1 ON r_bn1.x = s_bn1.x
    GROUP BY r_bn1.x
) TO '/tmp/aggjoin_build_nonnumeric_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_bn1.x,
           MIN(s_bn1.label),
           MAX(s_bn1.label)
    FROM r_bn1 JOIN s_bn1 ON r_bn1.x = s_bn1.x
    GROUP BY r_bn1.x
) TO '/tmp/aggjoin_build_nonnumeric_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_bn1;
DROP TABLE s_bn1;

.print
.print === Build-side ungrouped, DATE MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_bn2 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_bn2 AS
SELECT i % 100000 AS x,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(s_bn2.d),
           MAX(s_bn2.d)
    FROM r_bn2 JOIN s_bn2 ON r_bn2.x = s_bn2.x
) TO '/tmp/aggjoin_build_nonnumeric_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(s_bn2.d),
           MAX(s_bn2.d)
    FROM r_bn2 JOIN s_bn2 ON r_bn2.x = s_bn2.x
) TO '/tmp/aggjoin_build_nonnumeric_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_bn2;
DROP TABLE s_bn2;
