-- Benchmark: mixed probe-side + build-side aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_probe_build_mixed.sql

.timer on

.print === Mixed grouped-by-key, probe SUM + build SUM+VARCHAR MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_pbm1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pbm1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       'v' || LPAD(CAST(i % 10 AS VARCHAR), 2, '0') AS label
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_pbm1.x,
           SUM(r_pbm1.rv),
           SUM(s_pbm1.sv),
           MIN(s_pbm1.label),
           MAX(s_pbm1.label)
    FROM r_pbm1 JOIN s_pbm1 ON r_pbm1.x = s_pbm1.x
    GROUP BY r_pbm1.x
) TO '/tmp/aggjoin_probe_build_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_pbm1.x,
           SUM(r_pbm1.rv),
           SUM(s_pbm1.sv),
           MIN(s_pbm1.label),
           MAX(s_pbm1.label)
    FROM r_pbm1 JOIN s_pbm1 ON r_pbm1.x = s_pbm1.x
    GROUP BY r_pbm1.x
) TO '/tmp/aggjoin_probe_build_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pbm1;
DROP TABLE s_pbm1;

.print
.print === Mixed grouped-by-key, probe SUM + build SUM+DATE MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_pbm2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pbm2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_pbm2.x,
           SUM(r_pbm2.rv),
           SUM(s_pbm2.sv),
           MIN(s_pbm2.d),
           MAX(s_pbm2.d)
    FROM r_pbm2 JOIN s_pbm2 ON r_pbm2.x = s_pbm2.x
    GROUP BY r_pbm2.x
) TO '/tmp/aggjoin_probe_build_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_pbm2.x,
           SUM(r_pbm2.rv),
           SUM(s_pbm2.sv),
           MIN(s_pbm2.d),
           MAX(s_pbm2.d)
    FROM r_pbm2 JOIN s_pbm2 ON r_pbm2.x = s_pbm2.x
    GROUP BY r_pbm2.x
) TO '/tmp/aggjoin_probe_build_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pbm2;
DROP TABLE s_pbm2;

.print
.print === Mixed ungrouped, probe SUM + build SUM+DATE MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_pbm3 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pbm3 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT SUM(r_pbm3.rv),
           SUM(s_pbm3.sv),
           MIN(s_pbm3.d),
           MAX(s_pbm3.d)
    FROM r_pbm3 JOIN s_pbm3 ON r_pbm3.x = s_pbm3.x
) TO '/tmp/aggjoin_probe_build_mixed_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_pbm3.rv),
           SUM(s_pbm3.sv),
           MIN(s_pbm3.d),
           MAX(s_pbm3.d)
    FROM r_pbm3 JOIN s_pbm3 ON r_pbm3.x = s_pbm3.x
) TO '/tmp/aggjoin_probe_build_mixed_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pbm3;
DROP TABLE s_pbm3;

.print
.print === Mixed grouped-by-key, probe AVG + build AVG+DATE MIN+MAX, 100K keys, 1M rows ===
CREATE TABLE r_pbm4 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_pbm4 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_pbm4.x,
           AVG(r_pbm4.rv),
           AVG(s_pbm4.sv),
           MIN(s_pbm4.d),
           MAX(s_pbm4.d)
    FROM r_pbm4 JOIN s_pbm4 ON r_pbm4.x = s_pbm4.x
    GROUP BY r_pbm4.x
) TO '/tmp/aggjoin_probe_build_mixed_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_pbm4.x,
           AVG(r_pbm4.rv),
           AVG(s_pbm4.sv),
           MIN(s_pbm4.d),
           MAX(s_pbm4.d)
    FROM r_pbm4 JOIN s_pbm4 ON r_pbm4.x = s_pbm4.x
    GROUP BY r_pbm4.x
) TO '/tmp/aggjoin_probe_build_mixed_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_pbm4;
DROP TABLE s_pbm4;
