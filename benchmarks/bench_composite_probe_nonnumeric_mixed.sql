-- Benchmark: composite-key probe-side non-numeric + build-side numeric shapes
-- Run: build/Release/duckdb < benchmarks/bench_composite_probe_nonnumeric_mixed.sql

.timer on

.print === Composite grouped-by-key, probe DATE MIN+MAX + build SUM, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cpnm1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cpnm1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cpnm1.k1,
           r_cpnm1.k2,
           MIN(r_cpnm1.d),
           MAX(r_cpnm1.d),
           SUM(s_cpnm1.sv)
    FROM r_cpnm1 JOIN s_cpnm1
      ON r_cpnm1.k1 = s_cpnm1.k1 AND r_cpnm1.k2 = s_cpnm1.k2
    GROUP BY r_cpnm1.k1, r_cpnm1.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cpnm1.k1,
           r_cpnm1.k2,
           MIN(r_cpnm1.d),
           MAX(r_cpnm1.d),
           SUM(s_cpnm1.sv)
    FROM r_cpnm1 JOIN s_cpnm1
      ON r_cpnm1.k1 = s_cpnm1.k1 AND r_cpnm1.k2 = s_cpnm1.k2
    GROUP BY r_cpnm1.k1, r_cpnm1.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cpnm1;
DROP TABLE s_cpnm1;

.print
.print === Composite grouped-by-subset, probe DATE MIN+MAX + build SUM, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cpnm2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cpnm2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cpnm2.k1,
           MIN(r_cpnm2.d),
           MAX(r_cpnm2.d),
           SUM(s_cpnm2.sv)
    FROM r_cpnm2 JOIN s_cpnm2
      ON r_cpnm2.k1 = s_cpnm2.k1 AND r_cpnm2.k2 = s_cpnm2.k2
    GROUP BY r_cpnm2.k1
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cpnm2.k1,
           MIN(r_cpnm2.d),
           MAX(r_cpnm2.d),
           SUM(s_cpnm2.sv)
    FROM r_cpnm2 JOIN s_cpnm2
      ON r_cpnm2.k1 = s_cpnm2.k1 AND r_cpnm2.k2 = s_cpnm2.k2
    GROUP BY r_cpnm2.k1
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cpnm2;
DROP TABLE s_cpnm2;

.print
.print === Composite ungrouped, probe DATE MIN+MAX + build SUM, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cpnm3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cpnm3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_cpnm3.d),
           MAX(r_cpnm3.d),
           SUM(s_cpnm3.sv)
    FROM r_cpnm3 JOIN s_cpnm3
      ON r_cpnm3.k1 = s_cpnm3.k1 AND r_cpnm3.k2 = s_cpnm3.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_cpnm3.d),
           MAX(r_cpnm3.d),
           SUM(s_cpnm3.sv)
    FROM r_cpnm3 JOIN s_cpnm3
      ON r_cpnm3.k1 = s_cpnm3.k1 AND r_cpnm3.k2 = s_cpnm3.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cpnm3;
DROP TABLE s_cpnm3;

.print
.print === Composite grouped-by-key, probe VARCHAR MIN+MAX + build SUM, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cpnm4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       'v' || LPAD(CAST(i % 10 AS VARCHAR), 2, '0') AS label
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cpnm4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cpnm4.k1,
           r_cpnm4.k2,
           MIN(r_cpnm4.label),
           MAX(r_cpnm4.label),
           SUM(s_cpnm4.sv)
    FROM r_cpnm4 JOIN s_cpnm4
      ON r_cpnm4.k1 = s_cpnm4.k1 AND r_cpnm4.k2 = s_cpnm4.k2
    GROUP BY r_cpnm4.k1, r_cpnm4.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cpnm4.k1,
           r_cpnm4.k2,
           MIN(r_cpnm4.label),
           MAX(r_cpnm4.label),
           SUM(s_cpnm4.sv)
    FROM r_cpnm4 JOIN s_cpnm4
      ON r_cpnm4.k1 = s_cpnm4.k1 AND r_cpnm4.k2 = s_cpnm4.k2
    GROUP BY r_cpnm4.k1, r_cpnm4.k2
) TO '/tmp/aggjoin_composite_probe_nonnumeric_mixed_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cpnm4;
DROP TABLE s_cpnm4;
