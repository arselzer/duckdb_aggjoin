-- Benchmark: asymmetric composite-key richer mixed shapes with probe-side VARCHAR MIN/MAX
-- Run: build/Release/duckdb < benchmarks/bench_composite_asymmetric_probe_varchar_richer_mixed.sql

.timer on

.print === Composite asym probe-varchar richer mixed, small probe / large build, grouped full key ===
CREATE TABLE r_cvs1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cvs1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cvs1.k1,
           r_cvs1.k2,
           MIN(r_cvs1.lbl),
           MAX(r_cvs1.lbl),
           CAST(AVG(s_cvs1.sv) AS INTEGER),
           MIN(s_cvs1.sd),
           MAX(s_cvs1.sd)
    FROM r_cvs1 JOIN s_cvs1
      ON r_cvs1.k1 = s_cvs1.k1 AND r_cvs1.k2 = s_cvs1.k2
    GROUP BY r_cvs1.k1, r_cvs1.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cvs1.k1,
           r_cvs1.k2,
           MIN(r_cvs1.lbl),
           MAX(r_cvs1.lbl),
           CAST(AVG(s_cvs1.sv) AS INTEGER),
           MIN(s_cvs1.sd),
           MAX(s_cvs1.sd)
    FROM r_cvs1 JOIN s_cvs1
      ON r_cvs1.k1 = s_cvs1.k1 AND r_cvs1.k2 = s_cvs1.k2
    GROUP BY r_cvs1.k1, r_cvs1.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs1;
DROP TABLE s_cvs1;

.print
.print === Composite asym probe-varchar richer mixed, large probe / small build, grouped full key ===
CREATE TABLE r_cvs2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cvs2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cvs2.k1,
           r_cvs2.k2,
           MIN(r_cvs2.lbl),
           MAX(r_cvs2.lbl),
           CAST(AVG(s_cvs2.sv) AS INTEGER),
           MIN(s_cvs2.sd),
           MAX(s_cvs2.sd)
    FROM r_cvs2 JOIN s_cvs2
      ON r_cvs2.k1 = s_cvs2.k1 AND r_cvs2.k2 = s_cvs2.k2
    GROUP BY r_cvs2.k1, r_cvs2.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cvs2.k1,
           r_cvs2.k2,
           MIN(r_cvs2.lbl),
           MAX(r_cvs2.lbl),
           CAST(AVG(s_cvs2.sv) AS INTEGER),
           MIN(s_cvs2.sd),
           MAX(s_cvs2.sd)
    FROM r_cvs2 JOIN s_cvs2
      ON r_cvs2.k1 = s_cvs2.k1 AND r_cvs2.k2 = s_cvs2.k2
    GROUP BY r_cvs2.k1, r_cvs2.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs2;
DROP TABLE s_cvs2;

.print
.print === Composite asym probe-varchar richer mixed, small probe / large build, grouped subset key ===
CREATE TABLE r_cvs3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cvs3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cvs3.k1,
           MIN(r_cvs3.lbl),
           MAX(r_cvs3.lbl),
           CAST(AVG(s_cvs3.sv) AS INTEGER),
           MIN(s_cvs3.sd),
           MAX(s_cvs3.sd)
    FROM r_cvs3 JOIN s_cvs3
      ON r_cvs3.k1 = s_cvs3.k1 AND r_cvs3.k2 = s_cvs3.k2
    GROUP BY r_cvs3.k1
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cvs3.k1,
           MIN(r_cvs3.lbl),
           MAX(r_cvs3.lbl),
           CAST(AVG(s_cvs3.sv) AS INTEGER),
           MIN(s_cvs3.sd),
           MAX(s_cvs3.sd)
    FROM r_cvs3 JOIN s_cvs3
      ON r_cvs3.k1 = s_cvs3.k1 AND r_cvs3.k2 = s_cvs3.k2
    GROUP BY r_cvs3.k1
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs3;
DROP TABLE s_cvs3;

.print
.print === Composite asym probe-varchar richer mixed, large probe / small build, grouped subset key ===
CREATE TABLE r_cvs4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cvs4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cvs4.k1,
           MIN(r_cvs4.lbl),
           MAX(r_cvs4.lbl),
           CAST(AVG(s_cvs4.sv) AS INTEGER),
           MIN(s_cvs4.sd),
           MAX(s_cvs4.sd)
    FROM r_cvs4 JOIN s_cvs4
      ON r_cvs4.k1 = s_cvs4.k1 AND r_cvs4.k2 = s_cvs4.k2
    GROUP BY r_cvs4.k1
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cvs4.k1,
           MIN(r_cvs4.lbl),
           MAX(r_cvs4.lbl),
           CAST(AVG(s_cvs4.sv) AS INTEGER),
           MIN(s_cvs4.sd),
           MAX(s_cvs4.sd)
    FROM r_cvs4 JOIN s_cvs4
      ON r_cvs4.k1 = s_cvs4.k1 AND r_cvs4.k2 = s_cvs4.k2
    GROUP BY r_cvs4.k1
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs4;
DROP TABLE s_cvs4;

.print
.print === Composite asym probe-varchar richer mixed, small probe / large build, ungrouped ===
CREATE TABLE r_cvs5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cvs5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_cvs5.lbl),
           MAX(r_cvs5.lbl),
           CAST(AVG(s_cvs5.sv) AS INTEGER),
           MIN(s_cvs5.sd),
           MAX(s_cvs5.sd)
    FROM r_cvs5 JOIN s_cvs5
      ON r_cvs5.k1 = s_cvs5.k1 AND r_cvs5.k2 = s_cvs5.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_5a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_cvs5.lbl),
           MAX(r_cvs5.lbl),
           CAST(AVG(s_cvs5.sv) AS INTEGER),
           MIN(s_cvs5.sd),
           MAX(s_cvs5.sd)
    FROM r_cvs5 JOIN s_cvs5
      ON r_cvs5.k1 = s_cvs5.k1 AND r_cvs5.k2 = s_cvs5.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_5b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs5;
DROP TABLE s_cvs5;

.print
.print === Composite asym probe-varchar richer mixed, large probe / small build, ungrouped ===
CREATE TABLE r_cvs6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cvs6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_cvs6.lbl),
           MAX(r_cvs6.lbl),
           CAST(AVG(s_cvs6.sv) AS INTEGER),
           MIN(s_cvs6.sd),
           MAX(s_cvs6.sd)
    FROM r_cvs6 JOIN s_cvs6
      ON r_cvs6.k1 = s_cvs6.k1 AND r_cvs6.k2 = s_cvs6.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_6a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_cvs6.lbl),
           MAX(r_cvs6.lbl),
           CAST(AVG(s_cvs6.sv) AS INTEGER),
           MIN(s_cvs6.sd),
           MAX(s_cvs6.sd)
    FROM r_cvs6 JOIN s_cvs6
      ON r_cvs6.k1 = s_cvs6.k1 AND r_cvs6.k2 = s_cvs6.k2
) TO '/tmp/aggjoin_composite_asym_probe_varchar_richer_6b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cvs6;
DROP TABLE s_cvs6;
