-- Benchmark: single-key pure nonnumeric probe/build aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_singlekey_pure_nonnumeric.sql

.timer on

.print === Single-key pure nonnumeric, small probe / large build, grouped ===
CREATE TABLE r_sn1 AS
SELECT i % 1000 AS k,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_sn1 AS
SELECT i % 1000 AS k,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_sn1.k,
           MIN(r_sn1.lbl),
           MAX(r_sn1.lbl),
           MIN(s_sn1.d),
           MAX(s_sn1.d)
    FROM r_sn1 JOIN s_sn1 ON r_sn1.k = s_sn1.k
    GROUP BY r_sn1.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_sn1.k,
           MIN(r_sn1.lbl),
           MAX(r_sn1.lbl),
           MIN(s_sn1.d),
           MAX(s_sn1.d)
    FROM r_sn1 JOIN s_sn1 ON r_sn1.k = s_sn1.k
    GROUP BY r_sn1.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_sn1;
DROP TABLE s_sn1;

.print
.print === Single-key pure nonnumeric, large probe / small build, grouped ===
CREATE TABLE r_sn2 AS
SELECT i % 1000 AS k,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_sn2 AS
SELECT i % 1000 AS k,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_sn2.k,
           MIN(r_sn2.lbl),
           MAX(r_sn2.lbl),
           MIN(s_sn2.d),
           MAX(s_sn2.d)
    FROM r_sn2 JOIN s_sn2 ON r_sn2.k = s_sn2.k
    GROUP BY r_sn2.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_sn2.k,
           MIN(r_sn2.lbl),
           MAX(r_sn2.lbl),
           MIN(s_sn2.d),
           MAX(s_sn2.d)
    FROM r_sn2 JOIN s_sn2 ON r_sn2.k = s_sn2.k
    GROUP BY r_sn2.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_sn2;
DROP TABLE s_sn2;

.print
.print === Single-key pure nonnumeric, small probe / large build, ungrouped ===
CREATE TABLE r_sn3 AS
SELECT i % 1000 AS k,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_sn3 AS
SELECT i % 1000 AS k,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_sn3.lbl),
           MAX(r_sn3.lbl),
           MIN(s_sn3.d),
           MAX(s_sn3.d)
    FROM r_sn3 JOIN s_sn3 ON r_sn3.k = s_sn3.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_sn3.lbl),
           MAX(r_sn3.lbl),
           MIN(s_sn3.d),
           MAX(s_sn3.d)
    FROM r_sn3 JOIN s_sn3 ON r_sn3.k = s_sn3.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_sn3;
DROP TABLE s_sn3;

.print
.print === Single-key pure nonnumeric, large probe / small build, ungrouped ===
CREATE TABLE r_sn4 AS
SELECT i % 1000 AS k,
       CASE WHEN i % 3 = 0 THEN 'c_' || CAST(i % 17 AS VARCHAR)
            WHEN i % 3 = 1 THEN 'a_' || CAST(i % 19 AS VARCHAR)
            ELSE 'b_' || CAST(i % 23 AS VARCHAR) END AS lbl
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_sn4 AS
SELECT i % 1000 AS k,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_sn4.lbl),
           MAX(r_sn4.lbl),
           MIN(s_sn4.d),
           MAX(s_sn4.d)
    FROM r_sn4 JOIN s_sn4 ON r_sn4.k = s_sn4.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_sn4.lbl),
           MAX(r_sn4.lbl),
           MIN(s_sn4.d),
           MAX(s_sn4.d)
    FROM r_sn4 JOIN s_sn4 ON r_sn4.k = s_sn4.k
) TO '/tmp/aggjoin_singlekey_pure_nonnumeric_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_sn4;
DROP TABLE s_sn4;
