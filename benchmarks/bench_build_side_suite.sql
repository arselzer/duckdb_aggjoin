-- Benchmark: build-side aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_build_side_suite.sql

.timer on

.print === Build-side grouped-by-key, SUM+COUNT+AVG, 100K keys, 1M rows ===
CREATE TABLE r_b1 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_b1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_b1.x,
           SUM(s_b1.y),
           COUNT(s_b1.y),
           AVG(s_b1.y)
    FROM r_b1 JOIN s_b1 ON r_b1.x = s_b1.x
    GROUP BY r_b1.x
) TO '/tmp/aggjoin_build_suite_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_b1.x,
           SUM(s_b1.y),
           COUNT(s_b1.y),
           AVG(s_b1.y)
    FROM r_b1 JOIN s_b1 ON r_b1.x = s_b1.x
    GROUP BY r_b1.x
) TO '/tmp/aggjoin_build_suite_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_b1;
DROP TABLE s_b1;

.print
.print === Build-side grouped-by-key, SUM+MIN+MAX+AVG, 100K keys, 1M rows ===
CREATE TABLE r_b2 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_b2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_b2.x,
           SUM(s_b2.y),
           MIN(s_b2.y),
           MAX(s_b2.y),
           AVG(s_b2.y)
    FROM r_b2 JOIN s_b2 ON r_b2.x = s_b2.x
    GROUP BY r_b2.x
) TO '/tmp/aggjoin_build_suite_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_b2.x,
           SUM(s_b2.y),
           MIN(s_b2.y),
           MAX(s_b2.y),
           AVG(s_b2.y)
    FROM r_b2 JOIN s_b2 ON r_b2.x = s_b2.x
    GROUP BY r_b2.x
) TO '/tmp/aggjoin_build_suite_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_b2;
DROP TABLE s_b2;

.print
.print === Build-side ungrouped, SUM+COUNT+AVG, 100K keys, 1M rows ===
CREATE TABLE r_b3 AS
SELECT i % 100000 AS x
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_b3 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT SUM(s_b3.y),
           COUNT(s_b3.y),
           AVG(s_b3.y)
    FROM r_b3 JOIN s_b3 ON r_b3.x = s_b3.x
) TO '/tmp/aggjoin_build_suite_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(s_b3.y),
           COUNT(s_b3.y),
           AVG(s_b3.y)
    FROM r_b3 JOIN s_b3 ON r_b3.x = s_b3.x
) TO '/tmp/aggjoin_build_suite_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_b3;
DROP TABLE s_b3;

.print
.print === Build-side asymmetric, 100K probe x 5M build, grouped SUM+COUNT ===
CREATE TABLE r_b4 AS
SELECT i % 50000 AS x
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_b4 AS
SELECT i % 50000 AS x,
       CAST(random() * 100 AS DOUBLE) AS y
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_b4.x,
           SUM(s_b4.y),
           COUNT(s_b4.y)
    FROM r_b4 JOIN s_b4 ON r_b4.x = s_b4.x
    GROUP BY r_b4.x
) TO '/tmp/aggjoin_build_suite_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_b4.x,
           SUM(s_b4.y),
           COUNT(s_b4.y)
    FROM r_b4 JOIN s_b4 ON r_b4.x = s_b4.x
    GROUP BY r_b4.x
) TO '/tmp/aggjoin_build_suite_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_b4;
DROP TABLE s_b4;
