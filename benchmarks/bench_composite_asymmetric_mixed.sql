-- Benchmark: asymmetric composite-key mixed probe/build aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_composite_asymmetric_mixed.sql

.timer on

.print === Composite asymmetric mixed, small probe / large build, grouped full key ===
CREATE TABLE r_cam1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cam1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cam1.k1,
           r_cam1.k2,
           CAST(AVG(r_cam1.rv) AS INTEGER),
           CAST(AVG(s_cam1.sv) AS INTEGER),
           MIN(s_cam1.d),
           MAX(s_cam1.d)
    FROM r_cam1 JOIN s_cam1
      ON r_cam1.k1 = s_cam1.k1 AND r_cam1.k2 = s_cam1.k2
    GROUP BY r_cam1.k1, r_cam1.k2
) TO '/tmp/aggjoin_composite_asym_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cam1.k1,
           r_cam1.k2,
           CAST(AVG(r_cam1.rv) AS INTEGER),
           CAST(AVG(s_cam1.sv) AS INTEGER),
           MIN(s_cam1.d),
           MAX(s_cam1.d)
    FROM r_cam1 JOIN s_cam1
      ON r_cam1.k1 = s_cam1.k1 AND r_cam1.k2 = s_cam1.k2
    GROUP BY r_cam1.k1, r_cam1.k2
) TO '/tmp/aggjoin_composite_asym_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam1;
DROP TABLE s_cam1;

.print
.print === Composite asymmetric mixed, large probe / small build, grouped full key ===
CREATE TABLE r_cam2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cam2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cam2.k1,
           r_cam2.k2,
           CAST(AVG(r_cam2.rv) AS INTEGER),
           CAST(AVG(s_cam2.sv) AS INTEGER),
           MIN(s_cam2.d),
           MAX(s_cam2.d)
    FROM r_cam2 JOIN s_cam2
      ON r_cam2.k1 = s_cam2.k1 AND r_cam2.k2 = s_cam2.k2
    GROUP BY r_cam2.k1, r_cam2.k2
) TO '/tmp/aggjoin_composite_asym_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cam2.k1,
           r_cam2.k2,
           CAST(AVG(r_cam2.rv) AS INTEGER),
           CAST(AVG(s_cam2.sv) AS INTEGER),
           MIN(s_cam2.d),
           MAX(s_cam2.d)
    FROM r_cam2 JOIN s_cam2
      ON r_cam2.k1 = s_cam2.k1 AND r_cam2.k2 = s_cam2.k2
    GROUP BY r_cam2.k1, r_cam2.k2
) TO '/tmp/aggjoin_composite_asym_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam2;
DROP TABLE s_cam2;

.print
.print === Composite asymmetric mixed, subset-key small probe / large build ===
CREATE TABLE r_cam3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cam3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cam3.k1,
           CAST(AVG(r_cam3.rv) AS INTEGER),
           CAST(AVG(s_cam3.sv) AS INTEGER),
           MIN(s_cam3.d),
           MAX(s_cam3.d)
    FROM r_cam3 JOIN s_cam3
      ON r_cam3.k1 = s_cam3.k1 AND r_cam3.k2 = s_cam3.k2
    GROUP BY r_cam3.k1
) TO '/tmp/aggjoin_composite_asym_mixed_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cam3.k1,
           CAST(AVG(r_cam3.rv) AS INTEGER),
           CAST(AVG(s_cam3.sv) AS INTEGER),
           MIN(s_cam3.d),
           MAX(s_cam3.d)
    FROM r_cam3 JOIN s_cam3
      ON r_cam3.k1 = s_cam3.k1 AND r_cam3.k2 = s_cam3.k2
    GROUP BY r_cam3.k1
) TO '/tmp/aggjoin_composite_asym_mixed_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam3;
DROP TABLE s_cam3;

.print
.print === Composite asymmetric mixed, subset-key large probe / small build ===
CREATE TABLE r_cam4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cam4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cam4.k1,
           CAST(AVG(r_cam4.rv) AS INTEGER),
           CAST(AVG(s_cam4.sv) AS INTEGER),
           MIN(s_cam4.d),
           MAX(s_cam4.d)
    FROM r_cam4 JOIN s_cam4
      ON r_cam4.k1 = s_cam4.k1 AND r_cam4.k2 = s_cam4.k2
    GROUP BY r_cam4.k1
) TO '/tmp/aggjoin_composite_asym_mixed_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cam4.k1,
           CAST(AVG(r_cam4.rv) AS INTEGER),
           CAST(AVG(s_cam4.sv) AS INTEGER),
           MIN(s_cam4.d),
           MAX(s_cam4.d)
    FROM r_cam4 JOIN s_cam4
      ON r_cam4.k1 = s_cam4.k1 AND r_cam4.k2 = s_cam4.k2
    GROUP BY r_cam4.k1
) TO '/tmp/aggjoin_composite_asym_mixed_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam4;
DROP TABLE s_cam4;

.print
.print === Composite asymmetric mixed, ungrouped small probe / large build ===
CREATE TABLE r_cam5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_cam5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT CAST(AVG(r_cam5.rv) AS INTEGER),
           CAST(AVG(s_cam5.sv) AS INTEGER),
           MIN(s_cam5.d),
           MAX(s_cam5.d)
    FROM r_cam5 JOIN s_cam5
      ON r_cam5.k1 = s_cam5.k1 AND r_cam5.k2 = s_cam5.k2
) TO '/tmp/aggjoin_composite_asym_mixed_5a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT CAST(AVG(r_cam5.rv) AS INTEGER),
           CAST(AVG(s_cam5.sv) AS INTEGER),
           MIN(s_cam5.d),
           MAX(s_cam5.d)
    FROM r_cam5 JOIN s_cam5
      ON r_cam5.k1 = s_cam5.k1 AND r_cam5.k2 = s_cam5.k2
) TO '/tmp/aggjoin_composite_asym_mixed_5b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam5;
DROP TABLE s_cam5;

.print
.print === Composite asymmetric mixed, ungrouped large probe / small build ===
CREATE TABLE r_cam6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_cam6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT CAST(AVG(r_cam6.rv) AS INTEGER),
           CAST(AVG(s_cam6.sv) AS INTEGER),
           MIN(s_cam6.d),
           MAX(s_cam6.d)
    FROM r_cam6 JOIN s_cam6
      ON r_cam6.k1 = s_cam6.k1 AND r_cam6.k2 = s_cam6.k2
) TO '/tmp/aggjoin_composite_asym_mixed_6a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT CAST(AVG(r_cam6.rv) AS INTEGER),
           CAST(AVG(s_cam6.sv) AS INTEGER),
           MIN(s_cam6.d),
           MAX(s_cam6.d)
    FROM r_cam6 JOIN s_cam6
      ON r_cam6.k1 = s_cam6.k1 AND r_cam6.k2 = s_cam6.k2
) TO '/tmp/aggjoin_composite_asym_mixed_6b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cam6;
DROP TABLE s_cam6;
