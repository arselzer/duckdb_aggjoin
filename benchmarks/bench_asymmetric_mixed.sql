-- Benchmark: asymmetric mixed probe/build aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_asymmetric_mixed.sql

.timer on
PRAGMA threads=1;

.print === Asymmetric mixed, small probe / large build, grouped SUM+SUM+DATE MIN+MAX ===
CREATE TABLE r_abm1 AS
SELECT i % 50000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_abm1 AS
SELECT i % 50000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_abm1.x,
           SUM(r_abm1.rv),
           SUM(s_abm1.sv),
           MIN(s_abm1.d),
           MAX(s_abm1.d)
    FROM r_abm1 JOIN s_abm1 ON r_abm1.x = s_abm1.x
    GROUP BY r_abm1.x
) TO '/tmp/aggjoin_asymmetric_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_abm1.x,
           SUM(r_abm1.rv),
           SUM(s_abm1.sv),
           MIN(s_abm1.d),
           MAX(s_abm1.d)
    FROM r_abm1 JOIN s_abm1 ON r_abm1.x = s_abm1.x
    GROUP BY r_abm1.x
) TO '/tmp/aggjoin_asymmetric_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_abm1;
DROP TABLE s_abm1;

.print
.print === Asymmetric mixed, large probe / small build, grouped SUM+SUM+DATE MIN+MAX ===
CREATE TABLE r_abm2 AS
SELECT i % 50000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_abm2 AS
SELECT i % 50000 AS x,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_abm2.x,
           SUM(r_abm2.rv),
           SUM(s_abm2.sv),
           MIN(s_abm2.d),
           MAX(s_abm2.d)
    FROM r_abm2 JOIN s_abm2 ON r_abm2.x = s_abm2.x
    GROUP BY r_abm2.x
) TO '/tmp/aggjoin_asymmetric_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_abm2.x,
           SUM(r_abm2.rv),
           SUM(s_abm2.sv),
           MIN(s_abm2.d),
           MAX(s_abm2.d)
    FROM r_abm2 JOIN s_abm2 ON r_abm2.x = s_abm2.x
    GROUP BY r_abm2.x
) TO '/tmp/aggjoin_asymmetric_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_abm2;
DROP TABLE s_abm2;
