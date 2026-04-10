-- Benchmark: asymmetric composite-key richer mixed probe/build aggregate shapes
-- Run: build/Release/duckdb < benchmarks/bench_composite_asymmetric_richer_mixed.sql

.timer on

.print === Composite asym richer mixed, small probe / large build, grouped full key ===
CREATE TABLE r_car1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_car1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_car1.k1,
           r_car1.k2,
           CAST(AVG(r_car1.rv) AS INTEGER),
           MIN(r_car1.rd),
           MAX(r_car1.rd),
           CAST(AVG(s_car1.sv) AS INTEGER),
           MIN(s_car1.sd),
           MAX(s_car1.sd)
    FROM r_car1 JOIN s_car1
      ON r_car1.k1 = s_car1.k1 AND r_car1.k2 = s_car1.k2
    GROUP BY r_car1.k1, r_car1.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_car1.k1,
           r_car1.k2,
           CAST(AVG(r_car1.rv) AS INTEGER),
           MIN(r_car1.rd),
           MAX(r_car1.rd),
           CAST(AVG(s_car1.sv) AS INTEGER),
           MIN(s_car1.sd),
           MAX(s_car1.sd)
    FROM r_car1 JOIN s_car1
      ON r_car1.k1 = s_car1.k1 AND r_car1.k2 = s_car1.k2
    GROUP BY r_car1.k1, r_car1.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car1;
DROP TABLE s_car1;

.print
.print === Composite asym richer mixed, large probe / small build, grouped full key ===
CREATE TABLE r_car2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_car2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_car2.k1,
           r_car2.k2,
           CAST(AVG(r_car2.rv) AS INTEGER),
           MIN(r_car2.rd),
           MAX(r_car2.rd),
           CAST(AVG(s_car2.sv) AS INTEGER),
           MIN(s_car2.sd),
           MAX(s_car2.sd)
    FROM r_car2 JOIN s_car2
      ON r_car2.k1 = s_car2.k1 AND r_car2.k2 = s_car2.k2
    GROUP BY r_car2.k1, r_car2.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_car2.k1,
           r_car2.k2,
           CAST(AVG(r_car2.rv) AS INTEGER),
           MIN(r_car2.rd),
           MAX(r_car2.rd),
           CAST(AVG(s_car2.sv) AS INTEGER),
           MIN(s_car2.sd),
           MAX(s_car2.sd)
    FROM r_car2 JOIN s_car2
      ON r_car2.k1 = s_car2.k1 AND r_car2.k2 = s_car2.k2
    GROUP BY r_car2.k1, r_car2.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car2;
DROP TABLE s_car2;

.print
.print === Composite asym richer mixed, small probe / large build, grouped subset key ===
CREATE TABLE r_car3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_car3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_car3.k1,
           CAST(AVG(r_car3.rv) AS INTEGER),
           MIN(r_car3.rd),
           MAX(r_car3.rd),
           CAST(AVG(s_car3.sv) AS INTEGER),
           MIN(s_car3.sd),
           MAX(s_car3.sd)
    FROM r_car3 JOIN s_car3
      ON r_car3.k1 = s_car3.k1 AND r_car3.k2 = s_car3.k2
    GROUP BY r_car3.k1
) TO '/tmp/aggjoin_composite_asym_richer_mixed_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_car3.k1,
           CAST(AVG(r_car3.rv) AS INTEGER),
           MIN(r_car3.rd),
           MAX(r_car3.rd),
           CAST(AVG(s_car3.sv) AS INTEGER),
           MIN(s_car3.sd),
           MAX(s_car3.sd)
    FROM r_car3 JOIN s_car3
      ON r_car3.k1 = s_car3.k1 AND r_car3.k2 = s_car3.k2
    GROUP BY r_car3.k1
) TO '/tmp/aggjoin_composite_asym_richer_mixed_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car3;
DROP TABLE s_car3;

.print
.print === Composite asym richer mixed, large probe / small build, grouped subset key ===
CREATE TABLE r_car4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_car4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_car4.k1,
           CAST(AVG(r_car4.rv) AS INTEGER),
           MIN(r_car4.rd),
           MAX(r_car4.rd),
           CAST(AVG(s_car4.sv) AS INTEGER),
           MIN(s_car4.sd),
           MAX(s_car4.sd)
    FROM r_car4 JOIN s_car4
      ON r_car4.k1 = s_car4.k1 AND r_car4.k2 = s_car4.k2
    GROUP BY r_car4.k1
) TO '/tmp/aggjoin_composite_asym_richer_mixed_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_car4.k1,
           CAST(AVG(r_car4.rv) AS INTEGER),
           MIN(r_car4.rd),
           MAX(r_car4.rd),
           CAST(AVG(s_car4.sv) AS INTEGER),
           MIN(s_car4.sd),
           MAX(s_car4.sd)
    FROM r_car4 JOIN s_car4
      ON r_car4.k1 = s_car4.k1 AND r_car4.k2 = s_car4.k2
    GROUP BY r_car4.k1
) TO '/tmp/aggjoin_composite_asym_richer_mixed_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car4;
DROP TABLE s_car4;

.print
.print === Composite asym richer mixed, small probe / large build, ungrouped ===
CREATE TABLE r_car5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_car5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT CAST(AVG(r_car5.rv) AS INTEGER),
           MIN(r_car5.rd),
           MAX(r_car5.rd),
           CAST(AVG(s_car5.sv) AS INTEGER),
           MIN(s_car5.sd),
           MAX(s_car5.sd)
    FROM r_car5 JOIN s_car5
      ON r_car5.k1 = s_car5.k1 AND r_car5.k2 = s_car5.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_5a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT CAST(AVG(r_car5.rv) AS INTEGER),
           MIN(r_car5.rd),
           MAX(r_car5.rd),
           CAST(AVG(s_car5.sv) AS INTEGER),
           MIN(s_car5.sd),
           MAX(s_car5.sd)
    FROM r_car5 JOIN s_car5
      ON r_car5.k1 = s_car5.k1 AND r_car5.k2 = s_car5.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_5b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car5;
DROP TABLE s_car5;

.print
.print === Composite asym richer mixed, large probe / small build, ungrouped ===
CREATE TABLE r_car6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS rd
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_car6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2010-01-01' + CAST(i % 7 AS INTEGER) AS sd
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT CAST(AVG(r_car6.rv) AS INTEGER),
           MIN(r_car6.rd),
           MAX(r_car6.rd),
           CAST(AVG(s_car6.sv) AS INTEGER),
           MIN(s_car6.sd),
           MAX(s_car6.sd)
    FROM r_car6 JOIN s_car6
      ON r_car6.k1 = s_car6.k1 AND r_car6.k2 = s_car6.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_6a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT CAST(AVG(r_car6.rv) AS INTEGER),
           MIN(r_car6.rd),
           MAX(r_car6.rd),
           CAST(AVG(s_car6.sv) AS INTEGER),
           MIN(s_car6.sd),
           MAX(s_car6.sd)
    FROM r_car6 JOIN s_car6
      ON r_car6.k1 = s_car6.k1 AND r_car6.k2 = s_car6.k2
) TO '/tmp/aggjoin_composite_asym_richer_mixed_6b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_car6;
DROP TABLE s_car6;
