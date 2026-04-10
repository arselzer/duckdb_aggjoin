-- Benchmark: asymmetric composite-key probe-side non-numeric + build-side numeric shapes
-- Run: build/Release/duckdb < benchmarks/bench_composite_asymmetric_probe_nonnumeric_mixed.sql

.timer on

.print === Composite asymmetric, small probe / large build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_capnm1 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_capnm1.k1,
           r_capnm1.k2,
           MIN(r_capnm1.d),
           MAX(r_capnm1.d),
           SUM(s_capnm1.sv)
    FROM r_capnm1 JOIN s_capnm1
      ON r_capnm1.k1 = s_capnm1.k1 AND r_capnm1.k2 = s_capnm1.k2
    GROUP BY r_capnm1.k1, r_capnm1.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_capnm1.k1,
           r_capnm1.k2,
           MIN(r_capnm1.d),
           MAX(r_capnm1.d),
           SUM(s_capnm1.sv)
    FROM r_capnm1 JOIN s_capnm1
      ON r_capnm1.k1 = s_capnm1.k1 AND r_capnm1.k2 = s_capnm1.k2
    GROUP BY r_capnm1.k1, r_capnm1.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm1;
DROP TABLE s_capnm1;

.print
.print === Composite asymmetric, large probe / small build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_capnm2 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_capnm2.k1,
           r_capnm2.k2,
           MIN(r_capnm2.d),
           MAX(r_capnm2.d),
           SUM(s_capnm2.sv)
    FROM r_capnm2 JOIN s_capnm2
      ON r_capnm2.k1 = s_capnm2.k1 AND r_capnm2.k2 = s_capnm2.k2
    GROUP BY r_capnm2.k1, r_capnm2.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_capnm2.k1,
           r_capnm2.k2,
           MIN(r_capnm2.d),
           MAX(r_capnm2.d),
           SUM(s_capnm2.sv)
    FROM r_capnm2 JOIN s_capnm2
      ON r_capnm2.k1 = s_capnm2.k1 AND r_capnm2.k2 = s_capnm2.k2
    GROUP BY r_capnm2.k1, r_capnm2.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm2;
DROP TABLE s_capnm2;

.print
.print === Composite asymmetric subset-key, small probe / large build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_capnm3 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_capnm3.k1,
           MIN(r_capnm3.d),
           MAX(r_capnm3.d),
           SUM(s_capnm3.sv)
    FROM r_capnm3 JOIN s_capnm3
      ON r_capnm3.k1 = s_capnm3.k1 AND r_capnm3.k2 = s_capnm3.k2
    GROUP BY r_capnm3.k1
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_capnm3.k1,
           MIN(r_capnm3.d),
           MAX(r_capnm3.d),
           SUM(s_capnm3.sv)
    FROM r_capnm3 JOIN s_capnm3
      ON r_capnm3.k1 = s_capnm3.k1 AND r_capnm3.k2 = s_capnm3.k2
    GROUP BY r_capnm3.k1
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm3;
DROP TABLE s_capnm3;

.print
.print === Composite asymmetric subset-key, large probe / small build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_capnm4 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_capnm4.k1,
           MIN(r_capnm4.d),
           MAX(r_capnm4.d),
           SUM(s_capnm4.sv)
    FROM r_capnm4 JOIN s_capnm4
      ON r_capnm4.k1 = s_capnm4.k1 AND r_capnm4.k2 = s_capnm4.k2
    GROUP BY r_capnm4.k1
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_capnm4.k1,
           MIN(r_capnm4.d),
           MAX(r_capnm4.d),
           SUM(s_capnm4.sv)
    FROM r_capnm4 JOIN s_capnm4
      ON r_capnm4.k1 = s_capnm4.k1 AND r_capnm4.k2 = s_capnm4.k2
    GROUP BY r_capnm4.k1
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm4;
DROP TABLE s_capnm4;

.print
.print === Composite asymmetric ungrouped, small probe / large build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_capnm5 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 5000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_capnm5.d),
           MAX(r_capnm5.d),
           SUM(s_capnm5.sv)
    FROM r_capnm5 JOIN s_capnm5
      ON r_capnm5.k1 = s_capnm5.k1 AND r_capnm5.k2 = s_capnm5.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_5a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_capnm5.d),
           MAX(r_capnm5.d),
           SUM(s_capnm5.sv)
    FROM r_capnm5 JOIN s_capnm5
      ON r_capnm5.k1 = s_capnm5.k1 AND r_capnm5.k2 = s_capnm5.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_5b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm5;
DROP TABLE s_capnm5;

.print
.print === Composite asymmetric ungrouped, large probe / small build, probe DATE MIN+MAX + build SUM ===
CREATE TABLE r_capnm6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_capnm6 AS
SELECT i % 1000 AS k1,
       i % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv
FROM generate_series(1, 100000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT MIN(r_capnm6.d),
           MAX(r_capnm6.d),
           SUM(s_capnm6.sv)
    FROM r_capnm6 JOIN s_capnm6
      ON r_capnm6.k1 = s_capnm6.k1 AND r_capnm6.k2 = s_capnm6.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_6a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT MIN(r_capnm6.d),
           MAX(r_capnm6.d),
           SUM(s_capnm6.sv)
    FROM r_capnm6 JOIN s_capnm6
      ON r_capnm6.k1 = s_capnm6.k1 AND r_capnm6.k2 = s_capnm6.k2
) TO '/tmp/aggjoin_composite_asym_probe_nonnumeric_6b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_capnm6;
DROP TABLE s_capnm6;
