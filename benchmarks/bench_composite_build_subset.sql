-- Benchmark: composite-key build-heavy aggregate shape grouped by a subset of
-- the join key.
-- Run: build/Release/duckdb < benchmarks/bench_composite_build_subset.sql

.timer on
PRAGMA threads=1;

.print === Composite build-side grouped by subset key, build AVG+DATE MIN+MAX, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cbs2 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cbs2 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cbs2.k1,
           AVG(s_cbs2.sv),
           MIN(s_cbs2.d),
           MAX(s_cbs2.d)
    FROM r_cbs2 JOIN s_cbs2
      ON r_cbs2.k1 = s_cbs2.k1
     AND r_cbs2.k2 = s_cbs2.k2
    GROUP BY r_cbs2.k1
) TO '/tmp/aggjoin_composite_build_subset_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cbs2.k1,
           AVG(s_cbs2.sv),
           MIN(s_cbs2.d),
           MAX(s_cbs2.d)
    FROM r_cbs2 JOIN s_cbs2
      ON r_cbs2.k1 = s_cbs2.k1
     AND r_cbs2.k2 = s_cbs2.k2
    GROUP BY r_cbs2.k1
) TO '/tmp/aggjoin_composite_build_subset_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cbs2;
DROP TABLE s_cbs2;
