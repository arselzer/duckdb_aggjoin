-- Benchmark: composite-key mixed probe/build aggregate shape grouped by a
-- subset of the join key.
-- Run: build/Release/duckdb < benchmarks/bench_composite_subset_probe_build_mixed.sql

.timer on
PRAGMA threads=1;

.print === Composite mixed grouped by subset key, probe SUM + build SUM+DATE MIN+MAX, 1Kx100 keys, 1M rows ===
CREATE TABLE r_csbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_csbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_csbm1.k1,
           SUM(r_csbm1.rv),
           SUM(s_csbm1.sv),
           MIN(s_csbm1.d),
           MAX(s_csbm1.d)
    FROM r_csbm1 JOIN s_csbm1
      ON r_csbm1.k1 = s_csbm1.k1
     AND r_csbm1.k2 = s_csbm1.k2
    GROUP BY r_csbm1.k1
) TO '/tmp/aggjoin_composite_subset_probe_build_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_csbm1.k1,
           SUM(r_csbm1.rv),
           SUM(s_csbm1.sv),
           MIN(s_csbm1.d),
           MAX(s_csbm1.d)
    FROM r_csbm1 JOIN s_csbm1
      ON r_csbm1.k1 = s_csbm1.k1
     AND r_csbm1.k2 = s_csbm1.k2
    GROUP BY r_csbm1.k1
) TO '/tmp/aggjoin_composite_subset_probe_build_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_csbm1;
DROP TABLE s_csbm1;
