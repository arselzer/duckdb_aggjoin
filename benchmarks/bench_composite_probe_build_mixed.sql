-- Benchmark: composite-key mixed probe/build aggregate shape
-- Run: build/Release/duckdb < benchmarks/bench_composite_probe_build_mixed.sql

.timer on
PRAGMA threads=1;

.print === Composite mixed grouped-by-key, probe AVG + build AVG+DATE MIN+MAX, 1Kx100 keys, 1M rows ===
CREATE TABLE r_cbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_cbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT r_cbm1.k1,
           r_cbm1.k2,
           AVG(r_cbm1.rv),
           AVG(s_cbm1.sv),
           MIN(s_cbm1.d),
           MAX(s_cbm1.d)
    FROM r_cbm1 JOIN s_cbm1
      ON r_cbm1.k1 = s_cbm1.k1
     AND r_cbm1.k2 = s_cbm1.k2
    GROUP BY r_cbm1.k1, r_cbm1.k2
) TO '/tmp/aggjoin_composite_probe_build_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_cbm1.k1,
           r_cbm1.k2,
           AVG(r_cbm1.rv),
           AVG(s_cbm1.sv),
           MIN(s_cbm1.d),
           MAX(s_cbm1.d)
    FROM r_cbm1 JOIN s_cbm1
      ON r_cbm1.k1 = s_cbm1.k1
     AND r_cbm1.k2 = s_cbm1.k2
    GROUP BY r_cbm1.k1, r_cbm1.k2
) TO '/tmp/aggjoin_composite_probe_build_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_cbm1;
DROP TABLE s_cbm1;
