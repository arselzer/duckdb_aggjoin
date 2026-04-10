-- Benchmark: ungrouped composite-key mixed probe/build aggregate shape
-- Run: build/Release/duckdb < benchmarks/bench_ungrouped_composite_probe_build_mixed.sql

.timer on
PRAGMA threads=1;

.print === Ungrouped composite mixed, probe AVG + build AVG+DATE MIN+MAX, 1Kx100 keys, 1M rows ===
CREATE TABLE r_ucpbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_ucpbm1 AS
SELECT i % 1000 AS k1,
       (i / 1000) % 100 AS k2,
       CAST(random() * 100 AS DOUBLE) AS sv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct query shape (planner decides) ---
COPY (
    SELECT AVG(r_ucpbm1.rv),
           AVG(s_ucpbm1.sv),
           MIN(s_ucpbm1.d),
           MAX(s_ucpbm1.d)
    FROM r_ucpbm1 JOIN s_ucpbm1
      ON r_ucpbm1.k1 = s_ucpbm1.k1
     AND r_ucpbm1.k2 = s_ucpbm1.k2
) TO '/tmp/aggjoin_ungrouped_composite_probe_build_mixed_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT AVG(r_ucpbm1.rv),
           AVG(s_ucpbm1.sv),
           MIN(s_ucpbm1.d),
           MAX(s_ucpbm1.d)
    FROM r_ucpbm1 JOIN s_ucpbm1
      ON r_ucpbm1.k1 = s_ucpbm1.k1
     AND r_ucpbm1.k2 = s_ucpbm1.k2
) TO '/tmp/aggjoin_ungrouped_composite_probe_build_mixed_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE r_ucpbm1;
DROP TABLE s_ucpbm1;
