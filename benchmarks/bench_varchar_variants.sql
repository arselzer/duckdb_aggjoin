-- Benchmark: narrow VARCHAR-key variants around the current win region
-- Measures grouped COUNT(*) and AVG on single-key VARCHAR shapes.
-- Run: build/Release/duckdb < benchmarks/bench_varchar_variants.sql

.timer on

.print === Dense VARCHAR keys, grouped COUNT(*), 1K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vvar AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vvar AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vvar.x, COUNT(*) AS c
    FROM r_vvar JOIN s_vvar ON r_vvar.x = s_vvar.x
    GROUP BY r_vvar.x
) TO '/tmp/varchar_variants_count_1k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vvar.x, COUNT(*) AS c
    FROM r_vvar JOIN s_vvar ON r_vvar.x = s_vvar.x
    GROUP BY r_vvar.x
) TO '/tmp/varchar_variants_count_1k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Dense VARCHAR keys, grouped AVG, 1K keys / 100K rows ===
.print --- Direct query ---
COPY (
    SELECT r_vvar.x, AVG(r_vvar.val) AS a
    FROM r_vvar JOIN s_vvar ON r_vvar.x = s_vvar.x
    GROUP BY r_vvar.x
) TO '/tmp/varchar_variants_avg_1k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vvar.x, AVG(r_vvar.val) AS a
    FROM r_vvar JOIN s_vvar ON r_vvar.x = s_vvar.x
    GROUP BY r_vvar.x
) TO '/tmp/varchar_variants_avg_1k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Boundary VARCHAR keys, grouped AVG, 5K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vvar_b AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vvar_b AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vvar_b.x, AVG(r_vvar_b.val) AS a
    FROM r_vvar_b JOIN s_vvar_b ON r_vvar_b.x = s_vvar_b.x
    GROUP BY r_vvar_b.x
) TO '/tmp/varchar_variants_avg_5k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vvar_b.x, AVG(r_vvar_b.val) AS a
    FROM r_vvar_b JOIN s_vvar_b ON r_vvar_b.x = s_vvar_b.x
    GROUP BY r_vvar_b.x
) TO '/tmp/varchar_variants_avg_5k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Boundary VARCHAR keys, grouped AVG, 10K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vvar_c AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vvar_c AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vvar_c.x, AVG(r_vvar_c.val) AS a
    FROM r_vvar_c JOIN s_vvar_c ON r_vvar_c.x = s_vvar_c.x
    GROUP BY r_vvar_c.x
) TO '/tmp/varchar_variants_avg_10k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vvar_c.x, AVG(r_vvar_c.val) AS a
    FROM r_vvar_c JOIN s_vvar_c ON r_vvar_c.x = s_vvar_c.x
    GROUP BY r_vvar_c.x
) TO '/tmp/varchar_variants_avg_10k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';
