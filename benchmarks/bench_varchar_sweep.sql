-- Benchmark: grouped VARCHAR-key sweep for the narrow varlen path
-- Measures where the current planner/runtime combination stops being a clear win.
-- Run: build/Release/duckdb < benchmarks/bench_varchar_sweep.sql

.timer on

.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 1K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_1k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_1k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 2K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 2000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 2000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_2k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_2k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 5K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_5k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_5k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 10K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_10k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_10k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 20K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_20k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_20k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === VARCHAR grouped SUM+MIN+MAX sweep, 100K rows / 50K keys ===
CREATE OR REPLACE TABLE r_vsweep AS
SELECT 'k' || LPAD(CAST(i % 50000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vsweep AS
SELECT 'k' || LPAD(CAST(i % 50000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_50k_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsweep.x, SUM(r_vsweep.val) AS s, MIN(r_vsweep.val) AS mn, MAX(r_vsweep.val) AS mx
    FROM r_vsweep JOIN s_vsweep ON r_vsweep.x = s_vsweep.x
    GROUP BY r_vsweep.x
) TO '/tmp/varchar_sweep_50k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';
