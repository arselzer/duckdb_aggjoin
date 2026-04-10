-- Benchmark: narrow dense VARCHAR-key path
-- Direct grouped/ungrouped aggregate-over-join queries using the current
-- single-key VARCHAR numeric SUM/COUNT/AVG/MIN/MAX hash path.
-- Run: build/Release/duckdb < benchmarks/bench_varchar_dense.sql

.timer on

.print === Dense VARCHAR keys, grouped SUM, 1K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vdense AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 4, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vdense AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 4, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- AGGJOIN / narrow VARCHAR path ---
COPY (
    SELECT r_vdense.x, SUM(r_vdense.val) AS s
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
    GROUP BY r_vdense.x
) TO '/tmp/varchar_dense_grouped_aggjoin.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vdense.x, SUM(r_vdense.val) AS s
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
    GROUP BY r_vdense.x
) TO '/tmp/varchar_dense_grouped_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Dense VARCHAR keys, grouped SUM+MIN+MAX, 1K keys / 100K rows ===
.print --- AGGJOIN / narrow VARCHAR path ---
COPY (
    SELECT r_vdense.x, SUM(r_vdense.val) AS s, MIN(r_vdense.val) AS mn, MAX(r_vdense.val) AS mx
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
    GROUP BY r_vdense.x
) TO '/tmp/varchar_dense_grouped_minmax_aggjoin.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vdense.x, SUM(r_vdense.val) AS s, MIN(r_vdense.val) AS mn, MAX(r_vdense.val) AS mx
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
    GROUP BY r_vdense.x
) TO '/tmp/varchar_dense_grouped_minmax_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Dense VARCHAR keys, ungrouped SUM, 1K keys / 100K rows ===
.print --- AGGJOIN / narrow VARCHAR path ---
COPY (
    SELECT SUM(r_vdense.val) AS s
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
) TO '/tmp/varchar_dense_ungrouped_aggjoin.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_vdense.val) AS s
    FROM r_vdense JOIN s_vdense ON r_vdense.x = s_vdense.x
) TO '/tmp/varchar_dense_ungrouped_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print
.print === Boundary VARCHAR keys, grouped MIN+MAX, 10K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vdense_b AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vdense_b AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Direct query ---
COPY (
    SELECT r_vdense_b.x, MIN(r_vdense_b.val) AS mn, MAX(r_vdense_b.val) AS mx
    FROM r_vdense_b JOIN s_vdense_b ON r_vdense_b.x = s_vdense_b.x
    GROUP BY r_vdense_b.x
) TO '/tmp/varchar_boundary_grouped_minmax_direct.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vdense_b.x, MIN(r_vdense_b.val) AS mn, MAX(r_vdense_b.val) AS mx
    FROM r_vdense_b JOIN s_vdense_b ON r_vdense_b.x = s_vdense_b.x
    GROUP BY r_vdense_b.x
) TO '/tmp/varchar_boundary_grouped_minmax_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';
