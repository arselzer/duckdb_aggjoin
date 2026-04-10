-- Benchmark: phase-0 matrix for a future semantic-id grouped VARCHAR project.
-- Compares:
--   1. current grouped VARCHAR path (including current planner rewrites)
--   2. same-binary native baseline
--   3. explicit dictionary/id lowering as a semantic-id proxy
-- Run: build/Release/duckdb < benchmarks/bench_varchar_semantic_id_matrix.sql

.timer on

.print === Semantic-id matrix, grouped SUM+MIN+MAX, 500K rows / 10K keys ===
CREATE OR REPLACE TABLE r_vsid AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 500000) t(i);

CREATE OR REPLACE TABLE s_vsid AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 500000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vsid.x, SUM(r_vsid.val) AS s, MIN(r_vsid.val) AS mn, MAX(r_vsid.val) AS mx
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_summinmax_500k_10k_current.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsid.x, SUM(r_vsid.val) AS s, MIN(r_vsid.val) AS mn, MAX(r_vsid.val) AS mx
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_summinmax_500k_10k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vsid) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vsid.val
        FROM r_vsid JOIN dict ON r_vsid.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vsid JOIN dict ON s_vsid.x = dict.x
    )
    SELECT r_enc.xid, SUM(r_enc.val) AS s, MIN(r_enc.val) AS mn, MAX(r_enc.val) AS mx
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_semantic_id_matrix_summinmax_500k_10k_dict.csv' (HEADER, DELIMITER ',');

.print
.print === Semantic-id matrix, grouped SUM+MIN+MAX, 1M rows / 20K keys ===
CREATE OR REPLACE TABLE r_vsid AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 1000000) t(i);

CREATE OR REPLACE TABLE s_vsid AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 1000000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vsid.x, SUM(r_vsid.val) AS s, MIN(r_vsid.val) AS mn, MAX(r_vsid.val) AS mx
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_summinmax_1m_20k_current.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsid.x, SUM(r_vsid.val) AS s, MIN(r_vsid.val) AS mn, MAX(r_vsid.val) AS mx
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_summinmax_1m_20k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vsid) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vsid.val
        FROM r_vsid JOIN dict ON r_vsid.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vsid JOIN dict ON s_vsid.x = dict.x
    )
    SELECT r_enc.xid, SUM(r_enc.val) AS s, MIN(r_enc.val) AS mn, MAX(r_enc.val) AS mx
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_semantic_id_matrix_summinmax_1m_20k_dict.csv' (HEADER, DELIMITER ',');

.print
.print === Semantic-id matrix, grouped AVG, 500K rows / 10K keys ===
CREATE OR REPLACE TABLE r_vsid AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 500000) t(i);

CREATE OR REPLACE TABLE s_vsid AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 500000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vsid.x, AVG(r_vsid.val) AS a
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_avg_500k_10k_current.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsid.x, AVG(r_vsid.val) AS a
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_avg_500k_10k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vsid) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vsid.val
        FROM r_vsid JOIN dict ON r_vsid.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vsid JOIN dict ON s_vsid.x = dict.x
    )
    SELECT r_enc.xid, AVG(r_enc.val) AS a
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_semantic_id_matrix_avg_500k_10k_dict.csv' (HEADER, DELIMITER ',');

.print
.print === Semantic-id matrix, grouped COUNT(*), 500K rows / 20K keys ===
CREATE OR REPLACE TABLE r_vsid AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 500000) t(i);

CREATE OR REPLACE TABLE s_vsid AS
SELECT 'k' || LPAD(CAST(i % 20000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 500000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vsid.x, COUNT(*) AS c
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_count_500k_20k_current.csv' (HEADER, DELIMITER ',');

.print --- Native baseline ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_vsid.x, COUNT(*) AS c
    FROM r_vsid JOIN s_vsid ON r_vsid.x = s_vsid.x
    GROUP BY r_vsid.x
) TO '/tmp/varchar_semantic_id_matrix_count_500k_20k_native.csv' (HEADER, DELIMITER ',');
PRAGMA disabled_optimizers='';

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vsid) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM r_vsid JOIN dict ON r_vsid.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vsid JOIN dict ON s_vsid.x = dict.x
    )
    SELECT r_enc.xid, COUNT(*) AS c
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_semantic_id_matrix_count_500k_20k_dict.csv' (HEADER, DELIMITER ',');
