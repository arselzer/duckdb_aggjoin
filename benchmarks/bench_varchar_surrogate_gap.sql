-- Benchmark: estimate remaining headroom for broader VARCHAR support
-- by comparing the current string-key path against the same workload on
-- precomputed integer surrogate keys.
-- Run: build/Release/duckdb < benchmarks/bench_varchar_surrogate_gap.sql

.timer on

.print === VARCHAR vs surrogate-id grouped SUM+MIN+MAX, 1K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vgap AS
SELECT i % 1000 AS xid,
       'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vgap AS
SELECT i % 1000 AS xid,
       'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vgap.x, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.x = s_vgap.x
    GROUP BY r_vgap.x
) TO '/tmp/varchar_surrogate_gap_1k_string.csv' (HEADER, DELIMITER ',');

.print --- Integer surrogate upper bound ---
COPY (
    SELECT r_vgap.xid, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.xid = s_vgap.xid
    GROUP BY r_vgap.xid
) TO '/tmp/varchar_surrogate_gap_1k_int.csv' (HEADER, DELIMITER ',');

.print
.print === VARCHAR vs surrogate-id grouped SUM+MIN+MAX, 5K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vgap AS
SELECT i % 5000 AS xid,
       'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vgap AS
SELECT i % 5000 AS xid,
       'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vgap.x, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.x = s_vgap.x
    GROUP BY r_vgap.x
) TO '/tmp/varchar_surrogate_gap_5k_string.csv' (HEADER, DELIMITER ',');

.print --- Integer surrogate upper bound ---
COPY (
    SELECT r_vgap.xid, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.xid = s_vgap.xid
    GROUP BY r_vgap.xid
) TO '/tmp/varchar_surrogate_gap_5k_int.csv' (HEADER, DELIMITER ',');

.print
.print === VARCHAR vs surrogate-id grouped SUM+MIN+MAX, 10K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vgap AS
SELECT i % 10000 AS xid,
       'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vgap AS
SELECT i % 10000 AS xid,
       'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vgap.x, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.x = s_vgap.x
    GROUP BY r_vgap.x
) TO '/tmp/varchar_surrogate_gap_10k_string.csv' (HEADER, DELIMITER ',');

.print --- Integer surrogate upper bound ---
COPY (
    SELECT r_vgap.xid, SUM(r_vgap.val) AS s, MIN(r_vgap.val) AS mn, MAX(r_vgap.val) AS mx
    FROM r_vgap JOIN s_vgap ON r_vgap.xid = s_vgap.xid
    GROUP BY r_vgap.xid
) TO '/tmp/varchar_surrogate_gap_10k_int.csv' (HEADER, DELIMITER ',');
