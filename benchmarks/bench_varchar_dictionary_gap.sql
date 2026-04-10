-- Benchmark: realistic semantic-id headroom for grouped single-key VARCHAR.
-- Compares:
--   1. current narrow VARCHAR path
--   2. explicit dictionary lowering that builds a dictionary, encodes both sides,
--      and then runs the same grouped numeric aggregate query on integer ids
-- Run: build/Release/duckdb < benchmarks/bench_varchar_dictionary_gap.sql

.timer on

.print === VARCHAR vs explicit dictionary lowering, grouped SUM+MIN+MAX, 1K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vdict AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vdict AS
SELECT 'k' || LPAD(CAST(i % 1000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vdict.x, SUM(r_vdict.val) AS s, MIN(r_vdict.val) AS mn, MAX(r_vdict.val) AS mx
    FROM r_vdict JOIN s_vdict ON r_vdict.x = s_vdict.x
    GROUP BY r_vdict.x
) TO '/tmp/varchar_dictionary_gap_1k_string.csv' (HEADER, DELIMITER ',');

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vdict) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vdict.val
        FROM r_vdict JOIN dict ON r_vdict.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vdict JOIN dict ON s_vdict.x = dict.x
    )
    SELECT r_enc.xid, SUM(r_enc.val) AS s, MIN(r_enc.val) AS mn, MAX(r_enc.val) AS mx
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_dictionary_gap_1k_dict.csv' (HEADER, DELIMITER ',');

.print
.print === VARCHAR vs explicit dictionary lowering, grouped SUM+MIN+MAX, 5K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vdict AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vdict AS
SELECT 'k' || LPAD(CAST(i % 5000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vdict.x, SUM(r_vdict.val) AS s, MIN(r_vdict.val) AS mn, MAX(r_vdict.val) AS mx
    FROM r_vdict JOIN s_vdict ON r_vdict.x = s_vdict.x
    GROUP BY r_vdict.x
) TO '/tmp/varchar_dictionary_gap_5k_string.csv' (HEADER, DELIMITER ',');

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vdict) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vdict.val
        FROM r_vdict JOIN dict ON r_vdict.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vdict JOIN dict ON s_vdict.x = dict.x
    )
    SELECT r_enc.xid, SUM(r_enc.val) AS s, MIN(r_enc.val) AS mn, MAX(r_enc.val) AS mx
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_dictionary_gap_5k_dict.csv' (HEADER, DELIMITER ',');

.print
.print === VARCHAR vs explicit dictionary lowering, grouped SUM+MIN+MAX, 10K keys / 100K rows ===
CREATE OR REPLACE TABLE r_vdict AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x,
       CAST(random() * 100 AS DOUBLE) AS val
FROM generate_series(1, 100000) t(i);

CREATE OR REPLACE TABLE s_vdict AS
SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 5, '0') AS x
FROM generate_series(1, 100000) t(i);

.print --- Current VARCHAR path ---
COPY (
    SELECT r_vdict.x, SUM(r_vdict.val) AS s, MIN(r_vdict.val) AS mn, MAX(r_vdict.val) AS mx
    FROM r_vdict JOIN s_vdict ON r_vdict.x = s_vdict.x
    GROUP BY r_vdict.x
) TO '/tmp/varchar_dictionary_gap_10k_string.csv' (HEADER, DELIMITER ',');

.print --- Explicit dictionary lowering ---
COPY (
    WITH dict AS MATERIALIZED (
        SELECT x, row_number() OVER () - 1 AS xid
        FROM (SELECT DISTINCT x FROM s_vdict) d
    ),
    r_enc AS MATERIALIZED (
        SELECT dict.xid, r_vdict.val
        FROM r_vdict JOIN dict ON r_vdict.x = dict.x
    ),
    s_enc AS MATERIALIZED (
        SELECT dict.xid
        FROM s_vdict JOIN dict ON s_vdict.x = dict.x
    )
    SELECT r_enc.xid, SUM(r_enc.val) AS s, MIN(r_enc.val) AS mn, MAX(r_enc.val) AS mx
    FROM r_enc JOIN s_enc ON r_enc.xid = s_enc.xid
    GROUP BY r_enc.xid
) TO '/tmp/varchar_dictionary_gap_10k_dict.csv' (HEADER, DELIMITER ',');
