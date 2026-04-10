-- Benchmark: ungrouped multi-relation / final-bag candidate shapes
-- Run: build/Release/duckdb < benchmarks/bench_final_bag_ungrouped.sql
--
-- Purpose:
-- 1. measure the current direct ungrouped 3-way aggregate-over-join shape
-- 2. compare against same-binary native DuckDB (`disabled_optimizers='extension'`)
-- 3. compare against one exact manual decomposition that exposes a plausible
--    future planner-side lowering target

.timer on

.print === Final-bag mixed chain, ungrouped, 100K x / 80K y, 1M rows each ===
CREATE TABLE r_fbu1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_fbu1 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 1000000) t(i);

CREATE TABLE t_fbu1 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT SUM(r_fbu1.rv),
           SUM(t_fbu1.tv),
           MIN(t_fbu1.d),
           MAX(t_fbu1.d)
    FROM r_fbu1
    JOIN s_fbu1 ON r_fbu1.x = s_fbu1.x
    JOIN t_fbu1 ON s_fbu1.y = t_fbu1.y
) TO '/tmp/aggjoin_final_bag_u1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_fbu1.rv),
           SUM(t_fbu1.tv),
           MIN(t_fbu1.d),
           MAX(t_fbu1.d)
    FROM r_fbu1
    JOIN s_fbu1 ON r_fbu1.x = s_fbu1.x
    JOIN t_fbu1 ON s_fbu1.y = t_fbu1.y
) TO '/tmp/aggjoin_final_bag_u1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH r_x AS (
        SELECT x,
               COUNT(*) AS freq_r,
               SUM(rv) AS sum_rv
        FROM r_fbu1
        GROUP BY x
    ),
    st AS (
        SELECT s_fbu1.x,
               COUNT(*) AS freq_st,
               SUM(t_fbu1.tv) AS sum_tv,
               MIN(t_fbu1.d) AS min_d,
               MAX(t_fbu1.d) AS max_d
        FROM s_fbu1
        JOIN t_fbu1 ON s_fbu1.y = t_fbu1.y
        GROUP BY s_fbu1.x
    )
    SELECT SUM(r_x.sum_rv * st.freq_st),
           SUM(st.sum_tv * r_x.freq_r),
           MIN(st.min_d),
           MAX(st.max_d)
    FROM r_x
    JOIN st ON r_x.x = st.x
) TO '/tmp/aggjoin_final_bag_u1c.csv' (FORMAT CSV);

DROP TABLE r_fbu1;
DROP TABLE s_fbu1;
DROP TABLE t_fbu1;

.print
.print === Final-bag numeric chain, ungrouped, 100K x / 80K y, 1M rows each ===
CREATE TABLE r_fbu2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_fbu2 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 1000000) t(i);

CREATE TABLE t_fbu2 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv
FROM generate_series(1, 1000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT SUM(r_fbu2.rv),
           SUM(t_fbu2.tv),
           AVG(t_fbu2.tv)
    FROM r_fbu2
    JOIN s_fbu2 ON r_fbu2.x = s_fbu2.x
    JOIN t_fbu2 ON s_fbu2.y = t_fbu2.y
) TO '/tmp/aggjoin_final_bag_u2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_fbu2.rv),
           SUM(t_fbu2.tv),
           AVG(t_fbu2.tv)
    FROM r_fbu2
    JOIN s_fbu2 ON r_fbu2.x = s_fbu2.x
    JOIN t_fbu2 ON s_fbu2.y = t_fbu2.y
) TO '/tmp/aggjoin_final_bag_u2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH r_x AS (
        SELECT x,
               COUNT(*) AS freq_r,
               SUM(rv) AS sum_rv
        FROM r_fbu2
        GROUP BY x
    ),
    st AS (
        SELECT s_fbu2.x,
               COUNT(*) AS freq_st,
               COUNT(t_fbu2.tv) AS cnt_tv,
               SUM(t_fbu2.tv) AS sum_tv
        FROM s_fbu2
        JOIN t_fbu2 ON s_fbu2.y = t_fbu2.y
        GROUP BY s_fbu2.x
    )
    SELECT SUM(r_x.sum_rv * st.freq_st),
           SUM(st.sum_tv * r_x.freq_r),
           SUM(st.sum_tv * r_x.freq_r) / SUM(st.cnt_tv * r_x.freq_r)
    FROM r_x
    JOIN st ON r_x.x = st.x
) TO '/tmp/aggjoin_final_bag_u2c.csv' (FORMAT CSV);

DROP TABLE r_fbu2;
DROP TABLE s_fbu2;
DROP TABLE t_fbu2;
