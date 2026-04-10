-- Benchmark: multi-relation / final-bag candidate shapes
-- Run: build/Release/duckdb < benchmarks/bench_final_bag.sql
--
-- Purpose:
-- 1. measure the current direct 3-way aggregate-over-join shape
-- 2. compare against same-binary native DuckDB (`disabled_optimizers='extension'`)
-- 3. compare against one exact manual tail decomposition that exposes a plausible
--    future planner-side lowering target

.timer on

.print === Final-bag mixed chain, grouped by head key, 100K x / 80K y, 1M rows each ===
CREATE TABLE r_fb1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_fb1 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 1000000) t(i);

CREATE TABLE t_fb1 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 1000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT r_fb1.x,
           SUM(r_fb1.rv),
           SUM(t_fb1.tv),
           MIN(t_fb1.d),
           MAX(t_fb1.d)
    FROM r_fb1
    JOIN s_fb1 ON r_fb1.x = s_fb1.x
    JOIN t_fb1 ON s_fb1.y = t_fb1.y
    GROUP BY r_fb1.x
) TO '/tmp/aggjoin_final_bag_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_fb1.x,
           SUM(r_fb1.rv),
           SUM(t_fb1.tv),
           MIN(t_fb1.d),
           MAX(t_fb1.d)
    FROM r_fb1
    JOIN s_fb1 ON r_fb1.x = s_fb1.x
    JOIN t_fb1 ON s_fb1.y = t_fb1.y
    GROUP BY r_fb1.x
) TO '/tmp/aggjoin_final_bag_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual tail decomposition (candidate lowering) ---
COPY (
    WITH st AS (
        SELECT s_fb1.x,
               COUNT(*) AS freq,
               SUM(t_fb1.tv) AS sum_tv,
               MIN(t_fb1.d) AS min_d,
               MAX(t_fb1.d) AS max_d
        FROM s_fb1
        JOIN t_fb1 ON s_fb1.y = t_fb1.y
        GROUP BY s_fb1.x
    )
    SELECT r_fb1.x,
           SUM(r_fb1.rv * st.freq),
           SUM(st.sum_tv),
           MIN(st.min_d),
           MAX(st.max_d)
    FROM r_fb1
    JOIN st ON r_fb1.x = st.x
    GROUP BY r_fb1.x
) TO '/tmp/aggjoin_final_bag_1c.csv' (FORMAT CSV);

DROP TABLE r_fb1;
DROP TABLE s_fb1;
DROP TABLE t_fb1;

.print
.print === Final-bag numeric chain, grouped by head key, 100K x / 80K y, 1M rows each ===
CREATE TABLE r_fb2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 1000000) t(i);

CREATE TABLE s_fb2 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 1000000) t(i);

CREATE TABLE t_fb2 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv
FROM generate_series(1, 1000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT r_fb2.x,
           SUM(r_fb2.rv),
           SUM(t_fb2.tv),
           AVG(t_fb2.tv)
    FROM r_fb2
    JOIN s_fb2 ON r_fb2.x = s_fb2.x
    JOIN t_fb2 ON s_fb2.y = t_fb2.y
    GROUP BY r_fb2.x
) TO '/tmp/aggjoin_final_bag_2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_fb2.x,
           SUM(r_fb2.rv),
           SUM(t_fb2.tv),
           AVG(t_fb2.tv)
    FROM r_fb2
    JOIN s_fb2 ON r_fb2.x = s_fb2.x
    JOIN t_fb2 ON s_fb2.y = t_fb2.y
    GROUP BY r_fb2.x
) TO '/tmp/aggjoin_final_bag_2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual tail decomposition (candidate lowering) ---
COPY (
    WITH st AS (
        SELECT s_fb2.x,
               COUNT(*) AS freq,
               COUNT(t_fb2.tv) AS cnt_tv,
               SUM(t_fb2.tv) AS sum_tv
        FROM s_fb2
        JOIN t_fb2 ON s_fb2.y = t_fb2.y
        GROUP BY s_fb2.x
    )
    SELECT r_fb2.x,
           SUM(r_fb2.rv * st.freq),
           SUM(st.sum_tv),
           SUM(st.sum_tv) / SUM(st.cnt_tv)
    FROM r_fb2
    JOIN st ON r_fb2.x = st.x
    GROUP BY r_fb2.x
) TO '/tmp/aggjoin_final_bag_2c.csv' (FORMAT CSV);

DROP TABLE r_fb2;
DROP TABLE s_fb2;
DROP TABLE t_fb2;
