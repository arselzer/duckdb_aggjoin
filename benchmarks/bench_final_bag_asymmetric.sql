-- Benchmark: asymmetric multi-relation / final-bag shapes
-- Run: build/Release/duckdb < benchmarks/bench_final_bag_asymmetric.sql

.timer on

.print === Final-bag mixed chain, grouped, small head / large bridge+tail ===
CREATE TABLE r_fba1 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_fba1 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 5000000) t(i);

CREATE TABLE t_fba1 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 5000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT r_fba1.x,
           SUM(r_fba1.rv),
           SUM(t_fba1.tv),
           MIN(t_fba1.d),
           MAX(t_fba1.d)
    FROM r_fba1
    JOIN s_fba1 ON r_fba1.x = s_fba1.x
    JOIN t_fba1 ON s_fba1.y = t_fba1.y
    GROUP BY r_fba1.x
) TO '/tmp/aggjoin_final_bag_a1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_fba1.x,
           SUM(r_fba1.rv),
           SUM(t_fba1.tv),
           MIN(t_fba1.d),
           MAX(t_fba1.d)
    FROM r_fba1
    JOIN s_fba1 ON r_fba1.x = s_fba1.x
    JOIN t_fba1 ON s_fba1.y = t_fba1.y
    GROUP BY r_fba1.x
) TO '/tmp/aggjoin_final_bag_a1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH st AS (
        SELECT s_fba1.x,
               COUNT(*) AS freq,
               SUM(t_fba1.tv) AS sum_tv,
               MIN(t_fba1.d) AS min_d,
               MAX(t_fba1.d) AS max_d
        FROM s_fba1
        JOIN t_fba1 ON s_fba1.y = t_fba1.y
        GROUP BY s_fba1.x
    )
    SELECT r_fba1.x,
           SUM(r_fba1.rv * st.freq),
           SUM(st.sum_tv),
           MIN(st.min_d),
           MAX(st.max_d)
    FROM r_fba1
    JOIN st ON r_fba1.x = st.x
    GROUP BY r_fba1.x
) TO '/tmp/aggjoin_final_bag_a1c.csv' (FORMAT CSV);

DROP TABLE r_fba1;
DROP TABLE s_fba1;
DROP TABLE t_fba1;

.print
.print === Final-bag mixed chain, grouped, large head / small bridge+tail ===
CREATE TABLE r_fba2 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_fba2 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 100000) t(i);

CREATE TABLE t_fba2 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv,
       DATE '2000-01-01' + CAST(i % 10 AS INTEGER) AS d
FROM generate_series(1, 100000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT r_fba2.x,
           SUM(r_fba2.rv),
           SUM(t_fba2.tv),
           MIN(t_fba2.d),
           MAX(t_fba2.d)
    FROM r_fba2
    JOIN s_fba2 ON r_fba2.x = s_fba2.x
    JOIN t_fba2 ON s_fba2.y = t_fba2.y
    GROUP BY r_fba2.x
) TO '/tmp/aggjoin_final_bag_a2a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT r_fba2.x,
           SUM(r_fba2.rv),
           SUM(t_fba2.tv),
           MIN(t_fba2.d),
           MAX(t_fba2.d)
    FROM r_fba2
    JOIN s_fba2 ON r_fba2.x = s_fba2.x
    JOIN t_fba2 ON s_fba2.y = t_fba2.y
    GROUP BY r_fba2.x
) TO '/tmp/aggjoin_final_bag_a2b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH st AS (
        SELECT s_fba2.x,
               COUNT(*) AS freq,
               SUM(t_fba2.tv) AS sum_tv,
               MIN(t_fba2.d) AS min_d,
               MAX(t_fba2.d) AS max_d
        FROM s_fba2
        JOIN t_fba2 ON s_fba2.y = t_fba2.y
        GROUP BY s_fba2.x
    )
    SELECT r_fba2.x,
           SUM(r_fba2.rv * st.freq),
           SUM(st.sum_tv),
           MIN(st.min_d),
           MAX(st.max_d)
    FROM r_fba2
    JOIN st ON r_fba2.x = st.x
    GROUP BY r_fba2.x
) TO '/tmp/aggjoin_final_bag_a2c.csv' (FORMAT CSV);

DROP TABLE r_fba2;
DROP TABLE s_fba2;
DROP TABLE t_fba2;

.print
.print === Final-bag numeric chain, ungrouped, small head / large bridge+tail ===
CREATE TABLE r_fba3 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 100000) t(i);

CREATE TABLE s_fba3 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 5000000) t(i);

CREATE TABLE t_fba3 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv
FROM generate_series(1, 5000000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT SUM(r_fba3.rv),
           SUM(t_fba3.tv),
           AVG(t_fba3.tv)
    FROM r_fba3
    JOIN s_fba3 ON r_fba3.x = s_fba3.x
    JOIN t_fba3 ON s_fba3.y = t_fba3.y
) TO '/tmp/aggjoin_final_bag_a3a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_fba3.rv),
           SUM(t_fba3.tv),
           AVG(t_fba3.tv)
    FROM r_fba3
    JOIN s_fba3 ON r_fba3.x = s_fba3.x
    JOIN t_fba3 ON s_fba3.y = t_fba3.y
) TO '/tmp/aggjoin_final_bag_a3b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH r_x AS (
        SELECT x,
               COUNT(*) AS freq_r,
               SUM(rv) AS sum_rv
        FROM r_fba3
        GROUP BY x
    ),
    st AS (
        SELECT s_fba3.x,
               COUNT(*) AS freq_st,
               COUNT(t_fba3.tv) AS cnt_tv,
               SUM(t_fba3.tv) AS sum_tv
        FROM s_fba3
        JOIN t_fba3 ON s_fba3.y = t_fba3.y
        GROUP BY s_fba3.x
    )
    SELECT SUM(r_x.sum_rv * st.freq_st),
           SUM(st.sum_tv * r_x.freq_r),
           SUM(st.sum_tv * r_x.freq_r) / SUM(st.cnt_tv * r_x.freq_r)
    FROM r_x
    JOIN st ON r_x.x = st.x
) TO '/tmp/aggjoin_final_bag_a3c.csv' (FORMAT CSV);

DROP TABLE r_fba3;
DROP TABLE s_fba3;
DROP TABLE t_fba3;

.print
.print === Final-bag numeric chain, ungrouped, large head / small bridge+tail ===
CREATE TABLE r_fba4 AS
SELECT i % 100000 AS x,
       CAST(random() * 100 AS DOUBLE) AS rv
FROM generate_series(1, 5000000) t(i);

CREATE TABLE s_fba4 AS
SELECT i % 100000 AS x,
       i % 80000 AS y
FROM generate_series(1, 100000) t(i);

CREATE TABLE t_fba4 AS
SELECT i % 80000 AS y,
       CAST(random() * 100 AS DOUBLE) AS tv
FROM generate_series(1, 100000) t(i);

.print --- Direct 3-way query shape (planner decides) ---
COPY (
    SELECT SUM(r_fba4.rv),
           SUM(t_fba4.tv),
           AVG(t_fba4.tv)
    FROM r_fba4
    JOIN s_fba4 ON r_fba4.x = s_fba4.x
    JOIN t_fba4 ON s_fba4.y = t_fba4.y
) TO '/tmp/aggjoin_final_bag_a4a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT SUM(r_fba4.rv),
           SUM(t_fba4.tv),
           AVG(t_fba4.tv)
    FROM r_fba4
    JOIN s_fba4 ON r_fba4.x = s_fba4.x
    JOIN t_fba4 ON s_fba4.y = t_fba4.y
) TO '/tmp/aggjoin_final_bag_a4b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

.print --- Manual decomposition (candidate lowering) ---
COPY (
    WITH r_x AS (
        SELECT x,
               COUNT(*) AS freq_r,
               SUM(rv) AS sum_rv
        FROM r_fba4
        GROUP BY x
    ),
    st AS (
        SELECT s_fba4.x,
               COUNT(*) AS freq_st,
               COUNT(t_fba4.tv) AS cnt_tv,
               SUM(t_fba4.tv) AS sum_tv
        FROM s_fba4
        JOIN t_fba4 ON s_fba4.y = t_fba4.y
        GROUP BY s_fba4.x
    )
    SELECT SUM(r_x.sum_rv * st.freq_st),
           SUM(st.sum_tv * r_x.freq_r),
           SUM(st.sum_tv * r_x.freq_r) / SUM(st.cnt_tv * r_x.freq_r)
    FROM r_x
    JOIN st ON r_x.x = st.x
) TO '/tmp/aggjoin_final_bag_a4c.csv' (FORMAT CSV);

DROP TABLE r_fba4;
DROP TABLE s_fba4;
DROP TABLE t_fba4;
