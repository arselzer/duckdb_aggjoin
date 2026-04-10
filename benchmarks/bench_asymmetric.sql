-- Benchmark: Asymmetric table size ratios
-- Real Yannakakis CTEs have reduced tables after semi-join reduction
-- Run: build/Release/duckdb < benchmarks/bench_asymmetric.sql

.timer on

.print === Asymmetric: 100K probe × 5M build (small probe, large build) ===
.print --- AGGJOIN ---
CREATE TABLE r_a1 AS SELECT i % 50000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s_a1 AS SELECT i % 50000 AS x, i AS y FROM generate_series(1,5000000) t(i);
COPY (SELECT r_a1.x, SUM(r_a1.val) FROM r_a1 JOIN s_a1 ON r_a1.x = s_a1.x GROUP BY r_a1.x) TO '/tmp/aggjoin_asym_1a.csv' (FORMAT CSV);
.print --- Native ---
COPY (SELECT s_a1.y, SUM(r_a1.val) FROM r_a1 JOIN s_a1 ON r_a1.x = s_a1.x GROUP BY s_a1.y) TO '/tmp/aggjoin_asym_1b.csv' (FORMAT CSV);
DROP TABLE r_a1; DROP TABLE s_a1;

.print
.print === Asymmetric: 10M probe × 1K build (large probe, tiny build) ===
.print --- AGGJOIN ---
CREATE TABLE r_a2 AS SELECT i % 1000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_a2 AS SELECT i AS x FROM generate_series(1,1000) t(i);
COPY (SELECT r_a2.x, SUM(r_a2.val) FROM r_a2 JOIN s_a2 ON r_a2.x = s_a2.x GROUP BY r_a2.x) TO '/tmp/aggjoin_asym_2a.csv' (FORMAT CSV);
.print --- Native ---
COPY (SELECT s_a2.x AS y, SUM(r_a2.val) FROM r_a2 JOIN s_a2 ON r_a2.x = s_a2.x GROUP BY s_a2.x) TO '/tmp/aggjoin_asym_2b.csv' (FORMAT CSV);
DROP TABLE r_a2; DROP TABLE s_a2;

.print
.print === Asymmetric: 1M probe × 100K build (10x ratio) ===
.print --- AGGJOIN ---
CREATE TABLE r_a3 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE s_a3 AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,100000) t(i);
COPY (SELECT r_a3.x, SUM(r_a3.val) FROM r_a3 JOIN s_a3 ON r_a3.x = s_a3.x GROUP BY r_a3.x) TO '/tmp/aggjoin_asym_3a.csv' (FORMAT CSV);
.print --- Native ---
COPY (SELECT s_a3.y, SUM(r_a3.val) FROM r_a3 JOIN s_a3 ON r_a3.x = s_a3.x GROUP BY s_a3.y) TO '/tmp/aggjoin_asym_3b.csv' (FORMAT CSV);
DROP TABLE r_a3; DROP TABLE s_a3;

.print
.print === Asymmetric: 10M probe × 100K build (100x ratio) ===
.print --- AGGJOIN ---
CREATE TABLE r_a4 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_a4 AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,100000) t(i);
COPY (SELECT r_a4.x, SUM(r_a4.val) FROM r_a4 JOIN s_a4 ON r_a4.x = s_a4.x GROUP BY r_a4.x) TO '/tmp/aggjoin_asym_4a.csv' (FORMAT CSV);
.print --- Native ---
COPY (SELECT s_a4.y, SUM(r_a4.val) FROM r_a4 JOIN s_a4 ON r_a4.x = s_a4.x GROUP BY s_a4.y) TO '/tmp/aggjoin_asym_4b.csv' (FORMAT CSV);
DROP TABLE r_a4; DROP TABLE s_a4;
