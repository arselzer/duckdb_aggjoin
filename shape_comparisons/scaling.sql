-- Shape-comparison study: scaling curves
-- Probe-side vs build-side comparison shape as a function of key count (10M rows, SUM)
-- Visualize direct → hash mode transition and crossover point
-- Run: build/Release/duckdb < shape_comparisons/scaling.sql

.timer on

.print === Scaling: 1K keys ===
CREATE TABLE r_sc AS SELECT i % 1000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 1000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_1a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_1b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 10K keys ===
CREATE TABLE r_sc AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 10000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_2a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_2b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 100K keys ===
CREATE TABLE r_sc AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_3a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_3b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 500K keys ===
CREATE TABLE r_sc AS SELECT i % 500000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 500000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_4a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_4b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 1M keys ===
CREATE TABLE r_sc AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_5a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_5b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 2M keys (direct mode boundary) ===
CREATE TABLE r_sc AS SELECT i % 2000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 2000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_6a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_6b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 3M keys (hash mode) ===
CREATE TABLE r_sc AS SELECT i % 3000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 3000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_7a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_7b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;

.print
.print === Scaling: 5M keys (deep hash mode) ===
CREATE TABLE r_sc AS SELECT i % 5000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 5000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_8a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_8b.csv' (FORMAT CSV);
DROP TABLE r_sc; DROP TABLE s_sc;
