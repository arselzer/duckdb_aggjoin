-- Benchmark: NULL-heavy data
-- Tests impact of NULL filtering on accumulation loop
-- Run: build/Release/duckdb < benchmarks/bench_nulls.sql

.timer on

.print === 0% NULLs (baseline) ===
.print --- AGGJOIN ---
CREATE TABLE r_n0 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_n0 AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT COUNT(*) FROM (SELECT r_n0.x, SUM(r_n0.val) FROM r_n0 JOIN s_n0 ON r_n0.x = s_n0.x GROUP BY r_n0.x);
DROP TABLE r_n0; DROP TABLE s_n0;

.print
.print === 10% NULLs in aggregate column ===
.print --- AGGJOIN ---
CREATE TABLE r_n10 AS SELECT i % 100000 AS x, CASE WHEN random() < 0.1 THEN NULL ELSE CAST(random()*100 AS DOUBLE) END AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_n10 AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT COUNT(*) FROM (SELECT r_n10.x, SUM(r_n10.val) FROM r_n10 JOIN s_n10 ON r_n10.x = s_n10.x GROUP BY r_n10.x);
DROP TABLE r_n10; DROP TABLE s_n10;

.print
.print === 50% NULLs in aggregate column ===
.print --- AGGJOIN ---
CREATE TABLE r_n50 AS SELECT i % 100000 AS x, CASE WHEN random() < 0.5 THEN NULL ELSE CAST(random()*100 AS DOUBLE) END AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_n50 AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT COUNT(*) FROM (SELECT r_n50.x, SUM(r_n50.val) FROM r_n50 JOIN s_n50 ON r_n50.x = s_n50.x GROUP BY r_n50.x);
DROP TABLE r_n50; DROP TABLE s_n50;

.print
.print === 10% NULLs in join key ===
.print --- AGGJOIN ---
CREATE TABLE r_nk AS SELECT CASE WHEN random() < 0.1 THEN NULL ELSE i % 100000 END AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_nk AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT COUNT(*) FROM (SELECT r_nk.x, SUM(r_nk.val) FROM r_nk JOIN s_nk ON r_nk.x = s_nk.x GROUP BY r_nk.x);
DROP TABLE r_nk; DROP TABLE s_nk;
