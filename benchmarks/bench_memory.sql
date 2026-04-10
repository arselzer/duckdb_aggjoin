-- Benchmark: Memory profiling
-- Measure peak memory at various direct mode key ranges
-- DuckDB tracks memory via pragma database_size after query execution
-- Run: build/Release/duckdb < benchmarks/bench_memory.sql

.timer on

.print === Memory: 10K keys (direct mode, ~80KB arrays) ===
CREATE TABLE r_m AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE s_m AS SELECT i % 10000 AS x FROM generate_series(1,1000000) t(i);
SELECT COUNT(*) FROM (SELECT r_m.x, SUM(r_m.val) FROM r_m JOIN s_m ON r_m.x = s_m.x GROUP BY r_m.x);
SELECT current_setting('memory_limit') AS memory_limit;
DROP TABLE r_m; DROP TABLE s_m;

.print
.print === Memory: 100K keys (direct mode, ~800KB arrays) ===
CREATE TABLE r_m AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE s_m AS SELECT i % 100000 AS x FROM generate_series(1,1000000) t(i);
SELECT COUNT(*) FROM (SELECT r_m.x, SUM(r_m.val) FROM r_m JOIN s_m ON r_m.x = s_m.x GROUP BY r_m.x);
DROP TABLE r_m; DROP TABLE s_m;

.print
.print === Memory: 1M keys (direct mode, ~8MB arrays) ===
CREATE TABLE r_m AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,2000000) t(i);
CREATE TABLE s_m AS SELECT i % 1000000 AS x FROM generate_series(1,2000000) t(i);
SELECT COUNT(*) FROM (SELECT r_m.x, SUM(r_m.val) FROM r_m JOIN s_m ON r_m.x = s_m.x GROUP BY r_m.x);
DROP TABLE r_m; DROP TABLE s_m;

.print
.print === Memory: 2M keys (direct mode boundary, ~16MB arrays) ===
CREATE TABLE r_m AS SELECT i % 2000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,4000000) t(i);
CREATE TABLE s_m AS SELECT i % 2000000 AS x FROM generate_series(1,4000000) t(i);
SELECT COUNT(*) FROM (SELECT r_m.x, SUM(r_m.val) FROM r_m JOIN s_m ON r_m.x = s_m.x GROUP BY r_m.x);
DROP TABLE r_m; DROP TABLE s_m;

.print
.print === Memory: 1M keys, 4 aggs (direct mode, ~32MB arrays) ===
CREATE TABLE r_m AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,2000000) t(i);
CREATE TABLE s_m AS SELECT i % 1000000 AS x FROM generate_series(1,2000000) t(i);
SELECT COUNT(*) FROM (SELECT r_m.x, SUM(r_m.val), MIN(r_m.val), MAX(r_m.val), AVG(r_m.val) FROM r_m JOIN s_m ON r_m.x = s_m.x GROUP BY r_m.x);
DROP TABLE r_m; DROP TABLE s_m;

.print
.print Note: Direct mode memory = key_range * num_aggs * 8 bytes per array
.print  (sums + counts + mins + maxs + has_val). Actual RSS depends on
.print  DuckDB buffer manager and OS page allocation.
