-- Benchmark: Latency (time-to-first-row)
-- Uses LIMIT 1 to measure pipeline startup + first result emission
-- Run: build/Release/duckdb < benchmarks/bench_latency.sql

.timer on

.print === Latency: AGGJOIN, 100K keys, LIMIT 1 ===
CREATE TABLE r_l AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_l AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT r_l.x, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY r_l.x LIMIT 1;

.print === Latency: Native, 100K keys, LIMIT 1 ===
SELECT s_l.x AS y, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY s_l.x LIMIT 1;
DROP TABLE r_l; DROP TABLE s_l;

.print
.print === Latency: AGGJOIN, 1M keys, LIMIT 1 ===
CREATE TABLE r_l AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_l AS SELECT i % 1000000 AS x FROM generate_series(1,10000000) t(i);
SELECT r_l.x, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY r_l.x LIMIT 1;

.print === Latency: Native, 1M keys, LIMIT 1 ===
SELECT s_l.x AS y, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY s_l.x LIMIT 1;
DROP TABLE r_l; DROP TABLE s_l;

.print
.print === Throughput: AGGJOIN, 100K keys, full result ===
CREATE TABLE r_l AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_l AS SELECT i % 100000 AS x FROM generate_series(1,10000000) t(i);
SELECT COUNT(*) FROM (SELECT r_l.x, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY r_l.x);

.print === Throughput: Native, 100K keys, full result ===
SELECT COUNT(*) FROM (SELECT s_l.x AS y, SUM(r_l.val) FROM r_l JOIN s_l ON r_l.x = s_l.x GROUP BY s_l.x);
DROP TABLE r_l; DROP TABLE s_l;

.print
.print Note: AGGJOIN is a blocking operator (must process all probe rows
.print  before emitting any results). LIMIT 1 still requires full execution.
.print  Compare latency vs throughput to quantify this overhead.
