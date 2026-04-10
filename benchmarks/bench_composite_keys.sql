-- Benchmark: Composite (multi-column) join keys
-- Run: build/Release/duckdb < benchmarks/bench_composite_keys.sql

.timer on

.print === 2-column key, 100K unique combos, 1M rows ===
.print --- AGGJOIN ---
CREATE TABLE r_c1 AS SELECT i % 1000 AS x, i % 100 AS y, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE s_c1 AS SELECT i % 1000 AS x, i % 100 AS y FROM generate_series(1,1000000) t(i);
SELECT COUNT(*) FROM (SELECT r_c1.x, SUM(r_c1.val) FROM r_c1 JOIN s_c1 ON r_c1.x = s_c1.x AND r_c1.y = s_c1.y GROUP BY r_c1.x);
.print --- Native ---
SELECT COUNT(*) FROM (SELECT s_c1.x AS grp, SUM(r_c1.val) FROM r_c1 JOIN s_c1 ON r_c1.x = s_c1.x AND r_c1.y = s_c1.y GROUP BY s_c1.x);
DROP TABLE r_c1; DROP TABLE s_c1;

.print
.print === 3-column key, 1M rows ===
.print --- AGGJOIN ---
CREATE TABLE r_c2 AS SELECT i % 100 AS a, i % 100 AS b, i % 100 AS c, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE s_c2 AS SELECT i % 100 AS a, i % 100 AS b, i % 100 AS c FROM generate_series(1,1000000) t(i);
SELECT COUNT(*) FROM (SELECT r_c2.a, SUM(r_c2.val) FROM r_c2 JOIN s_c2 ON r_c2.a = s_c2.a AND r_c2.b = s_c2.b AND r_c2.c = s_c2.c GROUP BY r_c2.a);
.print --- Native ---
SELECT COUNT(*) FROM (SELECT s_c2.a AS grp, SUM(r_c2.val) FROM r_c2 JOIN s_c2 ON r_c2.a = s_c2.a AND r_c2.b = s_c2.b AND r_c2.c = s_c2.c GROUP BY s_c2.a);
DROP TABLE r_c2; DROP TABLE s_c2;
