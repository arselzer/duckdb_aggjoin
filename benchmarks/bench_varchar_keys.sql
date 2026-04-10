-- Benchmark: VARCHAR join keys (hash mode only)
-- Non-integer keys force hash mode — tests the new column-major FlatResultHT
-- Run: build/Release/duckdb < benchmarks/bench_varchar_keys.sql

.timer on

.print === Short VARCHAR keys (4 char), 100K rows ===
.print --- AGGJOIN ---
CREATE TABLE r_v1 AS SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 3, '0') AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s_v1 AS SELECT 'k' || LPAD(CAST(i % 10000 AS VARCHAR), 3, '0') AS x FROM generate_series(1,100000) t(i);
SELECT COUNT(*) FROM (SELECT r_v1.x, SUM(r_v1.val) FROM r_v1 JOIN s_v1 ON r_v1.x = s_v1.x GROUP BY r_v1.x);
.print --- Native ---
SELECT COUNT(*) FROM (SELECT s_v1.x AS grp, SUM(r_v1.val) FROM r_v1 JOIN s_v1 ON r_v1.x = s_v1.x GROUP BY s_v1.x);
DROP TABLE r_v1; DROP TABLE s_v1;

.print
.print === Medium VARCHAR keys (20 char), 100K rows ===
.print --- AGGJOIN ---
CREATE TABLE r_v2 AS SELECT 'key_' || LPAD(CAST(i % 10000 AS VARCHAR), 16, '0') AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s_v2 AS SELECT 'key_' || LPAD(CAST(i % 10000 AS VARCHAR), 16, '0') AS x FROM generate_series(1,100000) t(i);
SELECT COUNT(*) FROM (SELECT r_v2.x, SUM(r_v2.val) FROM r_v2 JOIN s_v2 ON r_v2.x = s_v2.x GROUP BY r_v2.x);
.print --- Native ---
SELECT COUNT(*) FROM (SELECT s_v2.x AS grp, SUM(r_v2.val) FROM r_v2 JOIN s_v2 ON r_v2.x = s_v2.x GROUP BY s_v2.x);
DROP TABLE r_v2; DROP TABLE s_v2;

.print
.print === Long VARCHAR keys (100 char), 100K rows ===
.print --- AGGJOIN ---
CREATE TABLE r_v3 AS SELECT REPEAT('k', 90) || LPAD(CAST(i % 10000 AS VARCHAR), 10, '0') AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s_v3 AS SELECT REPEAT('k', 90) || LPAD(CAST(i % 10000 AS VARCHAR), 10, '0') AS x FROM generate_series(1,100000) t(i);
SELECT COUNT(*) FROM (SELECT r_v3.x, SUM(r_v3.val) FROM r_v3 JOIN s_v3 ON r_v3.x = s_v3.x GROUP BY r_v3.x);
.print --- Native ---
SELECT COUNT(*) FROM (SELECT s_v3.x AS grp, SUM(r_v3.val) FROM r_v3 JOIN s_v3 ON r_v3.x = s_v3.x GROUP BY s_v3.x);
DROP TABLE r_v3; DROP TABLE s_v3;
