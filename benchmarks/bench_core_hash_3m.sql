-- Split benchmark from bench.sql
-- 3. Hash mode: 3M keys, 10M rows (above direct limit)
-- Run: build/Release/duckdb < benchmarks/bench_core_hash_3m.sql

.print === Hash mode: 3M keys, 10M rows, uniform ===
.timer on

.print --- AGGJOIN ---
CREATE TABLE r3 AS SELECT i % 3000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s3 AS SELECT i % 3000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r3.x, SUM(r3.val) FROM r3 JOIN s3 ON r3.x = s3.x GROUP BY r3.x) TO '/tmp/aggjoin_bench_3a.csv' (FORMAT CSV);

.print --- Native ---
COPY (SELECT s3.y, SUM(r3.val) FROM r3 JOIN s3 ON r3.x = s3.x GROUP BY s3.y) TO '/tmp/aggjoin_bench_3b.csv' (FORMAT CSV);

DROP TABLE r3;
DROP TABLE s3;
