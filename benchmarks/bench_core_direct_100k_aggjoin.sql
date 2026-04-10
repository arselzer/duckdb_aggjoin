-- Split benchmark from bench.sql
-- 1. Direct mode: 100K keys, 10M rows (uniform), AGGJOIN side only
-- Run: build/Release/duckdb < benchmarks/bench_core_direct_100k_aggjoin.sql

.print === Direct mode: 100K keys, 10M rows, uniform (AGGJOIN only) ===
.timer on

CREATE TABLE r1 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s1 AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- AGGJOIN ---
COPY (SELECT r1.x, SUM(r1.val) FROM r1 JOIN s1 ON r1.x = s1.x GROUP BY r1.x) TO '/tmp/aggjoin_bench_1a.csv' (FORMAT CSV);

DROP TABLE r1;
DROP TABLE s1;
