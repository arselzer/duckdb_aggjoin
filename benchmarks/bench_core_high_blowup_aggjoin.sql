-- Split benchmark from bench.sql
-- 6. High blowup: 10K keys, 10M rows each (1000x), AGGJOIN side only
-- Run: build/Release/duckdb < benchmarks/bench_core_high_blowup_aggjoin.sql

.print === High blowup: 10K keys, 10M rows (1000x) (AGGJOIN only) ===
.timer on

CREATE TABLE r6 AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s6 AS SELECT i % 10000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- AGGJOIN ---
COPY (SELECT r6.x, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY r6.x) TO '/tmp/aggjoin_bench_6a.csv' (FORMAT CSV);

DROP TABLE r6;
DROP TABLE s6;
