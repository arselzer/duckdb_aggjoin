-- Split benchmark from bench.sql
-- 3. Hash mode: 3M keys, 10M rows, AGGJOIN side only
-- Run: build/Release/duckdb < benchmarks/bench_core_hash_3m_aggjoin.sql

.print === Hash mode: 3M keys, 10M rows, uniform (AGGJOIN only) ===
.timer on

CREATE TABLE r3 AS SELECT i % 3000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s3 AS SELECT i % 3000000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- AGGJOIN ---
COPY (SELECT r3.x, SUM(r3.val) FROM r3 JOIN s3 ON r3.x = s3.x GROUP BY r3.x) TO '/tmp/aggjoin_bench_3a.csv' (FORMAT CSV);

DROP TABLE r3;
DROP TABLE s3;
