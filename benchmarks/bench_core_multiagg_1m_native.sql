-- Split benchmark from bench.sql
-- 7. Multi-aggregate: SUM+MIN+MAX+AVG, 1M keys, 10M rows, native side only
-- Run: build/Release/duckdb < benchmarks/bench_core_multiagg_1m_native.sql

.print === Multi-agg SUM+MIN+MAX+AVG: 1M keys, 10M rows (Native only) ===
.timer on

CREATE TABLE r7 AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s7 AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Native ---
COPY (SELECT s7.y, SUM(r7.val), MIN(r7.val), MAX(r7.val), AVG(r7.val) FROM r7 JOIN s7 ON r7.x = s7.x GROUP BY s7.y) TO '/tmp/aggjoin_bench_7b.csv' (FORMAT CSV);

DROP TABLE r7;
DROP TABLE s7;
