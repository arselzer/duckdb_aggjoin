-- Split shape-comparison case from shape_comparisons/core.sql
-- 2. Direct mode: 1M keys, 10M rows (uniform), probe-side shape only
-- Run: build/Release/duckdb < shape_comparisons/core_direct_1m_probe_side.sql

.print === Direct mode: 1M keys, 10M rows, uniform (probe-side shape only) ===
.timer on

CREATE TABLE r2 AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s2 AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Probe-side shape ---
COPY (SELECT r2.x, SUM(r2.val) FROM r2 JOIN s2 ON r2.x = s2.x GROUP BY r2.x) TO '/tmp/aggjoin_bench_2a.csv' (FORMAT CSV);

DROP TABLE r2;
DROP TABLE s2;
