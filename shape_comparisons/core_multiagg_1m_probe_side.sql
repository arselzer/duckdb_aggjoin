-- Split shape-comparison case from shape_comparisons/core.sql
-- 7. Multi-aggregate: SUM+MIN+MAX+AVG, 1M keys, 10M rows, probe-side shape only
-- Run: build/Release/duckdb < shape_comparisons/core_multiagg_1m_probe_side.sql

.print === Multi-agg SUM+MIN+MAX+AVG: 1M keys, 10M rows (probe-side shape only) ===
.timer on

CREATE TABLE r7 AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s7 AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Probe-side shape ---
COPY (SELECT r7.x, SUM(r7.val), MIN(r7.val), MAX(r7.val), AVG(r7.val) FROM r7 JOIN s7 ON r7.x = s7.x GROUP BY r7.x) TO '/tmp/aggjoin_bench_7a.csv' (FORMAT CSV);

DROP TABLE r7;
DROP TABLE s7;
