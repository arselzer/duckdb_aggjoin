-- Split shape-comparison case from shape_comparisons/core.sql
-- 6. High blowup: 10K keys, 10M rows each (1000x), probe-side shape only
-- Run: build/Release/duckdb < shape_comparisons/core_high_blowup_probe_side.sql

.print === High blowup: 10K keys, 10M rows (1000x) (probe-side shape only) ===
.timer on

CREATE TABLE r6 AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s6 AS SELECT i % 10000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Probe-side shape ---
COPY (SELECT r6.x, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY r6.x) TO '/tmp/aggjoin_bench_6a.csv' (FORMAT CSV);

DROP TABLE r6;
DROP TABLE s6;
