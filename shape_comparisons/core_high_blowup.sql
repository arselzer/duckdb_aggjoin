-- Split shape-comparison case from shape_comparisons/core.sql
-- 6. High blowup: 10K keys, 10M rows each (1000x)
-- Run: build/Release/duckdb < shape_comparisons/core_high_blowup.sql

.print === High blowup: 10K keys, 10M rows (1000x) ===
.timer on

.print --- Probe-side shape ---
CREATE TABLE r6 AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s6 AS SELECT i % 10000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r6.x, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY r6.x) TO '/tmp/aggjoin_bench_6a.csv' (FORMAT CSV);

.print --- Build-side comparison shape ---
COPY (SELECT s6.y, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY s6.y) TO '/tmp/aggjoin_bench_6b.csv' (FORMAT CSV);

DROP TABLE r6;
DROP TABLE s6;
