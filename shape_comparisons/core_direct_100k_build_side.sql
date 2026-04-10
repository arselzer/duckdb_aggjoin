-- Split shape-comparison case from shape_comparisons/core.sql
-- 1. Direct mode: 100K keys, 10M rows (uniform), build-side comparison shape only
-- Run: build/Release/duckdb < shape_comparisons/core_direct_100k_build_side.sql

.print === Direct mode: 100K keys, 10M rows, uniform (build-side comparison shape only) ===
.timer on

CREATE TABLE r1 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s1 AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Build-side comparison shape ---
COPY (SELECT s1.y, SUM(r1.val) FROM r1 JOIN s1 ON r1.x = s1.x GROUP BY s1.y) TO '/tmp/aggjoin_bench_1b.csv' (FORMAT CSV);

DROP TABLE r1;
DROP TABLE s1;
