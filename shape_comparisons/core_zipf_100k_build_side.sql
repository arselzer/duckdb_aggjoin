-- Split shape-comparison case from shape_comparisons/core.sql
-- 4. Zipf-skewed distribution: 100K keys, 10M rows, build-side comparison shape only
-- Run: build/Release/duckdb < shape_comparisons/core_zipf_100k_build_side.sql

.print === Zipf-skewed: 100K keys, 10M rows (build-side comparison shape only) ===
.timer on

CREATE TABLE r4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Build-side comparison shape ---
COPY (SELECT s4.y, SUM(r4.val) FROM r4 JOIN s4 ON r4.x = s4.x GROUP BY s4.y) TO '/tmp/aggjoin_bench_4b.csv' (FORMAT CSV);

DROP TABLE r4;
DROP TABLE s4;
