-- Split shape-comparison case from shape_comparisons/core.sql
-- 4. Zipf-skewed distribution: 100K keys, 10M rows, probe-side shape only
-- Run: build/Release/duckdb < shape_comparisons/core_zipf_100k_probe_side.sql

.print === Zipf-skewed: 100K keys, 10M rows (probe-side shape only) ===
.timer on

CREATE TABLE r4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Probe-side shape ---
COPY (SELECT r4.x, SUM(r4.val) FROM r4 JOIN s4 ON r4.x = s4.x GROUP BY r4.x) TO '/tmp/aggjoin_bench_4a.csv' (FORMAT CSV);

DROP TABLE r4;
DROP TABLE s4;
