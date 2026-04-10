-- Split shape-comparison case from shape_comparisons/scaling.sql
-- Scaling: 1K keys, build-side comparison shape only
-- Run: build/Release/duckdb < shape_comparisons/scaling_1k_build_side.sql

.print === Scaling: 1K keys (build-side comparison shape only) ===
.timer on

CREATE TABLE r_sc AS SELECT i % 1000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 1000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Build-side comparison shape ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_1b.csv' (FORMAT CSV);

DROP TABLE r_sc;
DROP TABLE s_sc;
