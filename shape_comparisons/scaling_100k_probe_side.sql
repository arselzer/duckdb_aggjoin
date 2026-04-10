-- Split shape-comparison case from shape_comparisons/scaling.sql
-- Scaling: 100K keys, probe-side shape only
-- Run: build/Release/duckdb < shape_comparisons/scaling_100k_probe_side.sql

.print === Scaling: 100K keys (probe-side shape only) ===
.timer on

CREATE TABLE r_sc AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- Probe-side shape ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_3a.csv' (FORMAT CSV);

DROP TABLE r_sc;
DROP TABLE s_sc;
