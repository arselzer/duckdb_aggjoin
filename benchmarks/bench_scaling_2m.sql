-- Split benchmark from bench_scaling.sql
-- Scaling: 2M keys (direct mode boundary)
-- Run: build/Release/duckdb < benchmarks/bench_scaling_2m.sql

.print === Scaling: 2M keys (direct mode boundary) ===
.timer on

CREATE TABLE r_sc AS SELECT i % 2000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s_sc AS SELECT i % 2000000 AS x, i AS y FROM generate_series(1,10000000) t(i);

.print --- AGGJOIN ---
COPY (SELECT r_sc.x, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY r_sc.x) TO '/tmp/aggjoin_scaling_6a.csv' (FORMAT CSV);

.print --- Native ---
COPY (SELECT s_sc.y, SUM(r_sc.val) FROM r_sc JOIN s_sc ON r_sc.x = s_sc.x GROUP BY s_sc.y) TO '/tmp/aggjoin_scaling_6b.csv' (FORMAT CSV);

DROP TABLE r_sc;
DROP TABLE s_sc;
