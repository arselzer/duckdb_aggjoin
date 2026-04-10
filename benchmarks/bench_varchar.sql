-- VARCHAR MIN/MAX benchmark
-- Tests non-numeric MIN/MAX through AGGJOIN
-- Native baselines use MATERIALIZED CTE to prevent AGGJOIN from firing
.timer on

-- Setup: 100K keys, 1M rows
CREATE TABLE r_vc AS SELECT i % 100000 AS x,
    'name_' || LPAD(CAST((random()*100000)::INT AS VARCHAR), 6, '0') AS name
FROM generate_series(1, 1000000) t(i);
CREATE TABLE s_vc AS SELECT i % 100000 AS x FROM generate_series(1, 1000000) t(i);

.print === Ungrouped VARCHAR MIN/MAX ===
.print --- AGGJOIN ---
SELECT MIN(r_vc.name), MAX(r_vc.name) FROM r_vc JOIN s_vc ON r_vc.x = s_vc.x;
.print --- Native (MATERIALIZED CTE) ---
WITH joined AS MATERIALIZED (SELECT r_vc.name FROM r_vc JOIN s_vc ON r_vc.x = s_vc.x)
SELECT MIN(name), MAX(name) FROM joined;

.print === Grouped VARCHAR MIN/MAX ===
.print --- AGGJOIN ---
SELECT r_vc.x, MIN(r_vc.name), MAX(r_vc.name) FROM r_vc JOIN s_vc ON r_vc.x = s_vc.x GROUP BY r_vc.x;
.print --- Native (MATERIALIZED CTE) ---
WITH joined AS MATERIALIZED (SELECT r_vc.x, r_vc.name FROM r_vc JOIN s_vc ON r_vc.x = s_vc.x)
SELECT x, MIN(name), MAX(name) FROM joined GROUP BY x;

.print === Mixed: SUM(DOUBLE) + MIN(VARCHAR) ===
CREATE TABLE r_mix AS SELECT i % 100000 AS x,
    CAST(random()*100 AS DOUBLE) AS val,
    'name_' || LPAD(CAST((random()*100000)::INT AS VARCHAR), 6, '0') AS name
FROM generate_series(1, 1000000) t(i);

.print --- AGGJOIN ---
SELECT r_mix.x, SUM(r_mix.val), MIN(r_mix.name), MAX(r_mix.name) FROM r_mix JOIN s_vc ON r_mix.x = s_vc.x GROUP BY r_mix.x;
.print --- Native (MATERIALIZED CTE) ---
WITH joined AS MATERIALIZED (SELECT r_mix.x, r_mix.val, r_mix.name FROM r_mix JOIN s_vc ON r_mix.x = s_vc.x)
SELECT x, SUM(val), MIN(name), MAX(name) FROM joined GROUP BY x;
