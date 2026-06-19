-- Benchmark: physical planned-direct path for build-side numeric aggregates.
-- Run: build/Release/duckdb < benchmarks/bench_planned_direct_build_side.sql
--
-- Cascade rewrites are disabled on purpose so this measures the fused operator's
-- planned-direct build-side arrays, not the native build/mixed preagg lowerings.

.timer on

PRAGMA threads=8;
SELECT aggjoin_set_cascade_enabled(false);
SELECT aggjoin_set_operator_enabled(true);

.print === Build-side planned-direct grouped, SUM+COUNT+AVG+MIN+MAX, 50K keys, 3M probe x 1M build ===
CREATE TABLE pd_build_grouped_probe AS
SELECT (i % 50000)::INT AS k
FROM range(3000000) t(i);

CREATE TABLE pd_build_grouped_build AS
SELECT (i % 50000)::INT AS k,
       CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i % 997 AS DOUBLE) END AS y
FROM range(1000000) t(i);

.print --- Fused planned-direct build-side path ---
SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
COPY (
    SELECT pd_build_grouped_probe.k,
           SUM(pd_build_grouped_build.y),
           COUNT(pd_build_grouped_build.y),
           AVG(pd_build_grouped_build.y),
           MIN(pd_build_grouped_build.y),
           MAX(pd_build_grouped_build.y)
    FROM pd_build_grouped_probe JOIN pd_build_grouped_build USING(k)
    GROUP BY pd_build_grouped_probe.k
) TO '/tmp/aggjoin_planned_direct_build_side_grouped_fused.csv' (FORMAT CSV);
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_operator_enabled(false);
COPY (
    SELECT pd_build_grouped_probe.k,
           SUM(pd_build_grouped_build.y),
           COUNT(pd_build_grouped_build.y),
           AVG(pd_build_grouped_build.y),
           MIN(pd_build_grouped_build.y),
           MAX(pd_build_grouped_build.y)
    FROM pd_build_grouped_probe JOIN pd_build_grouped_build USING(k)
    GROUP BY pd_build_grouped_probe.k
) TO '/tmp/aggjoin_planned_direct_build_side_grouped_native.csv' (FORMAT CSV);

DROP TABLE pd_build_grouped_probe;
DROP TABLE pd_build_grouped_build;

.print
.print === Build-side planned-direct scalar, SUM+COUNT+AVG+MIN+MAX, 5K keys, 250K probe x 200K build ===
CREATE TABLE pd_build_scalar_probe AS
SELECT (i % 5000)::INT AS k
FROM range(250000) t(i);

CREATE TABLE pd_build_scalar_build AS
SELECT (i % 5000)::INT AS k,
       CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i % 997 AS DOUBLE) END AS y
FROM range(200000) t(i);

.print --- Fused planned-direct build-side path ---
SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
COPY (
    SELECT SUM(pd_build_scalar_build.y),
           COUNT(pd_build_scalar_build.y),
           AVG(pd_build_scalar_build.y),
           MIN(pd_build_scalar_build.y),
           MAX(pd_build_scalar_build.y)
    FROM pd_build_scalar_probe JOIN pd_build_scalar_build USING(k)
) TO '/tmp/aggjoin_planned_direct_build_side_scalar_fused.csv' (FORMAT CSV);
SELECT aggjoin_last_rewrite();

.print --- Native baseline ---
SELECT aggjoin_set_operator_enabled(false);
COPY (
    SELECT SUM(pd_build_scalar_build.y),
           COUNT(pd_build_scalar_build.y),
           AVG(pd_build_scalar_build.y),
           MIN(pd_build_scalar_build.y),
           MAX(pd_build_scalar_build.y)
    FROM pd_build_scalar_probe JOIN pd_build_scalar_build USING(k)
) TO '/tmp/aggjoin_planned_direct_build_side_scalar_native.csv' (FORMAT CSV);

DROP TABLE pd_build_scalar_probe;
DROP TABLE pd_build_scalar_build;

.print
.print === Build-side planned-direct low-fanout guard, 100K unique probe x 100K unique build ===
CREATE TABLE pd_build_low_probe AS
SELECT i::INT AS k
FROM range(100000) t(i);

CREATE TABLE pd_build_low_build AS
SELECT i::INT AS k,
       CAST(i % 997 AS DOUBLE) AS y
FROM range(100000) t(i);

.print --- Planner should bail to native despite direct-plannable key range ---
SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_reset_rewrite_marker();
COPY (
    SELECT pd_build_low_probe.k,
           SUM(pd_build_low_build.y),
           COUNT(pd_build_low_build.y),
           AVG(pd_build_low_build.y),
           MIN(pd_build_low_build.y),
           MAX(pd_build_low_build.y)
    FROM pd_build_low_probe JOIN pd_build_low_build USING(k)
    GROUP BY pd_build_low_probe.k
) TO '/tmp/aggjoin_planned_direct_build_side_low_guard.csv' (FORMAT CSV);
SELECT aggjoin_last_rewrite();

SELECT aggjoin_set_operator_enabled(true);
SELECT aggjoin_set_cascade_enabled(true);

DROP TABLE pd_build_low_probe;
DROP TABLE pd_build_low_build;
