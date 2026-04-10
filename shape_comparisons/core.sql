-- AggJoin shape-comparison suite
-- Tests direct mode, hash mode, various distributions
-- Run: build/Release/duckdb < shape_comparisons/core.sql

.print ============================================================
.print  AggJoin Shape-Comparison Suite
.print  DuckDB with aggjoin extension (optimizer fires automatically)
.print ============================================================
.print
.print  AGGJOIN fires when GROUP BY and agg cols are on probe side.
.print  Build-side comparison shape uses GROUP BY on the build side (optimizer bails).
.print  Both produce identical aggregate totals.
.print ============================================================

.timer on

-- ============================================================
-- 1. Direct mode: 100K keys, 10M rows (uniform)
-- ============================================================
.print
.print === 1. Direct mode: 100K keys, 10M rows, uniform ===
.print --- Probe-side shape ---
CREATE TABLE r1 AS SELECT i % 100000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s1 AS SELECT i % 100000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r1.x, SUM(r1.val) FROM r1 JOIN s1 ON r1.x = s1.x GROUP BY r1.x) TO '/tmp/aggjoin_bench_1a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s1.y, SUM(r1.val) FROM r1 JOIN s1 ON r1.x = s1.x GROUP BY s1.y) TO '/tmp/aggjoin_bench_1b.csv' (FORMAT CSV);
DROP TABLE r1; DROP TABLE s1;

-- ============================================================
-- 2. Direct mode: 1M keys, 10M rows (uniform)
-- ============================================================
.print
.print === 2. Direct mode: 1M keys, 10M rows, uniform ===
.print --- Probe-side shape ---
CREATE TABLE r2 AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s2 AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r2.x, SUM(r2.val) FROM r2 JOIN s2 ON r2.x = s2.x GROUP BY r2.x) TO '/tmp/aggjoin_bench_2a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s2.y, SUM(r2.val) FROM r2 JOIN s2 ON r2.x = s2.x GROUP BY s2.y) TO '/tmp/aggjoin_bench_2b.csv' (FORMAT CSV);
DROP TABLE r2; DROP TABLE s2;

-- ============================================================
-- 3. Hash mode: 3M keys, 10M rows (above direct limit)
-- ============================================================
.print
.print === 3. Hash mode: 3M keys, 10M rows, uniform ===
.print --- Probe-side shape ---
CREATE TABLE r3 AS SELECT i % 3000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s3 AS SELECT i % 3000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r3.x, SUM(r3.val) FROM r3 JOIN s3 ON r3.x = s3.x GROUP BY r3.x) TO '/tmp/aggjoin_bench_3a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s3.y, SUM(r3.val) FROM r3 JOIN s3 ON r3.x = s3.x GROUP BY s3.y) TO '/tmp/aggjoin_bench_3b.csv' (FORMAT CSV);
DROP TABLE r3; DROP TABLE s3;

-- ============================================================
-- 4. Skewed distribution: Zipf-like, 100K keys, 10M rows
-- ============================================================
.print
.print === 4. Zipf-skewed: 100K keys, 10M rows ===
.print --- Probe-side shape ---
CREATE TABLE r4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s4 AS SELECT CAST(POWER(random(), 3) * 100000 AS INT) AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r4.x, SUM(r4.val) FROM r4 JOIN s4 ON r4.x = s4.x GROUP BY r4.x) TO '/tmp/aggjoin_bench_4a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s4.y, SUM(r4.val) FROM r4 JOIN s4 ON r4.x = s4.x GROUP BY s4.y) TO '/tmp/aggjoin_bench_4b.csv' (FORMAT CSV);
DROP TABLE r4; DROP TABLE s4;

-- ============================================================
-- 5. Sparse keys (big range, few keys): 100K rows, 10M range
-- ============================================================
.print
.print === 5. Sparse: ~100K keys, range 10M, 100K rows ===
.print --- AGGJOIN (hash mode) ---
CREATE TABLE r5 AS SELECT CAST(random() * 10000000 AS INT) AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,100000) t(i);
CREATE TABLE s5 AS SELECT x FROM r5;
COPY (SELECT r5.x, SUM(r5.val) FROM r5 JOIN s5 ON r5.x = s5.x GROUP BY r5.x) TO '/tmp/aggjoin_bench_5a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s5.x AS y, SUM(r5.val) FROM r5 JOIN s5 ON r5.x = s5.x GROUP BY s5.x) TO '/tmp/aggjoin_bench_5b.csv' (FORMAT CSV);
DROP TABLE r5; DROP TABLE s5;

-- ============================================================
-- 6. High blowup: 10K keys, 10M rows each (1000x)
-- ============================================================
.print
.print === 6. High blowup: 10K keys, 10M rows (1000x) ===
.print --- Probe-side shape ---
CREATE TABLE r6 AS SELECT i % 10000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s6 AS SELECT i % 10000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r6.x, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY r6.x) TO '/tmp/aggjoin_bench_6a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s6.y, SUM(r6.val) FROM r6 JOIN s6 ON r6.x = s6.x GROUP BY s6.y) TO '/tmp/aggjoin_bench_6b.csv' (FORMAT CSV);
DROP TABLE r6; DROP TABLE s6;

-- ============================================================
-- 7. Multi-aggregate: SUM+MIN+MAX+AVG, 1M keys, 10M rows
-- ============================================================
.print
.print === 7. Multi-agg SUM+MIN+MAX+AVG: 1M keys, 10M rows ===
.print --- Probe-side shape ---
CREATE TABLE r7 AS SELECT i % 1000000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000000) t(i);
CREATE TABLE s7 AS SELECT i % 1000000 AS x, i AS y FROM generate_series(1,10000000) t(i);
COPY (SELECT r7.x, SUM(r7.val), MIN(r7.val), MAX(r7.val), AVG(r7.val) FROM r7 JOIN s7 ON r7.x = s7.x GROUP BY r7.x) TO '/tmp/aggjoin_bench_7a.csv' (FORMAT CSV);
.print --- Build-side comparison shape ---
COPY (SELECT s7.y, SUM(r7.val), MIN(r7.val), MAX(r7.val), AVG(r7.val) FROM r7 JOIN s7 ON r7.x = s7.x GROUP BY s7.y) TO '/tmp/aggjoin_bench_7b.csv' (FORMAT CSV);
DROP TABLE r7; DROP TABLE s7;

.print
.print ============================================================
.print  Done. Compare "Run Time" lines for AGGJOIN vs Native.
.print ============================================================
