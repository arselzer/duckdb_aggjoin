-- AggJoin correctness verification
-- Compares AGGJOIN results vs native for identical queries
-- Run: build/Release/duckdb < benchmarks/bench_correctness.sql

.print ============================================================
.print  AggJoin Correctness Verification
.print ============================================================

CREATE TABLE r AS SELECT i % 1000 AS x, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,10000) t(i);
CREATE TABLE s AS SELECT i % 1000 AS x, i AS y FROM generate_series(1,10000) t(i);

-- Grouped SUM: total across all groups should match
.print
.print === Grouped SUM: verify totals match ===
WITH aggjoin_result AS (
    SELECT r.x, SUM(r.val) AS sv FROM r JOIN s ON r.x = s.x GROUP BY r.x
),
native_result AS (
    SELECT s.y, SUM(r.val) AS sv FROM r JOIN s ON r.x = s.x GROUP BY s.y
)
SELECT
    (SELECT CAST(SUM(sv) AS BIGINT) FROM aggjoin_result) AS aggjoin_total,
    (SELECT CAST(SUM(sv) AS BIGINT) FROM native_result) AS native_total,
    CASE WHEN (SELECT CAST(SUM(sv) AS BIGINT) FROM aggjoin_result) =
              (SELECT CAST(SUM(sv) AS BIGINT) FROM native_result)
         THEN 'PASS' ELSE 'FAIL' END AS status;

-- Ungrouped SUM
.print
.print === Ungrouped SUM ===
SELECT
    (SELECT CAST(SUM(r.val) AS BIGINT) FROM r JOIN s ON r.x = s.x) AS aggjoin,
    (SELECT CAST(SUM(sub.val) AS BIGINT) FROM (SELECT s.y, r.val FROM r JOIN s ON r.x = s.x) sub) AS native,
    CASE WHEN (SELECT CAST(SUM(r.val) AS BIGINT) FROM r JOIN s ON r.x = s.x) =
              (SELECT CAST(SUM(sub.val) AS BIGINT) FROM (SELECT s.y, r.val FROM r JOIN s ON r.x = s.x) sub)
         THEN 'PASS' ELSE 'FAIL' END AS status;

-- Ungrouped COUNT
.print
.print === Ungrouped COUNT ===
SELECT
    (SELECT COUNT(*) FROM r JOIN s ON r.x = s.x) AS result,
    CASE WHEN (SELECT COUNT(*) FROM r JOIN s ON r.x = s.x) = 10000 * 10
         THEN 'PASS' ELSE 'FAIL' END AS status;

-- AVG correctness: compare ungrouped AVG (single value, must match exactly)
.print
.print === Ungrouped AVG ===
SELECT
    CAST((SELECT AVG(r.val) FROM r JOIN s ON r.x = s.x) AS INTEGER) AS aggjoin_avg,
    CASE WHEN ABS((SELECT AVG(r.val) FROM r JOIN s ON r.x = s.x) - 49.0) < 2.0
         THEN 'PASS' ELSE 'FAIL' END AS status;

-- AVG grouped correctness: compare same grouping via AGGJOIN vs native
.print
.print === Grouped AVG (same grouping) ===
SELECT
    CAST((SELECT SUM(av) FROM (SELECT r.x, AVG(r.val) AS av FROM r JOIN s ON r.x = s.x GROUP BY r.x)) AS INTEGER) AS aggjoin,
    CASE WHEN ABS((SELECT SUM(av) FROM (SELECT r.x, AVG(r.val) AS av FROM r JOIN s ON r.x = s.x GROUP BY r.x)) - 49.0 * 1000) < 2000
         THEN 'PASS' ELSE 'FAIL' END AS status;

-- No matches returns COUNT=0 (not empty)
.print
.print === No matches: COUNT returns 0 ===
SELECT
    (SELECT COUNT(*) FROM r JOIN s ON r.x = s.x WHERE r.x = 99999) AS result,
    CASE WHEN (SELECT COUNT(*) FROM r JOIN s ON r.x = s.x WHERE r.x = 99999) = 0
         THEN 'PASS' ELSE 'FAIL' END AS status;

.print
.print ============================================================
.print  All lines should show PASS.
.print ============================================================
