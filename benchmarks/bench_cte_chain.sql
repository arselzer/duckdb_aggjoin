-- Benchmark: CTE chain (Yannakakis frequency propagation pattern)
-- The real use case: AGGJOIN fires within CTE steps, not standalone
-- Run: build/Release/duckdb < benchmarks/bench_cte_chain.sql

.print === Setup: 4-cycle schema R(x,w), S(x,y), T(y,z), U(z,w) ===
CREATE TABLE R AS SELECT i % 100000 AS x, i % 50000 AS w, CAST(random()*100 AS DOUBLE) AS val FROM generate_series(1,1000000) t(i);
CREATE TABLE S AS SELECT i % 100000 AS x, i % 80000 AS y FROM generate_series(1,1000000) t(i);
CREATE TABLE T AS SELECT i % 80000 AS y, i % 60000 AS z FROM generate_series(1,1000000) t(i);
CREATE TABLE U AS SELECT i % 60000 AS z, i % 50000 AS w FROM generate_series(1,1000000) t(i);

.timer on

.print
.print === CTE chain: frequency propagation (AGGJOIN fires inside CTEs) ===
WITH
  cte_1 AS (
    SELECT S.y, CAST(COUNT(*) AS DOUBLE) AS _freq
    FROM R JOIN S ON R.x = S.x
    GROUP BY S.y
  ),
  cte_2 AS (
    SELECT T.z, SUM(cte_1._freq) AS _freq
    FROM cte_1 JOIN T ON cte_1.y = T.y
    GROUP BY T.z
  ),
  cte_3 AS (
    SELECT U.w, SUM(cte_2._freq) AS _freq
    FROM cte_2 JOIN U ON cte_2.z = U.z
    GROUP BY U.w
  )
SELECT CAST(SUM(_freq) AS BIGINT) AS total FROM cte_3;

.print
.print === Native: same query without CTEs (DuckDB optimizes freely) ===
SELECT COUNT(*) FROM R, S, T, U
WHERE R.x = S.x AND S.y = T.y AND T.z = U.z;

.print
.print === CTE chain: with grouped output ===
WITH
  cte_1 AS (
    SELECT R.x, CAST(COUNT(*) AS DOUBLE) AS _freq
    FROM R JOIN S ON R.x = S.x
    GROUP BY R.x
  ),
  cte_2 AS (
    SELECT cte_1.x, SUM(cte_1._freq) AS _freq
    FROM cte_1 JOIN S AS S2 ON cte_1.x = S2.x
    GROUP BY cte_1.x
  )
SELECT COUNT(*), CAST(SUM(_freq) AS BIGINT) FROM cte_2;

DROP TABLE R; DROP TABLE S; DROP TABLE T; DROP TABLE U;
