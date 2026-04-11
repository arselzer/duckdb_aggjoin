-- Benchmark: dblp SNAP path06 query
-- Uses: benchmarks/data/dblp.parquet by default
-- Run:
--   scripts/prepare_dblp_parquet.sh        # only if regenerating the cache
--   build/Release/duckdb < benchmarks/bench_dblp_path06.sql
--
-- Exact chain shape:
--   select count(*) from dblp p1, dblp p2, dblp p3, dblp p4, dblp p5, dblp p6, dblp p7
--   where p1.toNode = p2.fromNode
--     and p2.toNode = p3.fromNode
--     and p3.toNode = p4.fromNode
--     and p4.toNode = p5.fromNode
--     and p5.toNode = p6.fromNode
--     and p6.toNode = p7.fromNode
--
-- Current-plan only. Native baselines for these longer paths are already
-- beyond practical local timeout budgets.

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp path06, exact current-plan query on cached parquet ===
COPY (
    SELECT COUNT(*)
    FROM dblp p1, dblp p2, dblp p3, dblp p4, dblp p5, dblp p6, dblp p7
    WHERE p1.toNode = p2.fromNode
      AND p2.toNode = p3.fromNode
      AND p3.toNode = p4.fromNode
      AND p4.toNode = p5.fromNode
      AND p5.toNode = p6.fromNode
      AND p6.toNode = p7.fromNode
) TO '/tmp/aggjoin_dblp_path06_1a.csv' (FORMAT CSV);

DROP TABLE dblp;
