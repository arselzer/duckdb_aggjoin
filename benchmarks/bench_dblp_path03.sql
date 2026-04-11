-- Benchmark: dblp SNAP path03 query from spark-eval
-- Uses: benchmarks/data/dblp.parquet by default
-- Run:
--   scripts/prepare_dblp_parquet.sh        # only if regenerating the cache
--   build/Release/duckdb < benchmarks/bench_dblp_path03.sql
--
-- Uses the exact spark-eval dblp query shape:
--   select count(*) from dblp p1, dblp p2, dblp p3, dblp p4
--   where p1.toNode = p2.fromNode
--     and p2.toNode = p3.fromNode
--     and p3.toNode = p4.fromNode

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp path03, exact spark-eval query on cached parquet ===
.print --- Direct query shape (planner decides) ---
COPY (
    SELECT COUNT(*)
    FROM dblp p1, dblp p2, dblp p3, dblp p4
    WHERE p1.toNode = p2.fromNode
      AND p2.toNode = p3.fromNode
      AND p3.toNode = p4.fromNode
) TO '/tmp/aggjoin_dblp_path03_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT COUNT(*)
    FROM dblp p1, dblp p2, dblp p3, dblp p4
    WHERE p1.toNode = p2.fromNode
      AND p2.toNode = p3.fromNode
      AND p3.toNode = p4.fromNode
) TO '/tmp/aggjoin_dblp_path03_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE dblp;
