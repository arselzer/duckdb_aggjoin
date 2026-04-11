-- Benchmark: dblp SNAP path query from spark-eval
-- Uses: benchmarks/data/dblp.parquet by default
-- Run:
--   scripts/prepare_dblp_parquet.sh        # only if regenerating the cache
--   build/Release/duckdb < benchmarks/bench_dblp_path02.sql
--
-- Uses the exact spark-eval dblp query shape:
--   select count(*) from dblp p1, dblp p2, dblp p3
--   where p1.toNode = p2.fromNode AND p2.toNode = p3.fromNode
--
-- Current planner behavior on a graph-shaped synthetic dblp table:
-- the exact query already fires AGGJOIN, so no adaptation is needed.

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp path02, exact spark-eval query on cached parquet ===
.print --- Direct query shape (planner decides) ---
COPY (
    SELECT COUNT(*)
    FROM dblp p1, dblp p2, dblp p3
    WHERE p1.toNode = p2.fromNode
      AND p2.toNode = p3.fromNode
) TO '/tmp/aggjoin_dblp_path02_1a.csv' (FORMAT CSV);

.print --- Native baseline (extension disabled) ---
PRAGMA disabled_optimizers='extension';
COPY (
    SELECT COUNT(*)
    FROM dblp p1, dblp p2, dblp p3
    WHERE p1.toNode = p2.fromNode
      AND p2.toNode = p3.fromNode
) TO '/tmp/aggjoin_dblp_path02_1b.csv' (FORMAT CSV);
PRAGMA disabled_optimizers='';

DROP TABLE dblp;
