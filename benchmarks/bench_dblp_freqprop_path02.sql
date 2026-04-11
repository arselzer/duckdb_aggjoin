-- Benchmark: manual rewrite-y frequency-propagation CTE for dblp path02
-- Uses: benchmarks/data/dblp.parquet
-- Run:
--   build/Release/duckdb < benchmarks/bench_dblp_freqprop_path02.sql
--
-- Original raw query:
--   select count(*)
--   from dblp p1, dblp p2, dblp p3
--   where p1.toNode = p2.fromNode
--     and p2.toNode = p3.fromNode
--
-- This file benchmarks the manual frequency-propagation rewriting as a
-- same-query extension-on/off comparison.

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp freqprop path02, manual rewrite ===
.print --- Rewritten query, extension enabled ---
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p2"
    JOIN "dblp" AS "p3" ON "p2"."tonode" = "p3"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p1"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_2"
) TO '/tmp/aggjoin_dblp_freqprop_path02_enabled.csv' (FORMAT CSV, HEADER);

.print --- Rewritten query, extension disabled ---
PRAGMA disabled_optimizers='extension';
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p2"
    JOIN "dblp" AS "p3" ON "p2"."tonode" = "p3"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p1"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_2"
) TO '/tmp/aggjoin_dblp_freqprop_path02_disabled.csv' (FORMAT CSV, HEADER);
PRAGMA disabled_optimizers='';

DROP TABLE dblp;
