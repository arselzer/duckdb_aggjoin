-- Benchmark: manual rewrite-y frequency-propagation CTE for dblp path03
-- Uses: benchmarks/data/dblp.parquet
-- Run:
--   build/Release/duckdb < benchmarks/bench_dblp_freqprop_path03.sql

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp freqprop path03, manual rewrite ===
.print --- Rewritten query, extension enabled ---
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p3"."fromnode", "p3"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p3"
    JOIN "dblp" AS "p4" ON "p3"."tonode" = "p4"."fromnode"
    GROUP BY "p3"."fromnode", "p3"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p2"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p2"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_3" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_2_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_2"
      GROUP BY "fromnode"
    ) AS "_yw_cte_2_agg" ON "p1"."tonode" = "_yw_cte_2_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_3"
) TO '/tmp/aggjoin_dblp_freqprop_path03_enabled.csv' (FORMAT CSV, HEADER);

.print --- Rewritten query, extension disabled ---
PRAGMA disabled_optimizers='extension';
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p3"."fromnode", "p3"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p3"
    JOIN "dblp" AS "p4" ON "p3"."tonode" = "p4"."fromnode"
    GROUP BY "p3"."fromnode", "p3"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p2"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p2"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_3" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_2_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_2"
      GROUP BY "fromnode"
    ) AS "_yw_cte_2_agg" ON "p1"."tonode" = "_yw_cte_2_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_3"
) TO '/tmp/aggjoin_dblp_freqprop_path03_disabled.csv' (FORMAT CSV, HEADER);
PRAGMA disabled_optimizers='';

DROP TABLE dblp;
