-- Benchmark: manual rewrite-y frequency-propagation CTE for dblp path07
-- Uses: benchmarks/data/dblp.parquet
-- Run:
--   build/Release/duckdb < benchmarks/bench_dblp_freqprop_path07.sql

.timer on

DROP TABLE IF EXISTS dblp;
CREATE TABLE dblp AS
SELECT fromNode, toNode
FROM read_parquet('benchmarks/data/dblp.parquet');

.print === dblp freqprop path07, manual rewrite ===
.print --- Rewritten query, extension enabled ---
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p7"."fromnode", "p7"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p7"
    JOIN "dblp" AS "p8" ON "p7"."tonode" = "p8"."fromnode"
    GROUP BY "p7"."fromnode", "p7"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p6"."fromnode", "p6"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p6"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p6"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p6"."fromnode", "p6"."tonode"
  ),
  "_yw_cte_3" AS (
    SELECT "p5"."fromnode", "p5"."tonode",
           SUM("_yw_cte_2_agg"."_freq") AS _freq
    FROM "dblp" AS "p5"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_2"
      GROUP BY "fromnode"
    ) AS "_yw_cte_2_agg" ON "p5"."tonode" = "_yw_cte_2_agg"."fromnode"
    GROUP BY "p5"."fromnode", "p5"."tonode"
  ),
  "_yw_cte_4" AS (
    SELECT "p4"."fromnode", "p4"."tonode",
           SUM("_yw_cte_3_agg"."_freq") AS _freq
    FROM "dblp" AS "p4"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_3"
      GROUP BY "fromnode"
    ) AS "_yw_cte_3_agg" ON "p4"."tonode" = "_yw_cte_3_agg"."fromnode"
    GROUP BY "p4"."fromnode", "p4"."tonode"
  ),
  "_yw_cte_5" AS (
    SELECT "p3"."fromnode", "p3"."tonode",
           SUM("_yw_cte_4_agg"."_freq") AS _freq
    FROM "dblp" AS "p3"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_4"
      GROUP BY "fromnode"
    ) AS "_yw_cte_4_agg" ON "p3"."tonode" = "_yw_cte_4_agg"."fromnode"
    GROUP BY "p3"."fromnode", "p3"."tonode"
  ),
  "_yw_cte_6" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM("_yw_cte_5_agg"."_freq") AS _freq
    FROM "dblp" AS "p2"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_5"
      GROUP BY "fromnode"
    ) AS "_yw_cte_5_agg" ON "p2"."tonode" = "_yw_cte_5_agg"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_7" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_6_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_6"
      GROUP BY "fromnode"
    ) AS "_yw_cte_6_agg" ON "p1"."tonode" = "_yw_cte_6_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_7"
) TO '/tmp/aggjoin_dblp_freqprop_path07_enabled.csv' (FORMAT CSV, HEADER);

.print --- Rewritten query, extension disabled ---
PRAGMA disabled_optimizers='extension';
COPY (
WITH
  "_yw_cte_1" AS (
    SELECT "p7"."fromnode", "p7"."tonode",
           SUM(1.0) AS _freq
    FROM "dblp" AS "p7"
    JOIN "dblp" AS "p8" ON "p7"."tonode" = "p8"."fromnode"
    GROUP BY "p7"."fromnode", "p7"."tonode"
  ),
  "_yw_cte_2" AS (
    SELECT "p6"."fromnode", "p6"."tonode",
           SUM("_yw_cte_1_agg"."_freq") AS _freq
    FROM "dblp" AS "p6"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_1"
      GROUP BY "fromnode"
    ) AS "_yw_cte_1_agg" ON "p6"."tonode" = "_yw_cte_1_agg"."fromnode"
    GROUP BY "p6"."fromnode", "p6"."tonode"
  ),
  "_yw_cte_3" AS (
    SELECT "p5"."fromnode", "p5"."tonode",
           SUM("_yw_cte_2_agg"."_freq") AS _freq
    FROM "dblp" AS "p5"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_2"
      GROUP BY "fromnode"
    ) AS "_yw_cte_2_agg" ON "p5"."tonode" = "_yw_cte_2_agg"."fromnode"
    GROUP BY "p5"."fromnode", "p5"."tonode"
  ),
  "_yw_cte_4" AS (
    SELECT "p4"."fromnode", "p4"."tonode",
           SUM("_yw_cte_3_agg"."_freq") AS _freq
    FROM "dblp" AS "p4"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_3"
      GROUP BY "fromnode"
    ) AS "_yw_cte_3_agg" ON "p4"."tonode" = "_yw_cte_3_agg"."fromnode"
    GROUP BY "p4"."fromnode", "p4"."tonode"
  ),
  "_yw_cte_5" AS (
    SELECT "p3"."fromnode", "p3"."tonode",
           SUM("_yw_cte_4_agg"."_freq") AS _freq
    FROM "dblp" AS "p3"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_4"
      GROUP BY "fromnode"
    ) AS "_yw_cte_4_agg" ON "p3"."tonode" = "_yw_cte_4_agg"."fromnode"
    GROUP BY "p3"."fromnode", "p3"."tonode"
  ),
  "_yw_cte_6" AS (
    SELECT "p2"."fromnode", "p2"."tonode",
           SUM("_yw_cte_5_agg"."_freq") AS _freq
    FROM "dblp" AS "p2"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_5"
      GROUP BY "fromnode"
    ) AS "_yw_cte_5_agg" ON "p2"."tonode" = "_yw_cte_5_agg"."fromnode"
    GROUP BY "p2"."fromnode", "p2"."tonode"
  ),
  "_yw_cte_7" AS (
    SELECT "p1"."tonode",
           SUM("_yw_cte_6_agg"."_freq") AS _freq
    FROM "dblp" AS "p1"
    JOIN (
      SELECT "fromnode", SUM("_freq") AS "_freq"
      FROM "_yw_cte_6"
      GROUP BY "fromnode"
    ) AS "_yw_cte_6_agg" ON "p1"."tonode" = "_yw_cte_6_agg"."fromnode"
    GROUP BY "p1"."tonode"
  )
SELECT COALESCE(SUM("_freq"), 0) AS "count_star"
FROM "_yw_cte_7"
) TO '/tmp/aggjoin_dblp_freqprop_path07_disabled.csv' (FORMAT CSV, HEADER);
PRAGMA disabled_optimizers='';

DROP TABLE dblp;
