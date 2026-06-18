// Curated, self-contained demos. Each builds its own data with `range()` (no
// import needed) and is sized so the NATIVE baseline still completes in the
// browser's single-threaded engine — slower, so the speedup is visible, but not
// so large it OOMs. CREATE OR REPLACE makes re-running a card idempotent.

export type ExampleKind = 'operator' | 'cascade'

export interface Example {
  id: string
  title: string
  kind: ExampleKind
  blurb: string
  setup: string
  query: string
  /** A bundled dataset (from /public) to fetch + register before running. */
  dataset?: { file: string; table: string; sizeLabel: string }
}

export const KIND_LABEL: Record<ExampleKind, string> = {
  operator: 'Fused operator',
  cascade: 'Native rewrite · Yannakakis',
}

export const examples: Example[] = [
  {
    id: 'varchar-dense',
    title: 'Dense string-key COUNT',
    kind: 'operator',
    blurb:
      'A 100-key VARCHAR join. The fused operator counts per key without ever building the ~9M-row join product the native plan has to.',
    setup: `CREATE OR REPLACE TABLE r_str AS
  SELECT 'k' || lpad((i % 100)::VARCHAR, 3, '0') AS x
  FROM range(300000) t(i);
CREATE OR REPLACE TABLE s_str AS
  SELECT 'k' || lpad((i % 100)::VARCHAR, 3, '0') AS x
  FROM range(30000) t(i);`,
    query: `SELECT r_str.x, COUNT(*) AS n
FROM r_str JOIN s_str ON r_str.x = s_str.x
GROUP BY r_str.x
ORDER BY r_str.x;`,
  },
  {
    id: 'asymmetric-sum',
    title: 'Asymmetric SUM over a join',
    kind: 'operator',
    blurb:
      'A 2M-row fact probing a 100K-key dimension. AggJoin fuses the aggregate into the probe — no 2M-row intermediate to scan twice.',
    setup: `CREATE OR REPLACE TABLE dim AS
  SELECT i AS x FROM range(100000) t(i);
CREATE OR REPLACE TABLE fact AS
  SELECT (i % 100000) AS x, (i % 997)::DOUBLE AS v
  FROM range(2000000) t(i);`,
    query: `SELECT SUM(fact.v) AS total
FROM fact JOIN dim ON fact.x = dim.x;`,
  },
  {
    id: 'grouped-sum',
    title: 'Grouped SUM by join key',
    kind: 'operator',
    blurb:
      'The bread-and-butter shape: SUM grouped by the join key. Look for the AGGJOIN node replacing the HASH_GROUP_BY ▸ HASH_JOIN pair.',
    setup: `CREATE OR REPLACE TABLE keys AS
  SELECT i AS x FROM range(150000) t(i);
CREATE OR REPLACE TABLE rows_t AS
  SELECT (i % 150000) AS x, (i % 500)::DOUBLE AS v
  FROM range(1500000) t(i);`,
    query: `SELECT rows_t.x, SUM(rows_t.v) AS s, MIN(rows_t.v) AS lo, MAX(rows_t.v) AS hi
FROM rows_t JOIN keys ON rows_t.x = keys.x
GROUP BY rows_t.x;`,
  },
  {
    id: 'dblp-graph',
    title: 'DBLP graph · 3-hop paths',
    kind: 'cascade',
    blurb:
      'A real 1.05M-edge co-authorship graph (SNAP com-DBLP). Counting 3-edge paths explodes to 67M — the Yannakakis cascade propagates per-node frequencies instead of building that product. The win climbs steeply with path length (≈8× at 4 hops, ≈24× at 5).',
    dataset: { file: 'dblp.parquet', table: 'edges', sizeLabel: '3.3 MB · 1.05M edges' },
    setup: '',
    query: `SELECT COUNT(*) AS three_hop_paths
FROM edges e1, edges e2, edges e3
WHERE e1.toNode = e2.fromNode
  AND e2.toNode = e3.fromNode;`,
  },
  {
    id: 'chain-count',
    title: 'Three-table chain COUNT',
    kind: 'cascade',
    blurb:
      'COUNT(*) over a chain that blows up to 6M join rows. The Yannakakis cascade propagates per-key frequencies and never materialises the product — the plan stays native (no AGGJOIN node), but the 6M-row build vanishes.',
    setup: `CREATE OR REPLACE TABLE pt0 AS SELECT i AS k FROM range(60000) t(i);
CREATE OR REPLACE TABLE pt1 AS SELECT i AS k, (i % 600) AS j FROM range(60000) t(i);
CREATE OR REPLACE TABLE pt2 AS SELECT (i % 600) AS j FROM range(60000) t(i);`,
    query: `SELECT COUNT(*) AS n
FROM pt0, pt1, pt2
WHERE pt0.k = pt1.k AND pt1.j = pt2.j;`,
  },
  {
    id: 'exact-variance',
    title: 'Exact VAR_POP over a join',
    kind: 'cascade',
    blurb:
      'Variance over the same 6M-row chain, via exact integer moments. The cascade is not just faster — it is deterministic and correctly-rounded, where native parallel Welford varies by ~1e-9 across runs.',
    setup: `CREATE OR REPLACE TABLE vt0 AS SELECT i AS k, (i % 97) AS v FROM range(60000) t(i);
CREATE OR REPLACE TABLE vt1 AS SELECT i AS k, (i % 600) AS j FROM range(60000) t(i);
CREATE OR REPLACE TABLE vt2 AS SELECT (i % 600) AS j FROM range(60000) t(i);`,
    query: `SELECT round(VAR_POP(vt0.v), 6) AS variance
FROM vt0, vt1, vt2
WHERE vt0.k = vt1.k AND vt1.j = vt2.j;`,
  },
]
