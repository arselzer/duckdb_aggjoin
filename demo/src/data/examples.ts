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
    title: 'Fanout SUM over a join',
    kind: 'operator',
    blurb:
      'A 1.2M-row fact joins a repeated-key dimension, expanding to 6M rows. AggJoin folds the dimension multiplicity into the aggregate instead of scanning the fanout.',
    setup: `CREATE OR REPLACE TABLE dim AS
  SELECT (i % 80000) AS x FROM range(400000) t(i);
CREATE OR REPLACE TABLE fact AS
  SELECT (i % 80000) AS x, (i % 997)::DOUBLE AS v
  FROM range(1200000) t(i);`,
    query: `SELECT SUM(fact.v) AS total
FROM fact JOIN dim ON fact.x = dim.x;`,
  },
  {
    id: 'high-fanout-count',
    title: '100M-row fanout COUNT',
    kind: 'operator',
    blurb:
      'A compact 250-key join expands to 100M rows. The fused operator computes the final count from key frequencies instead of walking the product.',
    setup: `CREATE OR REPLACE TABLE hf_r AS
  SELECT (i % 250)::INTEGER AS k FROM range(500000) t(i);
CREATE OR REPLACE TABLE hf_s AS
  SELECT (i % 250)::INTEGER AS k FROM range(50000) t(i);`,
    query: `SELECT COUNT(*) AS join_rows
FROM hf_r JOIN hf_s ON hf_r.k = hf_s.k;`,
  },
  {
    id: 'banded-count',
    title: 'Grouped COUNT by band',
    kind: 'operator',
    blurb:
      'A 72M-row fanout grouped into twelve output bands. The optimized plan aggregates at the key level before rolling up to the visible groups.',
    setup: `CREATE OR REPLACE TABLE band_r AS
  SELECT (i % 1200)::INTEGER AS k, (i % 12)::INTEGER AS band
  FROM range(720000) t(i);
CREATE OR REPLACE TABLE band_s AS
  SELECT (i % 1200)::INTEGER AS k FROM range(120000) t(i);`,
    query: `SELECT band_r.band, COUNT(*) AS n
FROM band_r JOIN band_s ON band_r.k = band_s.k
GROUP BY band_r.band
ORDER BY band_r.band;`,
  },
  {
    id: 'chain-aggregate-suite',
    title: 'Chain aggregate suite',
    kind: 'cascade',
    blurb:
      'SUM, AVG, MIN, and MAX over a 10M-row three-table chain. The cascade computes exact aggregate state while avoiding the expanded join.',
    setup: `CREATE OR REPLACE TABLE ca AS
  SELECT i AS k, (i % 97)::DOUBLE AS v FROM range(100000) t(i);
CREATE OR REPLACE TABLE cb AS
  SELECT i AS k, (i % 1000)::INTEGER AS j FROM range(100000) t(i);
CREATE OR REPLACE TABLE cc AS
  SELECT (i % 1000)::INTEGER AS j FROM range(100000) t(i);`,
    query: `SELECT SUM(ca.v) AS s, AVG(ca.v) AS a, MIN(ca.v) AS lo, MAX(ca.v) AS hi
FROM ca, cb, cc
WHERE ca.k = cb.k AND cb.j = cc.j;`,
  },
  {
    id: 'dblp-graph',
    title: 'DBLP graph · 3-hop paths',
    kind: 'cascade',
    blurb:
      'A real 1.05M-edge co-authorship graph (SNAP com-DBLP). Counting 3-edge paths already expands heavily; the cascade propagates per-node frequencies instead of building the product.',
    dataset: { file: 'dblp.parquet', table: 'edges', sizeLabel: '3.3 MB · 1.05M edges' },
    setup: '',
    query: `SELECT COUNT(*) AS three_hop_paths
FROM edges e1, edges e2, edges e3
WHERE e1.toNode = e2.fromNode
  AND e2.toNode = e3.fromNode;`,
  },
  {
    id: 'dblp-graph-4hop',
    title: 'DBLP graph · 4-hop paths',
    kind: 'cascade',
    blurb:
      'The same DBLP graph with one more hop. This is where the Yannakakis-style rewrite starts to look materially different from the native join tree.',
    dataset: { file: 'dblp.parquet', table: 'edges', sizeLabel: '3.3 MB · 1.05M edges' },
    setup: '',
    query: `SELECT COUNT(*) AS four_hop_paths
FROM edges e1, edges e2, edges e3, edges e4
WHERE e1.toNode = e2.fromNode
  AND e2.toNode = e3.fromNode
  AND e3.toNode = e4.fromNode;`,
  },
  {
    id: 'dblp-graph-5hop',
    title: 'DBLP graph · 5-hop paths',
    kind: 'cascade',
    blurb:
      'A 5-hop path count on the bundled graph. The native baseline has much more fanout to account for, while the cascade keeps propagating compact counts.',
    dataset: { file: 'dblp.parquet', table: 'edges', sizeLabel: '3.3 MB · 1.05M edges' },
    setup: '',
    query: `SELECT COUNT(*) AS five_hop_paths
FROM edges e1, edges e2, edges e3, edges e4, edges e5
WHERE e1.toNode = e2.fromNode
  AND e2.toNode = e3.fromNode
  AND e3.toNode = e4.fromNode
  AND e4.toNode = e5.fromNode;`,
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
