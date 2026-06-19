// Curated, self-contained demos. Each builds its own data with `range()` (no
// import needed) and is sized so the NATIVE baseline still completes in the
// browser's single-threaded engine — slower, so the speedup is visible, but not
// so large it OOMs. CREATE OR REPLACE makes re-running a card idempotent.

export type ExampleKind = 'operator' | 'propagation' | 'guard' | 'stress'

export interface ExampleDataset {
  file: string
  table: string
  sizeLabel: string
}

export interface Example {
  id: string
  title: string
  kind: ExampleKind
  blurb: string
  setup: string
  query: string
  /** A bundled dataset (from /public) to fetch + register before running. */
  dataset?: ExampleDataset | ExampleDataset[]
  /** False for heavy examples where the native baseline would make a click feel stuck. */
  autoBenchmark?: boolean
}

export const KIND_LABEL: Record<ExampleKind, string> = {
  operator: 'Fused operator',
  propagation: 'Native rewrite · aggregate propagation',
  guard: 'Planner guard',
  stress: 'Stress query · load only',
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
    kind: 'propagation',
    blurb:
      'SUM, AVG, MIN, and MAX over a 10M-row three-table chain. The aggregate propagation computes exact aggregate state while avoiding the expanded join.',
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
    kind: 'propagation',
    blurb:
      'A real 1.05M-edge co-authorship graph (SNAP com-DBLP). Counting 3-edge paths already expands heavily; aggregate propagation carries compact per-node frequencies instead of building the product.',
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
    kind: 'propagation',
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
    kind: 'propagation',
    blurb:
      'A 5-hop path count on the bundled graph. The native baseline has much more fanout to account for, while the aggregate propagation keeps propagating compact counts.',
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
    id: 'dblp-top-sources-4hop',
    title: 'DBLP graph · top 4-hop sources',
    kind: 'operator',
    blurb:
      'Ranks source nodes by their number of 4-hop paths on the bundled DBLP graph. This returns a readable top-20 table instead of just a scalar count.',
    dataset: { file: 'dblp.parquet', table: 'edges', sizeLabel: '3.3 MB · 1.05M edges' },
    setup: '',
    query: `SELECT e1.fromNode AS source, COUNT(*) AS paths
FROM edges e1, edges e2, edges e3, edges e4
WHERE e1.toNode = e2.fromNode
  AND e2.toNode = e3.fromNode
  AND e3.toNode = e4.fromNode
GROUP BY e1.fromNode
ORDER BY paths DESC
LIMIT 20;`,
  },
  {
    id: 'stats-top-users',
    title: 'STATS-CEB · top active users',
    kind: 'operator',
    blurb:
      'A real StackExchange-style STATS-CEB subset. It ranks users by the fanout between their posts and comments, avoiding a 56M-row join product.',
    dataset: [
      { file: 'stats_ceb/users.parquet', table: 'users', sizeLabel: '350 KB' },
      { file: 'stats_ceb/posts.parquet', table: 'posts', sizeLabel: '1.1 MB' },
      { file: 'stats_ceb/comments.parquet', table: 'comments', sizeLabel: '1.6 MB' },
    ],
    setup: '',
    query: `SELECT u.Id AS user_id, COUNT(*) AS interaction_paths
FROM users u
JOIN posts p ON u.Id = p.OwnerUserId
JOIN comments c ON u.Id = c.UserId
GROUP BY u.Id
ORDER BY interaction_paths DESC
LIMIT 20;`,
  },
  {
    id: 'stats-users-star-stress',
    title: 'STATS-CEB · 15B-row user star',
    kind: 'stress',
    blurb:
      'A heavy real-data COUNT over users, badges, posts, and comments. Optimized locally in ~0.13s; native took ~40s, so the card loads the query without auto-running the baseline.',
    dataset: [
      { file: 'stats_ceb/users.parquet', table: 'users', sizeLabel: '350 KB' },
      { file: 'stats_ceb/badges.parquet', table: 'badges', sizeLabel: '587 KB' },
      { file: 'stats_ceb/posts.parquet', table: 'posts', sizeLabel: '1.1 MB' },
      { file: 'stats_ceb/comments.parquet', table: 'comments', sizeLabel: '1.6 MB' },
    ],
    setup: '',
    autoBenchmark: false,
    query: `SELECT COUNT(*) AS rows
FROM users u
JOIN badges b ON u.Id = b.UserId
JOIN posts p ON u.Id = p.OwnerUserId
JOIN comments c ON u.Id = c.UserId;`,
  },
  {
    id: 'stats-ceb-q58',
    title: 'STATS-CEB official q58',
    kind: 'stress',
    blurb:
      'The largest official STATS-CEB workload query by answer cardinality: six relations and a 17.85B-row count. Optimized locally in ~1.31s; native exceeded 60s.',
    dataset: [
      { file: 'stats_ceb/posts.parquet', table: 'posts', sizeLabel: '1.1 MB' },
      { file: 'stats_ceb/postLinks.parquet', table: 'postLinks', sizeLabel: '118 KB' },
      { file: 'stats_ceb/postHistory.parquet', table: 'postHistory', sizeLabel: '2.1 MB' },
      { file: 'stats_ceb/votes.parquet', table: 'votes', sizeLabel: '1.2 MB' },
      { file: 'stats_ceb/badges.parquet', table: 'badges', sizeLabel: '587 KB' },
      { file: 'stats_ceb/users.parquet', table: 'users', sizeLabel: '350 KB' },
    ],
    setup: '',
    autoBenchmark: false,
    query: `SELECT COUNT(*)
FROM posts AS p, postLinks AS pl, postHistory AS ph, votes AS v, badges AS b, users AS u
WHERE p.Id = pl.RelatedPostId
  AND u.Id = p.OwnerUserId
  AND u.Id = b.UserId
  AND u.Id = ph.UserId
  AND u.Id = v.UserId
  AND p.CommentCount >= 0
  AND p.CommentCount <= 13
  AND ph.PostHistoryTypeId = 5
  AND ph.CreationDate <= '2014-08-13 09:20:10'::timestamp
  AND v.CreationDate >= '2010-07-19 00:00:00'::timestamp
  AND b.Date <= '2014-09-09 10:24:35'::timestamp
  AND u.Views >= 0
  AND u.DownVotes >= 0
  AND u.CreationDate >= '2010-08-04 16:59:53'::timestamp
  AND u.CreationDate <= '2014-07-22 15:15:22'::timestamp;`,
  },
  {
    id: 'stats-ceb-q120',
    title: 'STATS-CEB official q120',
    kind: 'stress',
    blurb:
      'The second-largest official STATS-CEB workload query by answer cardinality: 11.64B rows over postHistory, posts, users, and badges.',
    dataset: [
      { file: 'stats_ceb/postHistory.parquet', table: 'postHistory', sizeLabel: '2.1 MB' },
      { file: 'stats_ceb/posts.parquet', table: 'posts', sizeLabel: '1.1 MB' },
      { file: 'stats_ceb/users.parquet', table: 'users', sizeLabel: '350 KB' },
      { file: 'stats_ceb/badges.parquet', table: 'badges', sizeLabel: '587 KB' },
    ],
    setup: '',
    autoBenchmark: false,
    query: `SELECT COUNT(*)
FROM postHistory AS ph, posts AS p, users AS u, badges AS b
WHERE b.UserId = u.Id
  AND p.OwnerUserId = u.Id
  AND ph.UserId = u.Id
  AND ph.CreationDate >= '2010-07-19 19:52:31'::timestamp
  AND p.Score >= 0
  AND u.CreationDate >= '2010-07-27 02:56:06'::timestamp
  AND u.CreationDate <= '2014-09-10 10:44:00'::timestamp;`,
  },
  {
    id: 'stats-ceb-q122',
    title: 'STATS-CEB official q122',
    kind: 'stress',
    blurb:
      'The third-largest official STATS-CEB workload query by answer cardinality: 11.21B rows over the same four-relation user-centered join.',
    dataset: [
      { file: 'stats_ceb/postHistory.parquet', table: 'postHistory', sizeLabel: '2.1 MB' },
      { file: 'stats_ceb/posts.parquet', table: 'posts', sizeLabel: '1.1 MB' },
      { file: 'stats_ceb/users.parquet', table: 'users', sizeLabel: '350 KB' },
      { file: 'stats_ceb/badges.parquet', table: 'badges', sizeLabel: '587 KB' },
    ],
    setup: '',
    autoBenchmark: false,
    query: `SELECT COUNT(*)
FROM postHistory AS ph, posts AS p, users AS u, badges AS b
WHERE b.UserId = u.Id
  AND p.OwnerUserId = u.Id
  AND ph.UserId = u.Id
  AND ph.CreationDate >= '2010-07-27 18:08:19'::timestamp
  AND ph.CreationDate <= '2014-09-10 08:22:43'::timestamp
  AND p.PostTypeId = 2;`,
  },
  {
    id: 'agg-propagation',
    title: 'Three-table chain COUNT',
    kind: 'propagation',
    blurb:
      'COUNT(*) over a chain that blows up to 6M join rows. Aggregate propagation carries per-key frequencies and never materialises the product — the plan stays native (no AGGJOIN node), but the 6M-row build vanishes.',
    setup: `CREATE OR REPLACE TABLE pt0 AS SELECT i AS k FROM range(60000) t(i);
CREATE OR REPLACE TABLE pt1 AS SELECT i AS k, (i % 600) AS j FROM range(60000) t(i);
CREATE OR REPLACE TABLE pt2 AS SELECT (i % 600) AS j FROM range(60000) t(i);`,
    query: `SELECT COUNT(*) AS n
FROM pt0, pt1, pt2
WHERE pt0.k = pt1.k AND pt1.j = pt2.j;`,
  },
  {
    id: 'tree-star-propagation',
    title: 'Tree-shaped star COUNT + SUM',
    kind: 'propagation',
    blurb:
      'A branching join tree rather than a path. Aggregate propagation folds three dimension joins into compact per-key frequencies.',
    setup: `CREATE OR REPLACE TABLE star_fact AS
  SELECT (i % 20000)::INTEGER AS k1,
         (i % 20000)::INTEGER AS k2,
         (i % 20000)::INTEGER AS k3,
         (i % 101)::INTEGER AS v
  FROM range(100000) t(i);
CREATE OR REPLACE TABLE star_d1 AS
  SELECT (i % 20000)::INTEGER AS k FROM range(100000) t(i);
CREATE OR REPLACE TABLE star_d2 AS
  SELECT (i % 20000)::INTEGER AS k FROM range(100000) t(i);
CREATE OR REPLACE TABLE star_d3 AS
  SELECT (i % 20000)::INTEGER AS k FROM range(100000) t(i);`,
    query: `SELECT COUNT(*) AS join_rows, SUM(star_fact.v) AS total_v
FROM star_fact, star_d1, star_d2, star_d3
WHERE star_fact.k1 = star_d1.k
  AND star_fact.k2 = star_d2.k
  AND star_fact.k3 = star_d3.k;`,
  },
  {
    id: 'composite-key-chain',
    title: 'Composite-key chain',
    kind: 'propagation',
    blurb:
      'Two composite equality edges over a three-table chain. The rewrite uses key-domain stats to avoid the native fanout.',
    setup: `CREATE OR REPLACE TABLE ck0 AS
  SELECT (i % 600)::INTEGER AS a,
         (floor(i / 600) % 5)::INTEGER AS b,
         (i % 17)::INTEGER AS v
  FROM range(60000) t(i);
CREATE OR REPLACE TABLE ck1 AS
  SELECT (i % 600)::INTEGER AS a,
         (floor(i / 600) % 5)::INTEGER AS b,
         (i % 500)::INTEGER AS c,
         (floor(i / 500) % 6)::INTEGER AS d
  FROM range(60000) t(i);
CREATE OR REPLACE TABLE ck2 AS
  SELECT (i % 500)::INTEGER AS c,
         (floor(i / 500) % 6)::INTEGER AS d
  FROM range(60000) t(i);`,
    query: `SELECT COUNT(*) AS n, SUM(ck0.v) AS total_v
FROM ck0, ck1, ck2
WHERE ck0.a = ck1.a AND ck0.b = ck1.b
  AND ck1.c = ck2.c AND ck1.d = ck2.d;`,
  },
  {
    id: 'noop-filter-projection',
    title: 'Filtered projection recovery',
    kind: 'propagation',
    blurb:
      'Projection-wrapped leaves with filters that table statistics prove are no-ops. The planner recovers the base cardinality before gating.',
    setup: `CREATE OR REPLACE TABLE nf0 AS
  SELECT i AS b FROM range(100000) t(i);
CREATE OR REPLACE TABLE nf1 AS
  SELECT i AS b, (i % 500)::INTEGER AS c FROM range(100000) t(i);
CREATE OR REPLACE TABLE nf2 AS
  SELECT (i % 500)::INTEGER AS c FROM range(100000) t(i);`,
    query: `SELECT COUNT(*) AS n
FROM (SELECT b FROM nf0 WHERE b >= 0) f0
JOIN (SELECT b, c FROM nf1 WHERE b IS NOT NULL) f1 ON f0.b = f1.b
JOIN (SELECT c FROM nf2 WHERE c IS NOT NULL) f2 ON f1.c = f2.c;`,
  },
  {
    id: 'unique-chain-guard',
    title: 'Unique-key guard case',
    kind: 'guard',
    blurb:
      'A large but near-1:1 acyclic join. The planner should keep DuckDB native because extra propagation GROUP BYs would just add work.',
    setup: `CREATE OR REPLACE TABLE ug0 AS
  SELECT i AS b, (i % 17)::INTEGER AS v FROM range(120000) t(i);
CREATE OR REPLACE TABLE ug1 AS
  SELECT i AS b, i AS c FROM range(120000) t(i);
CREATE OR REPLACE TABLE ug2 AS
  SELECT i AS c, i AS d FROM range(120000) t(i);
CREATE OR REPLACE TABLE ug3 AS
  SELECT i AS d FROM range(120000) t(i);`,
    query: `SELECT SUM(ug0.v) AS s, AVG(ug0.v) AS a
FROM ug0, ug1, ug2, ug3
WHERE ug0.b = ug1.b
  AND ug1.c = ug2.c
  AND ug2.d = ug3.d;`,
  },
  {
    id: 'exact-variance',
    title: 'Exact VAR_POP over a join',
    kind: 'propagation',
    blurb:
      'Variance over the same 6M-row chain, via exact integer moments. The aggregate propagation is not just faster — it is deterministic and correctly-rounded, where native parallel Welford varies by ~1e-9 across runs.',
    setup: `CREATE OR REPLACE TABLE vt0 AS SELECT i AS k, (i % 97) AS v FROM range(60000) t(i);
CREATE OR REPLACE TABLE vt1 AS SELECT i AS k, (i % 600) AS j FROM range(60000) t(i);
CREATE OR REPLACE TABLE vt2 AS SELECT (i % 600) AS j FROM range(60000) t(i);`,
    query: `SELECT round(VAR_POP(vt0.v), 6) AS variance
FROM vt0, vt1, vt2
WHERE vt0.k = vt1.k AND vt1.j = vt2.j;`,
  },
]
