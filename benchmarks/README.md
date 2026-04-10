# AggJoin Benchmarks

Benchmarks for `PhysicalAggJoin` versus native DuckDB plans on DuckDB `v1.5.1`.

The targeted follow-up tables below were refreshed locally on April 9, 2026,
after the native build-preaggregation rewrite was extended to cover narrow
balanced build-heavy nonnumeric, mixed build-side, and grouped mixed
probe/build aggregate sets, and after the narrow single-key `VARCHAR` path was
widened and optimized further. The long
`bench.sql` and `bench_scaling.sql` native baselines did not complete cleanly on
this host during the latest rerun, so those two tables remain the prior full
snapshot and are called out explicitly below.

## Setup

```bash
cd duckdb_aggjoin
git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb
make
```

After source changes, `touch src/aggjoin_optimizer.cpp` before rebuilding to avoid
stale extension binaries.

## Running benchmarks

```bash
build/Release/duckdb < benchmarks/bench.sql
build/Release/duckdb < benchmarks/bench_scaling.sql
build/Release/duckdb < benchmarks/bench_asymmetric.sql
build/Release/duckdb < benchmarks/bench_cte_chain.sql
build/Release/duckdb < benchmarks/bench_native_ht_composite.sql
build/Release/duckdb < benchmarks/bench_build_side_path.sql
build/Release/duckdb < benchmarks/bench_build_side_suite.sql
build/Release/duckdb < benchmarks/bench_build_side_nonnumeric.sql
build/Release/duckdb < benchmarks/bench_build_side_mixed.sql
build/Release/duckdb < benchmarks/bench_probe_build_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_probe_build_mixed.sql
build/Release/duckdb < benchmarks/bench_asymmetric_mixed.sql
build/Release/duckdb < benchmarks/bench_sparse_gate.sql
build/Release/duckdb < benchmarks/bench_hash_nonintegral.sql
build/Release/duckdb < benchmarks/bench_nulls.sql
build/Release/duckdb < benchmarks/bench_composite_keys.sql
build/Release/duckdb < benchmarks/bench_latency.sql
build/Release/duckdb < benchmarks/bench_memory.sql
build/Release/duckdb < benchmarks/bench_varchar.sql
build/Release/duckdb < benchmarks/bench_varchar_dense.sql
build/Release/duckdb < benchmarks/bench_varchar_sweep.sql
build/Release/duckdb < benchmarks/bench_varchar_variants.sql
build/Release/duckdb < benchmarks/bench_varchar_keys.sql
build/Release/duckdb < benchmarks/bench_correctness.sql
```

## Methodology

- `bench.sql`, `bench_scaling.sql`, and `bench_asymmetric.sql` benchmark the aggregate
  query directly via `COPY (...) TO ...`; they no longer wrap the query in
  `COUNT(*) FROM (...)`, which could benchmark the wrong plan shape.
- For same-binary native baselines on targeted follow-up cases, the benchmark files
  use `PRAGMA disabled_optimizers='extension'` around the comparison query.
- Most numeric single-key benchmark shapes now hit direct or segmented-direct
  paths. `bench_hash_nonintegral.sql` now tracks the current best non-integral
  single-key strategy; on the current build that query rewrites to a native
  build-preaggregation plan rather than exercising the older custom hash path.
- `AGGJOIN_TRACE=1` logs planner fire/bail reasons. `AGGJOIN_TRACE_STATS=1`
  additionally logs runtime path and observed row/group counts, which is useful
  for the targeted path-validation benchmarks below.
- `bench_varchar_dense.sql` is the direct benchmark for the new narrow single-key
  `VARCHAR` hash path for numeric `SUM/COUNT/AVG/MIN/MAX`. `bench_varchar_keys.sql`
  remains a wrapper-based smoke test.
- `bench_varchar_sweep.sql` is the boundary benchmark for grouped single-key
  `VARCHAR` `SUM+MIN+MAX`, showing where the current planner keeps the narrow
  path and where it bails back to native.
- `bench_varchar_variants.sql` is the follow-up for grouped `COUNT(*)` and `AVG`
  on the same narrow single-key `VARCHAR` path. Those aggregate mixes stay
  competitive farther out than the heavier `SUM+MIN+MAX` sweep.
- `bench_varchar_keys.sql` still uses the older wrapper shape and is best treated as
  a smoke test, not a primary performance result.
- The latest local run used the standard current build with planner gating enabled.
  The targeted follow-up suites below completed cleanly. The long native halves
  of `bench.sql` and `bench_scaling.sql` were interrupted on this environment,
  so those two sections still show the previous full-run snapshot.

## Core suite

10M probe rows, single-key `SUM` unless noted otherwise.

These numbers are still the previous full-run snapshot. On the latest local
rerun, `bench.sql` was killed during the slow native Zipf half after the early
direct and hash cases completed, so the table below remains the most recent
complete run.

| # | Scenario | AGGJOIN | Native | Speedup |
|---|----------|---------|--------|---------|
| 1 | Direct mode, 100K keys | 0.236s | 13.807s | **58.5x** |
| 2 | Direct mode, 1M keys | 1.458s | 1.825s | **1.3x** |
| 3 | Hash mode, 3M keys | 0.396s | 0.984s | **2.5x** |
| 4 | Zipf-skewed, 100K keys | 0.138s | 5.759s | **41.7x** |
| 5 | Sparse, 100K rows over 10M range | 0.028s | 0.021s | 0.8x |
| 6 | High blowup, 10K keys (1000x) | 0.080s | 8.092s | **101.2x** |
| 7 | Multi-agg `SUM+MIN+MAX+AVG`, 1M keys | 3.382s | 4.017s | **1.2x** |

## Scaling curve

10M probe rows, probe-side `GROUP BY`.

This table is also the previous full-run snapshot. The latest local rerun of
`bench_scaling.sql` did not finish on this host because the native `1K` case was
still running after several minutes.

| Keys | Mode | AGGJOIN | Native | Speedup |
|------|------|---------|--------|---------|
| 1K | Direct | 0.146s | 8.002s | **54.8x** |
| 10K | Direct | 0.249s | 8.125s | **32.6x** |
| 100K | Direct | 0.699s | 8.994s | **12.9x** |
| 500K | Direct | 1.310s | 9.669s | **7.4x** |
| 1M | Direct | 1.749s | 9.511s | **5.4x** |
| 2M | Direct boundary | 2.574s | 10.155s | **3.9x** |
| 3M | Segmented direct | 3.277s | 10.404s | **3.2x** |
| 5M | Segmented direct | 5.003s | 11.672s | **2.3x** |

The biggest recent improvements are:

- the old `1M -> 2M` cliff is largely gone, so the 2M-key case stays much closer
  to the direct-mode region instead of dropping immediately into a much slower path
- the simplest `GROUP BY join_key` shape now stays out of hash mode through the
  3M and 5M-key cases via the segmented direct path
- the segmented multi-aggregate path now cuts the existing `1M`-key
  `SUM+MIN+MAX+AVG` benchmark from `6.7s` to `3.3s`; on a targeted `3M`-key
  grouped multi-aggregate query it reaches near-native parity (`6.00s` vs `5.99s`)

Other relevant follow-up optimizations now in the code:

- hash-mode range prefilter scratch is reused across chunks and now supports
  `INT8/16` and `UINT8/16` probe keys
- a selective DuckDB native `GroupedAggregateHashTable` path is enabled for
  composite grouping/join shapes using `SUM/MIN/MAX`
- grouped direct-mode emit for large `GROUP BY join_key` cases now tracks active
  matched keys instead of scanning the full key range at source time
- several native/hash fallback paths now use typed extraction for more integer
  widths instead of falling back to `GetValue()` boxing
- the weak-shape planner gate now also uses estimated probe/build/group
  cardinalities instead of only structural checks
- grouped direct and segmented-direct emit no longer sort active keys before
  returning batches
- a narrow segmented multi-aggregate path is enabled for grouped probe-side
  numeric `SUM/COUNT/AVG/MIN/MAX`

### Segmented multi-aggregate follow-up

Targeted grouped probe-side numeric multi-aggregate query:

| Query shape | AGGJOIN | Native | Result |
|-------------|---------|--------|--------|
| 1M keys, `SUM+MIN+MAX+AVG` | 3.317s | 24.802s | **7.5x faster** |
| 3M keys, grouped multi-agg follow-up | 6.002s | 5.988s | near parity |

### Non-integral single-key follow-up

`bench_hash_nonintegral.sql` now tracks the current best non-integral
single-key strategy rather than the older custom AGGJOIN hash path. On the
current build this query rewrites to a native build-preaggregation plan:

| Query shape | Direct query | Native baseline | Planner outcome |
|-------------|--------------|-----------------|-----------------|
| `DOUBLE` key, 100K keys, 1M rows | 0.062s | 0.128s | native build preagg rewrite |

So this workload is no longer a weak AGGJOIN hash case in practice; the current
planner lowering beats the disabled-extension baseline on the same logical query.

### Planner/path validation follow-up

These smaller benchmarks are meant to validate specific execution paths rather
than replace the core suite above.

| Benchmark | Observed path | Direct query | Native baseline | Result |
|-----------|---------------|--------------|-----------------|--------|
| `bench_native_ht_composite.sql` | `native_ht` | 0.244s | 2.090s | strong win on selective composite path |
| `bench_build_side_path.sql` | planner bail to native | 0.213s | 0.174s | near parity, bad build-side AGGJOIN plan avoided |
| `bench_sparse_gate.sql` | planner bail to native | 0.024s | 0.023s | near parity, bad AGGJOIN plan avoided |

Representative trace outcomes from these follow-up runs:

- `bench_native_ht_composite.sql`: `planner fired`, `path=native_ht`
- `bench_build_side_path.sql`: `planner cost gate would bail: build-side aggregate mix outside direct fast path`
- `bench_sparse_gate.sql`: `planner cost gate would bail: low estimated fanout`

### Build-side suite follow-up

The newer [bench_build_side_suite.sql](./bench_build_side_suite.sql)
benchmark family was added specifically to measure the remaining build-side
execution gap. The latest native-build-preaggregation rewrite materially
improves the balanced numeric cases, and now also covers grouped numeric
`MIN/MAX`, while the asymmetric build-heavy case still stays near native because
the planner bails it away from AGGJOIN:

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, `SUM+COUNT+AVG`, 100K keys / 1M rows | 0.160s | 0.293s | **1.8x faster** via native build preagg rewrite |
| Grouped by join key, `SUM+MIN+MAX+AVG`, 100K keys / 1M rows | 0.157s | 0.411s | **2.6x faster** via native build preagg rewrite |
| Ungrouped, `SUM+COUNT+AVG`, 100K keys / 1M rows | 0.072s | 0.127s | **1.8x faster** via native build preagg rewrite |
| Asymmetric build-heavy, `100K probe x 5M build`, grouped `SUM+COUNT` | 0.189s | 0.161s | near parity, planner bails |

Related failed attempts so far:

- a narrower direct-family build-side payload accumulation pass was benchmarked
  and reverted; it only moved the older `bench_build_side_path.sql` case from
  `0.549s` to `0.531s`, still far behind native `0.195s`
- an earlier partial-build-preaggregation attempt that tried to force AGGJOIN
  back into the post-preagg plan was decisively slower than a fully native
  rewrite and was reverted

### Build-side non-numeric follow-up

The native build-preaggregation lowering now also reaches a narrow build-heavy
non-numeric `MIN/MAX` class. These shapes still do not use AGGJOIN's old
Value-heavy build-side execution path; they lower to native grouped preagg
subplans instead.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, `VARCHAR MIN+MAX`, 100K keys / 1M rows | 0.180s | 0.307s | **1.7x faster** via native build preagg rewrite |
| Ungrouped, `DATE MIN+MAX`, 100K keys / 1M rows | 0.064s | 0.152s | **2.4x faster** via native build preagg rewrite |

Representative trace for the grouped `VARCHAR MIN+MAX` case:

- `native-build-preagg candidate: groups=1 aggs=2 build_aggs=2`
- `planner rewrite: native build preagg`

### Build-side mixed follow-up

The same native build-preaggregation lowering also covers narrow balanced
mixed build-side numeric + non-numeric aggregate sets.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, `SUM + VARCHAR MIN+MAX`, 100K keys / 1M rows | 0.219s | 0.326s | **1.5x faster** via native build preagg rewrite |
| Ungrouped, `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.077s | 0.129s | **1.7x faster** via native build preagg rewrite |

Representative trace for the grouped mixed case:

- `native-build-preagg candidate: groups=1 aggs=3 build_aggs=3`
- `planner rewrite: native build preagg`

### Mixed probe/build follow-up

The native build-preaggregation lowering now also covers a narrow single-key
class where aggregate payloads appear on both sides of the join. The current
kept rewrite is still deliberately narrow: grouped by join key or ungrouped,
single join key, balanced large inputs, and supported `SUM/COUNT/AVG/MIN/MAX`
families.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, probe `SUM` + build `SUM + VARCHAR MIN+MAX`, 100K keys / 1M rows | 0.093s | 0.192s | **2.1x faster** via native mixed-side preagg rewrite |
| Grouped by join key, probe `SUM` + build `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.047s | 0.138s | **2.9x faster** via native mixed-side preagg rewrite |
| Ungrouped, probe `SUM` + build `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.036s | 0.073s | **2.0x faster** via native mixed-side preagg rewrite |
| Grouped by join key, probe `AVG` + build `AVG + DATE MIN+MAX`, 100K keys / 1M rows | 0.053s | 0.144s | **2.7x faster** via native mixed-side preagg rewrite |

Representative trace for the grouped mixed-side case:

- `planner rewrite: native mixed-side preagg`

### Composite mixed probe/build follow-up

The same native mixed-side preaggregation idea now also covers a narrow
grouped-by-join-key composite-key class. This is still intentionally limited:
two-sided aggregate payloads, grouped by the full composite join key, balanced
large inputs, and supported `SUM/COUNT/AVG/MIN/MAX` families.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `1K x 100` keys / `1M` rows | 0.332s | 0.510s | **1.5x faster** via native mixed-side preagg rewrite |

### Asymmetric mixed probe/build follow-up

The mixed-side native-preaggregation rewrite is still not a good fit for
build-heavy asymmetric single-key shapes, but a later narrow widening showed
that probe-heavy grouped single-key mixed shapes can still benefit.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped `SUM + SUM + DATE MIN+MAX`, `100K x 5M` | 0.288s | 0.277s | near parity, still better left native-first |
| Large probe / small build, grouped `SUM + SUM + DATE MIN+MAX`, `5M x 100K` | 0.102s | 0.286s | **2.8x faster** via widened mixed-side preagg rewrite |

## Asymmetric table ratios

| Scenario | Probe | Build | AGGJOIN | Native | Speedup |
|----------|-------|-------|---------|--------|---------|
| Small probe, large build | 100K | 5M | 0.153s | 3.775s | **24.7x** |
| Large probe, tiny build | 10M | 1K | 0.061s | 0.060s | 1.0x |
| 10x ratio | 1M | 100K | 0.114s | 0.206s | **1.8x** |
| 100x ratio | 10M | 100K | 0.137s | 1.239s | **9.0x** |

AGGJOIN still does best on the Yannakakis-like shapes where probe is much larger
than build.

## Other suites

### CTE chain

4-cycle schema `R(x,w), S(x,y), T(y,z), U(z,w)`, 1M rows each:

| Query | Time |
|-------|------|
| 3-step CTE frequency propagation | 0.453s |
| Native comparison query | 0.616s |
| Speedup | **1.4x** |

### NULL-heavy data

100K keys, 10M rows:

| Scenario | Time | Change vs baseline |
|----------|------|--------------------|
| 0% NULLs | 1.495s | baseline |
| 10% NULLs in aggregate column | 1.509s | +0.8% |
| 50% NULLs in aggregate column | 1.473s | -1.6% |
| 10% NULLs in join key | 1.455s | -2.8% |

This run did not show a material NULL penalty.

### Composite keys

| Scenario | AGGJOIN | Native | Speedup |
|----------|---------|--------|---------|
| 2-column key, 100K unique combos | 0.994s | 0.956s | 0.96x |
| 3-column key, 1M rows | 9.007s | 8.936s | 1.0x |

Composite-key workloads remain roughly parity with native DuckDB, with small wins
possible on targeted shapes after the selective native grouped-HT fallback.

On the current build, the benchmark-informed planner gate may intentionally bail the
widest composite shapes to native plans instead of forcing AGGJOIN onto near-parity
cases. The 2-column case is still the more representative selective native-HT shape.

### VARCHAR and mixed-type aggregates

`bench_varchar.sql` compares the direct query shape against a forced materialized
baseline:

| Query | Direct query | Materialized baseline | Speedup |
|-------|--------------|-----------------------|---------|
| Ungrouped `MIN/MAX(VARCHAR)` | 0.116s | 0.101s | 0.9x |
| Grouped `MIN/MAX(VARCHAR)` | 0.136s | 0.270s | **2.0x** |
| Mixed `SUM(DOUBLE)+MIN/MAX(VARCHAR)` | 0.114s | 0.300s | **2.6x** |

`bench_varchar_keys.sql` completed cleanly, but it still uses the older wrapper
methodology. On the current build it is best treated as a planner-gate smoke test:
the direct query is intentionally routed to native for variable-width key shapes,
and the results remain roughly parity:

| Key shape | Direct query | Materialized baseline | Result |
|-----------|--------------|-----------------------|--------|
| Short VARCHAR keys | 0.178s | 0.177s | parity |
| Medium VARCHAR keys | 0.054s | 0.054s | parity |
| Long VARCHAR keys | 0.066s | 0.065s | parity |

The `bench_varchar.sql` results above compare the direct query shape against a
forced materialized baseline; they are not pure AGGJOIN-vs-native measurements.

`bench_varchar_dense.sql` is the direct benchmark for the narrow supported
single-key `VARCHAR` path:

| Query | AGGJOIN | Native baseline | Result |
|-------|---------|-----------------|--------|
| Dense `VARCHAR` grouped `SUM`, 1K keys / 100K rows | 0.010s | 0.198s | **19.8x faster** |
| Dense `VARCHAR` grouped `SUM+MIN+MAX`, 1K keys / 100K rows | 0.014s | 0.284s | **20.3x faster** |
| Dense `VARCHAR` ungrouped `SUM`, 1K keys / 100K rows | 0.009s | 0.049s | **5.4x faster** |
| Boundary grouped `MIN+MAX`, 10K keys / 100K rows | 0.063s | 0.061s | near parity, planner bails |

Representative trace/stats for this benchmark:

- grouped dense case: `planner fired`, `path=build_slot_hash`, `build_ht_count=1000`, `result_groups=1000`
- ungrouped case: `planner fired`, `path=hash`, `build_ht_count=1000`, `result_groups=1`
- 10K-key boundary case: `planner cost gate would bail: variable-width join/group key`

The latest grouped-path speedup comes from two runtime changes:
- storing exact single-key `VARCHAR` payloads directly on build entries and
  comparing `string_t` keys in the hot probe path instead of boxing through `Value`
- a fused single-pass `build_slot_hash` kernel for all-`DOUBLE`
  `SUM/COUNT/AVG/MIN/MAX` mixes, so grouped string-key queries no longer rescan
  the same matched rows once per aggregate

`bench_varchar_sweep.sql` is the grouped boundary sweep for the same narrow path:

| Keys | Direct query | Native baseline | Planner outcome |
|------|--------------|-----------------|-----------------|
| 1K | 0.010s | 0.289s | fires AGGJOIN |
| 2K | 0.013s | 0.164s | fires AGGJOIN |
| 5K | 0.022s | 0.091s | fires AGGJOIN |
| 10K | 0.064s | 0.064s | fires AGGJOIN, near parity |
| 20K | 0.045s | 0.044s | fires AGGJOIN, near parity |
| 50K | 0.034s | 0.035s | fires AGGJOIN, near parity |

On this `100K`-row setup, the current planner boundary is doing the right
practical thing for the heavier grouped `SUM+MIN+MAX` mix: the `VARCHAR` path
is clearly useful through about `5K` groups and then remains near parity much
farther out rather than falling off a cliff. The recent fused grouped kernel is
what pushed the `5K` case down materially.

`bench_varchar_variants.sql` fills in the lighter aggregate mixes on the same
single-key `VARCHAR` path:

| Query shape | Direct query | Native baseline | Planner outcome |
|-------------|--------------|-----------------|-----------------|
| Grouped `COUNT(*)`, 1K keys / 100K rows | 0.008s | 0.179s | fires AGGJOIN |
| Grouped `AVG`, 1K keys / 100K rows | 0.008s | 0.182s | fires AGGJOIN |
| Grouped `AVG`, 5K keys / 100K rows | 0.012s | 0.064s | fires AGGJOIN |
| Grouped `AVG`, 10K keys / 100K rows | 0.047s | 0.045s | fires AGGJOIN |
| Grouped `AVG`, 20K keys / 100K rows | not rerun in latest pass | not rerun in latest pass | older snapshot had slight loss |
| Grouped `AVG`, 50K keys / 100K rows | not rerun in latest pass | not rerun in latest pass | older snapshot had slight loss |

So the current `VARCHAR` boundary is aggregate-dependent:
- grouped `SUM+MIN+MAX` is worth keeping only on the denser end of the curve
- grouped `COUNT(*)` and `AVG` stay competitive farther out, reaching parity
  around `10K` groups and only slipping slightly behind native beyond that

Two later `VARCHAR` runtime follow-ups were benchmarked after the results above
and then reverted:
- exact packed short-string compare using DuckDB-style 128-bit string packing
- packed short-string hashing for single-key `VARCHAR` joins

Neither materially beat the current committed fused grouped kernel. The dense
`1K` grouped cases were mostly flat, while the `10K+` grouped and ungrouped
boundary cases were flat to slightly worse. So the current practical conclusion
is that the committed narrow grouped `VARCHAR` path has likely captured the easy
runtime win, and the next credible step in this area would need to be more
structural: either planner/runtime integration with internal `compress_string`
projections or a true interned/compressed-key path.

### Latency

`LIMIT 1` is still effectively full-work latency:

| Query | AGGJOIN | Native |
|-------|---------|--------|
| `LIMIT 1`, 100K keys | 0.223s | 0.222s |
| `LIMIT 1`, 1M keys | 0.234s | 0.220s |
| Full result, 100K keys | 1.581s | 1.599s |

The operator remains blocking.

### Memory-oriented smoke tests

`bench_memory.sql` completed cleanly through:

| Scenario | Time | Note |
|----------|------|------|
| 10K keys | 0.262s | stability/sizing check |
| 100K keys | 0.133s | stability/sizing check |
| 1M keys | 0.094s | stability/sizing check |
| 2M keys | 0.123s | stability/sizing check |
| 1M keys with 4 aggregates | 0.102s | wider aggregate payload |

These are stability/sizing checks more than comparative benchmarks.

## Correctness

`bench_correctness.sql` still completes, but it is not fully green:

| Check | Status | Note |
|-------|--------|------|
| Grouped `SUM` | `FAIL` | flawed benchmark comparison |
| Ungrouped `SUM` | `PASS` | |
| `COUNT` | `PASS` | |
| Ungrouped `AVG` | `PASS` | |
| Grouped `AVG` | `PASS` | |
| No-match case | `PASS` | |

The first grouped `SUM` check is not a reliable validator because it compares two
different groupings (`GROUP BY r.x` versus `GROUP BY s.y`), so its failure should not
be treated as a fresh regression by itself.

## Known limitations

| Limitation | Still relevant? | Notes |
|------------|------------------|-------|
| Blocking operator / poor `LIMIT` latency | Yes | `LIMIT 1` remains effectively full-work latency. |
| Composite-key and most variable-width key workloads are weak | Yes | Often near parity; the planner now bails the weakest shapes to native, with a narrow dense single-key `VARCHAR` numeric-path exception. |
| Non-numeric build-side `MIN/MAX` inside AGGJOIN | Partly | Narrow balanced grouped/ungrouped build-heavy shapes can now rewrite to native build-preaggregation plans, but the old AGGJOIN-internal path is still not used. |
| More than 4 build-side aggregates | No | The old fixed-size cap was removed; weak large build-side mixes now usually fall back to native for cost reasons instead. |
| `bench_varchar_keys.sql` is a primary performance benchmark | No | It is still a smoke/planner-gate check because of the wrapper methodology. |
| Catastrophically slow hash mode on the main numeric path | No | That limitation is obsolete after the current direct/segmented/hash work. |
| Repeated-execution segfault in the benchmark loop | No | This was fixed earlier and is no longer part of the current profile. |
| Multi-aggregate grouped queries are a major weak spot | Not broadly | The new segmented multi-aggregate path materially improved the 1M-key case and reaches near-native parity on a targeted 3M-key follow-up. |

## Current takeaways

- Numeric single-key workloads are now decisively strong, especially from 1K to 2M
  groups, and the segmented direct path keeps the simplest grouped `SUM` shape fast
  through 5M keys as well.
- The segmented multi-aggregate path materially improves the existing
  `SUM+MIN+MAX+AVG` benchmark and reaches near-native parity on a larger targeted
  3M-key grouped multi-aggregate query.
- Sparse workloads are still near parity and can flip between a small win and a
  small loss depending on the exact run.
- Composite-key and most variable-width key workloads remain near parity
  territory, and the planner now deliberately bails the weakest of those shapes
  to native plans. The new exception is the dense single-key `VARCHAR`
  `SUM/COUNT/AVG/MIN/MAX` hash path, which is now a real win on the direct
  benchmark, while higher-group-count boundary cases still fall back to native.
- Narrow grouped mixed probe/build aggregate sets can now also beat the
  disabled-extension baseline via a native preaggregation rewrite, but that
  path is still much narrower than the build-side-only lowerings and is
  currently limited to single-key balanced shapes.
- A later follow-up extended that same idea to one narrow composite-key grouped
  mixed shape, where it is also a real win over the disabled-extension baseline.
- The operator is still blocking, so it does not help latency-sensitive `LIMIT`
  queries.
