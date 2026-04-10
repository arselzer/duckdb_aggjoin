# AggJoin Benchmarks

Benchmarks for `PhysicalAggJoin` versus native DuckDB plans on DuckDB `v1.5.1`.

The targeted follow-up tables below were refreshed locally on April 10, 2026,
after the native build-preaggregation rewrite was extended to cover narrow
balanced build-heavy nonnumeric, mixed build-side, and grouped mixed
probe/build aggregate sets, and after the narrow single-key `VARCHAR` path was
widened and optimized further. The long
`bench.sql` and `bench_scaling.sql` native baselines still do not make sense to
run monolithically on this host. The current core and scaling tables below were
therefore rebuilt from the split `bench_core_*` / `bench_scaling_*` files:
AGGJOIN sides were run serially, and slow native-only cases were bounded with
the timeout runner. Cases that did not finish inside the cap are marked
explicitly as timed out.

## Setup

```bash
cd duckdb_aggjoin
git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb
make
```

After source changes, `touch src/aggjoin_optimizer.cpp` before rebuilding to avoid
stale extension binaries.

## Benchmark host

Unless otherwise noted, the latest local split/timeout snapshot in this README
was taken on:

- CPU: AMD Ryzen 9 3900X (12 cores / 24 threads, boost up to 4.67 GHz)
- Memory: 62 GiB RAM
- OS: Ubuntu 22.04, Linux `6.8.0-52-generic`
- Scratch/output filesystem: `/tmp` on a local 1.8T volume with ~782G free at run time

## Running benchmarks

```bash
build/Release/duckdb < benchmarks/bench.sql
build/Release/duckdb < benchmarks/bench_core_direct_100k.sql
build/Release/duckdb < benchmarks/bench_core_direct_100k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_direct_100k_native.sql
build/Release/duckdb < benchmarks/bench_core_direct_1m.sql
build/Release/duckdb < benchmarks/bench_core_direct_1m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_direct_1m_native.sql
build/Release/duckdb < benchmarks/bench_core_hash_3m.sql
build/Release/duckdb < benchmarks/bench_core_hash_3m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_hash_3m_native.sql
build/Release/duckdb < benchmarks/bench_core_zipf_100k.sql
build/Release/duckdb < benchmarks/bench_core_zipf_100k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_zipf_100k_native.sql
build/Release/duckdb < benchmarks/bench_core_sparse.sql
build/Release/duckdb < benchmarks/bench_core_sparse_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_sparse_native.sql
build/Release/duckdb < benchmarks/bench_core_high_blowup.sql
build/Release/duckdb < benchmarks/bench_core_high_blowup_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_high_blowup_native.sql
build/Release/duckdb < benchmarks/bench_core_multiagg_1m.sql
build/Release/duckdb < benchmarks/bench_core_multiagg_1m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_core_multiagg_1m_native.sql
build/Release/duckdb < benchmarks/bench_scaling.sql
build/Release/duckdb < benchmarks/bench_scaling_1k.sql
build/Release/duckdb < benchmarks/bench_scaling_1k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_1k_native.sql
build/Release/duckdb < benchmarks/bench_scaling_10k.sql
build/Release/duckdb < benchmarks/bench_scaling_10k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_10k_native.sql
build/Release/duckdb < benchmarks/bench_scaling_100k.sql
build/Release/duckdb < benchmarks/bench_scaling_100k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_100k_native.sql
build/Release/duckdb < benchmarks/bench_scaling_500k.sql
build/Release/duckdb < benchmarks/bench_scaling_500k_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_500k_native.sql
build/Release/duckdb < benchmarks/bench_scaling_1m.sql
build/Release/duckdb < benchmarks/bench_scaling_1m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_1m_native.sql
build/Release/duckdb < benchmarks/bench_scaling_2m.sql
build/Release/duckdb < benchmarks/bench_scaling_2m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_2m_native.sql
build/Release/duckdb < benchmarks/bench_scaling_3m.sql
build/Release/duckdb < benchmarks/bench_scaling_3m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_3m_native.sql
build/Release/duckdb < benchmarks/bench_scaling_5m.sql
build/Release/duckdb < benchmarks/bench_scaling_5m_aggjoin.sql
build/Release/duckdb < benchmarks/bench_scaling_5m_native.sql
build/Release/duckdb < benchmarks/bench_asymmetric.sql
build/Release/duckdb < benchmarks/bench_cte_chain.sql
build/Release/duckdb < benchmarks/bench_final_bag.sql
build/Release/duckdb < benchmarks/bench_native_ht_composite.sql
build/Release/duckdb < benchmarks/bench_build_side_path.sql
build/Release/duckdb < benchmarks/bench_build_side_suite.sql
build/Release/duckdb < benchmarks/bench_build_side_nonnumeric.sql
build/Release/duckdb < benchmarks/bench_build_side_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_build_subset.sql
build/Release/duckdb < benchmarks/bench_probe_build_mixed.sql
build/Release/duckdb < benchmarks/bench_probe_nonnumeric_mixed.sql
build/Release/duckdb < benchmarks/bench_singlekey_pure_nonnumeric.sql
build/Release/duckdb < benchmarks/bench_composite_asymmetric_richer_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_asymmetric_probe_varchar_richer_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_asymmetric_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_probe_build_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_probe_nonnumeric_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_asymmetric_probe_nonnumeric_mixed.sql
build/Release/duckdb < benchmarks/bench_composite_subset_probe_build_mixed.sql
build/Release/duckdb < benchmarks/bench_ungrouped_composite_probe_build_mixed.sql
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

On constrained hosts, use the timeout wrapper for slow native-only cases:

```bash
benchmarks/run_with_timeout.sh benchmarks/bench_scaling_1k_native.sql 120
benchmarks/run_with_timeout.sh benchmarks/bench_core_high_blowup_native.sql 300
```

## Methodology

- `bench.sql`, `bench_scaling.sql`, and `bench_asymmetric.sql` benchmark the aggregate
  query directly via `COPY (...) TO ...`; they no longer wrap the query in
  `COUNT(*) FROM (...)`, which could benchmark the wrong plan shape.
- `bench_core_*.sql` and `bench_scaling_*.sql` are exact per-case splits of the
  long `bench.sql` and `bench_scaling.sql` suites. They are useful on hosts
  where the monolithic native halves are too slow to finish cleanly in one run
  or where the slowest native cases may be OOM-killed.
- `bench_core_*_aggjoin.sql` / `bench_core_*_native.sql` and
  `bench_scaling_*_aggjoin.sql` / `bench_scaling_*_native.sql` split those
  cases one step further so each side can be run independently with its own
  table setup and teardown.
- `benchmarks/run_with_timeout.sh` is the recommended runner for long native-only
  split cases on constrained hosts. It distinguishes clean completion, timeout
  (`124`), and likely OOM/resource kill (`137`) in a consistent way.
- The split SQL files still use fixed per-case `/tmp` output paths, so run a
  given split case serially rather than overlapping the same case in multiple
  processes. Otherwise DuckDB may fail fast with a file-lock error instead of a
  benchmark result.
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
- `bench_varchar_semantic_id_matrix.sql` is the larger-row phase-0 matrix for a
  future semantic-id grouped-`VARCHAR` project. It compares the current plan,
  same-binary native, and an explicit dictionary/id lowering on larger grouped
  `SUM+MIN+MAX`, `AVG`, and `COUNT(*)` shapes.
- `bench_varchar_keys.sql` still uses the older wrapper shape and is best treated as
  a smoke test, not a primary performance result.
- The latest local run used the standard current build with planner gating enabled.
  The targeted follow-up suites below completed cleanly. The long native halves
  of `bench.sql` and `bench_scaling.sql` were interrupted on this environment,
  so those two sections still show the previous full-run snapshot.

## Core suite

10M probe rows, single-key `SUM` unless noted otherwise.

These numbers are the latest local split-run snapshot on this host. AGGJOIN
cases were rerun serially via `bench_core_*_aggjoin.sql`; native cases were
rerun via `bench_core_*_native.sql`, with the timeout wrapper used for the
slowest stress cases.

| # | Scenario | AGGJOIN | Native | Speedup |
|---|----------|---------|--------|---------|
| 1 | Direct mode, 100K keys | 0.246s | 13.750s | **55.9x** |
| 2 | Direct mode, 1M keys | 0.334s | 1.948s | **5.8x** |
| 3 | Hash mode, 3M keys | 0.412s | 1.076s | **2.6x** |
| 4 | Zipf-skewed, 100K keys | 0.170s | `>60s` timed out | **>=352x** |
| 5 | Sparse, 100K rows over 10M range | 0.025s | 0.025s | parity |
| 6 | High blowup, 10K keys (1000x) | 0.085s | 253.094s | **2977x** |
| 7 | Multi-agg `SUM+MIN+MAX+AVG`, 1M keys | 0.566s | 3.877s | **6.9x** |

## Scaling curve

10M probe rows, probe-side `GROUP BY`.

This table is the latest local split-run snapshot on this host. AGGJOIN cases
were rerun serially via `bench_scaling_*_aggjoin.sql`; native cases were rerun
via `bench_scaling_*_native.sql` with a `45s` timeout cap. The smallest native
group-count cases are runtime-bound enough on this host that they are more
usefully reported as timed out than left in a monolithic hung run.

| Keys | Mode | AGGJOIN | Native | Speedup |
|------|------|---------|--------|---------|
| 1K | Direct | 0.079s | `>45s` timed out | **>=569x** |
| 10K | Direct | 0.081s | `>45s` timed out | **>=556x** |
| 100K | Direct | 0.241s | 13.852s | **57.5x** |
| 500K | Direct | 0.264s | 3.148s | **11.9x** |
| 1M | Direct | 0.332s | 1.963s | **5.9x** |
| 2M | Direct boundary | 0.354s | 1.178s | **3.3x** |
| 3M | Segmented direct | 0.416s | 1.061s | **2.5x** |
| 5M | Segmented direct | 0.576s | 0.919s | **1.6x** |

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

### Composite build-side subset-key follow-up

The native build-preaggregation rewrite now also covers a narrow composite-key
class where the query groups by an ordered subset of the join key rather than
the full key. The current kept case is still deliberately limited to balanced
inputs and build-side aggregate payloads only.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by subset of composite join key, build `AVG + DATE MIN+MAX`, `1K x 100` keys / `1M` rows | 0.123s | 0.180s | **1.5x faster** via native build preagg rewrite |

### Mixed probe/build follow-up

The native build-preaggregation lowering now also covers a narrow single-key
class where aggregate payloads appear on both sides of the join. The current
kept rewrite is still deliberately narrow: grouped by join key or ungrouped,
single join key, balanced large inputs, and supported `SUM/COUNT/AVG/MIN/MAX`
families.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, probe `SUM` + build `SUM + VARCHAR MIN+MAX`, 100K keys / 1M rows | 0.103s | 0.204s | **2.0x faster** via native mixed-side preagg rewrite |
| Grouped by join key, probe `SUM` + build `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.063s | 0.160s | **2.5x faster** via native mixed-side preagg rewrite |
| Ungrouped, probe `SUM` + build `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.036s | 0.056s | **1.6x faster** via native mixed-side preagg rewrite |
| Grouped by join key, probe `AVG` + build `AVG + DATE MIN+MAX`, 100K keys / 1M rows | 0.055s | 0.163s | **3.0x faster** via native mixed-side preagg rewrite |

Representative trace for the grouped mixed-side case:

- `planner rewrite: native mixed-side preagg`

### Probe-side nonnumeric mixed follow-up

The same mixed-side native-preaggregation rewrite already covers a narrow
single-key class where the probe side contributes nonnumeric `MIN/MAX` and the
build side contributes numeric aggregates. This follow-up just makes that
existing coverage explicit.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by join key, probe `DATE MIN+MAX` + build `SUM`, 100K keys / 1M rows | 0.057s | 0.152s | **2.7x faster** via native mixed-side preagg rewrite |
| Grouped by join key, probe `VARCHAR MIN+MAX` + build `SUM`, 100K keys / 1M rows | 0.046s | 0.143s | **3.1x faster** via native mixed-side preagg rewrite |
| Ungrouped, probe `DATE MIN+MAX` + build `SUM`, 100K keys / 1M rows | 0.029s | 0.052s | **1.8x faster** via native mixed-side preagg rewrite |

### Single-key pure nonnumeric follow-up

The next adjacent single-key boundary is pure nonnumeric on both sides. The
current behavior is asymmetric:

- grouped probe-heavy is a clear win through the existing native mixed-side rewrite
- grouped build-heavy and both ungrouped shapes are only near parity, so they
  should stay native-first rather than widening this family further

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped single key, probe `VARCHAR MIN+MAX` + build `DATE MIN+MAX`, `100K x 5M` | 0.525s | 0.503s | near parity |
| Large probe / small build, grouped single key, probe `VARCHAR MIN+MAX` + build `DATE MIN+MAX`, `5M x 100K` | 0.019s | 0.634s | **33.4x faster** via existing native mixed-side preagg rewrite |
| Small probe / large build, ungrouped single key, probe `VARCHAR MIN+MAX` + build `DATE MIN+MAX`, `100K x 5M` | 0.306s | 0.314s | near parity |
| Large probe / small build, ungrouped single key, probe `VARCHAR MIN+MAX` + build `DATE MIN+MAX`, `5M x 100K` | 0.356s | 0.340s | slightly slower |

### Composite mixed probe/build follow-up

The same native mixed-side preaggregation idea now also covers a narrow
grouped-by-join-key composite-key class. This is still intentionally limited:
two-sided aggregate payloads, grouped by the full composite join key, balanced
large inputs, and supported `SUM/COUNT/AVG/MIN/MAX` families.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `1K x 100` keys / `1M` rows | 0.357s | 0.537s | **1.5x faster** via native mixed-side preagg rewrite |

### Composite asymmetric mixed probe/build follow-up

The same mixed-side native-preaggregation family now also covers narrow
composite asymmetric shapes when both sides contribute supported payloads and
one side includes nonnumeric `MIN/MAX`. The kept envelope now covers:

- grouped by the full composite join key
- grouped by an ordered subset of a 2-key composite join
- ungrouped composite joins

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped by composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.067s | 1.529s | **22.8x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, grouped by composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.049s | 1.319s | **26.9x faster** via widened composite mixed-side preagg rewrite |
| Small probe / large build, grouped by subset of composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.035s | 0.492s | **14.1x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, grouped by subset of composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.019s | 0.499s | **26.3x faster** via widened composite mixed-side preagg rewrite |
| Small probe / large build, ungrouped composite join, probe `AVG` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.017s | 0.245s | **14.4x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, ungrouped composite join, probe `AVG` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.016s | 0.283s | **17.7x faster** via widened composite mixed-side preagg rewrite |

### Composite asymmetric richer mixed follow-up

The same native mixed-side rewrite already covers a slightly richer asymmetric
composite class where both sides contribute `AVG` plus nonnumeric `MIN/MAX`
payloads. The kept envelope now covers:

- grouped by the full composite join key
- grouped by an ordered subset of a 2-key composite join
- ungrouped composite joins

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped by composite join key, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.037s | 0.929s | **25.1x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, grouped by composite join key, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.017s | 0.922s | **54.2x faster** via existing native mixed-side preagg rewrite |
| Small probe / large build, grouped by subset of composite join key, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.040s | 0.620s | **15.5x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, grouped by subset of composite join key, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.018s | 0.591s | **32.8x faster** via existing native mixed-side preagg rewrite |
| Small probe / large build, ungrouped composite join, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.016s | 0.343s | **21.4x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, ungrouped composite join, probe `AVG + DATE MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.017s | 0.346s | **20.4x faster** via existing native mixed-side preagg rewrite |

### Composite asymmetric probe-varchar richer mixed follow-up

The same native mixed-side rewrite also already covers a neighboring asymmetric
composite class where the probe side contributes `VARCHAR MIN/MAX` and the
build side contributes `AVG + DATE MIN/MAX`.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped by composite join key, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.026s | 0.953s | **36.7x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, grouped by composite join key, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.028s | 1.051s | **37.5x faster** via existing native mixed-side preagg rewrite |
| Small probe / large build, grouped by subset of composite join key, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.047s | 0.593s | **12.6x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, grouped by subset of composite join key, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.027s | 0.732s | **27.1x faster** via existing native mixed-side preagg rewrite |
| Small probe / large build, ungrouped composite join, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `100K x 5M` | 0.017s | 0.345s | **20.3x faster** via existing native mixed-side preagg rewrite |
| Large probe / small build, ungrouped composite join, probe `VARCHAR MIN+MAX` + build `AVG + DATE MIN+MAX`, `5M x 100K` | 0.026s | 0.413s | **15.9x faster** via existing native mixed-side preagg rewrite |

### Composite probe-side nonnumeric mixed follow-up

The same mixed-side native-preaggregation rewrite already covers a narrow
composite-key class where the probe side contributes nonnumeric `MIN/MAX` and
the build side contributes numeric aggregates.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by composite join key, probe `DATE MIN+MAX` + build `SUM`, `1K x 100` keys / `1M` rows | 0.021s | 2.128s | **101x faster** via native mixed-side preagg rewrite |
| Grouped by subset of composite join key, probe `DATE MIN+MAX` + build `SUM`, `1K x 100` keys / `1M` rows | 0.017s | 1.288s | **75.8x faster** via native mixed-side preagg rewrite |
| Ungrouped composite join key, probe `DATE MIN+MAX` + build `SUM`, `1K x 100` keys / `1M` rows | 0.016s | 0.827s | **51.7x faster** via native mixed-side preagg rewrite |
| Grouped by composite join key, probe `VARCHAR MIN+MAX` + build `SUM`, `1K x 100` keys / `1M` rows | 0.019s | 3.839s | **202x faster** via native mixed-side preagg rewrite |

### Composite asymmetric probe-side nonnumeric mixed follow-up

The same composite mixed-side family now also widens to a narrow asymmetric
composite case when one side contributes nonnumeric `MIN/MAX` and the other
side contributes linear numeric aggregates. The kept envelope now covers:

- grouped by the full composite join key
- grouped by an ordered subset of a 2-key composite join
- ungrouped composite joins

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped by composite join key, probe `DATE MIN+MAX` + build `SUM`, `100K x 5M` | 0.030s | 0.846s | **28.2x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, grouped by composite join key, probe `DATE MIN+MAX` + build `SUM`, `5M x 100K` | 0.021s | 0.763s | **36.3x faster** via widened composite mixed-side preagg rewrite |
| Small probe / large build, grouped by subset of composite join key, probe `DATE MIN+MAX` + build `SUM`, `100K x 5M` | 0.038s | 0.409s | **10.8x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, grouped by subset of composite join key, probe `DATE MIN+MAX` + build `SUM`, `5M x 100K` | 0.020s | 0.406s | **20.3x faster** via widened composite mixed-side preagg rewrite |
| Small probe / large build, ungrouped composite join, probe `DATE MIN+MAX` + build `SUM`, `100K x 5M` | 0.015s | 0.231s | **15.4x faster** via widened composite mixed-side preagg rewrite |
| Large probe / small build, ungrouped composite join, probe `DATE MIN+MAX` + build `SUM`, `5M x 100K` | 0.015s | 0.226s | **15.1x faster** via widened composite mixed-side preagg rewrite |

### Composite subset-key mixed probe/build follow-up

The same native mixed-side preaggregation family now also covers a narrower
composite-key class where `GROUP BY` is an ordered subset of the join key
rather than the full join key. The current kept case is still deliberately
limited to balanced inputs and supported `SUM/COUNT/AVG/MIN/MAX` families.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Grouped by subset of composite join key, probe `SUM` + build `SUM + DATE MIN+MAX`, `1K x 100` keys / `1M` rows | 0.169s | 0.210s | **1.2x faster** via native mixed-side preagg rewrite |

### Ungrouped composite mixed probe/build follow-up

The same mixed-side native-preaggregation rewrite now also covers the ungrouped
composite-key case on balanced inputs.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Ungrouped composite join key, probe `AVG` + build `AVG + DATE MIN+MAX`, `1K x 100` keys / `1M` rows | 0.151s | 0.182s | **1.2x faster** via native mixed-side preagg rewrite |

### Asymmetric mixed probe/build follow-up

The mixed-side native-preaggregation rewrite is still not a good fit for
build-heavy asymmetric single-key shapes, but a later narrow widening showed
that probe-heavy grouped single-key mixed shapes can still benefit.

| Query shape | Direct query | Native baseline | Result |
|-------------|--------------|-----------------|--------|
| Small probe / large build, grouped `SUM + SUM + DATE MIN+MAX`, `100K x 5M` | 0.295s | 0.266s | near parity, still better left native-first |
| Large probe / small build, grouped `SUM + SUM + DATE MIN+MAX`, `5M x 100K` | 0.100s | 0.282s | **2.8x faster** via widened mixed-side preagg rewrite |

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

### Final-bag / multi-relation chain

`bench_final_bag.sql` and `bench_final_bag_ungrouped.sql` now exercise a narrow
native final-bag preaggregation rewrite for 3-way aggregate-over-join chains.
The kept envelope is:

- grouped single-key or ungrouped
- one top join child is itself a single-condition inner equi-join
- aggregates are split across the head relation and the tail relation
- current implementation targets the benchmarked numeric and mixed
  `SUM/COUNT/AVG/MIN/MAX` shapes
- ungrouped asymmetric cases are now gated more tightly; the rewrite stays
  native-first when the matched head is much smaller than the downstream side

Current local results:

| Query shape | Rewritten direct query | Native baseline | Manual decomposition | Result |
|-------------|------------------------|-----------------|----------------------|--------|
| Grouped mixed chain, `SUM(r) + SUM(t) + MIN/MAX(date)`, `100K x / 80K y`, `1M` rows each | 0.185s | 0.470s | 0.265s | **2.5x faster** than native, **1.4x faster** than manual |
| Grouped numeric chain, `SUM(r) + SUM(t) + AVG(t)`, `100K x / 80K y`, `1M` rows each | 0.154s | 0.400s | 0.164s | **2.6x faster** than native, slightly faster than manual |
| Ungrouped mixed chain, `SUM(r) + SUM(t) + MIN/MAX(date)`, `100K x / 80K y`, `1M` rows each | 0.156s | 0.228s | 0.242s | **1.5x faster** than native, **1.6x faster** than manual |
| Ungrouped numeric chain, `SUM(r) + SUM(t) + AVG(t)`, `100K x / 80K y`, `1M` rows each | 0.142s | 0.213s | 0.133s | **1.5x faster** than native, near manual |

Trace confirmation on the benchmarked shapes:

- `planner rewrite: native final-bag preagg (groups=1, aggs=4, ...)`
- `planner rewrite: native final-bag preagg (groups=1, aggs=3, ...)`
- `planner rewrite: native final-bag preagg (groups=0, aggs=4, ...)`
- `planner rewrite: native final-bag preagg (groups=0, aggs=3, ...)`

`bench_final_bag_asymmetric.sql` is the guardrail benchmark for that gate.
Current local results:

| Query shape | Rewritten direct query | Native baseline | Manual decomposition | Result |
|-------------|------------------------|-----------------|----------------------|--------|
| Grouped mixed, small head / large bridge+tail | 0.888s | 1.674s | 1.388s | **1.9x faster** than native |
| Grouped mixed, large head / small bridge+tail | 0.095s | 0.097s | 0.086s | near parity |
| Ungrouped numeric, small head / large bridge+tail | 0.110s | 0.435s | 0.630s | **3.9x faster** than native after tighter gate |
| Ungrouped numeric, large head / small bridge+tail | 0.019s | 0.026s | 0.070s | stays near native-first |

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
| Dense `VARCHAR` grouped `SUM`, 1K keys / 100K rows | 0.009s | 0.183s | **20.3x faster** |
| Dense `VARCHAR` grouped `SUM+MIN+MAX`, 1K keys / 100K rows | 0.015s | 0.265s | **17.7x faster** |
| Dense `VARCHAR` ungrouped `SUM`, 1K keys / 100K rows | 0.009s | 0.045s | **5.0x faster** |
| Boundary grouped `MIN+MAX`, 10K keys / 100K rows | 0.016s | 0.059s | **3.7x faster**, rewrites to native build-preagg |

Representative trace/stats for this benchmark:

- grouped dense case: `planner fired`, `path=build_slot_hash`, `build_ht_count=1000`, `result_groups=1000`
- ungrouped case: `planner fired`, `path=hash`, `build_ht_count=1000`, `result_groups=1`
- 10K-key boundary case: `planner rewrite: native build preagg`

The latest grouped-path speedup comes from two runtime changes:
- storing exact single-key `VARCHAR` payloads directly on build entries and
  comparing `string_t` keys in the hot probe path instead of boxing through `Value`
- a fused single-pass `build_slot_hash` kernel for all-`DOUBLE`
  `SUM/COUNT/AVG/MIN/MAX` mixes, so grouped string-key queries no longer rescan
  the same matched rows once per aggregate

The latest follow-up keeps one more narrow runtime optimization:
- for grouped single-key `VARCHAR` shapes with `MIN/MAX` in the aggregate mix
  and at most about `5K` build keys, finalize now materializes an exact
  `string_t -> build slot` lookup table, so probe no longer has to rediscover
  the matching build bucket through the generic string-key hash path

`bench_varchar_sweep.sql` is the grouped boundary sweep for the same narrow path:

| Keys | Direct query | Native baseline | Planner outcome |
|------|--------------|-----------------|-----------------|
| 1K | 0.011s | 0.278s | fires AGGJOIN |
| 2K | 0.012s | 0.159s | fires AGGJOIN |
| 5K | 0.022s | 0.095s | fires AGGJOIN |
| 10K | 0.021s | 0.061s | rewrites to native build-preagg |
| 20K | 0.028s | 0.043s | rewrites to native build-preagg |
| 50K | 0.039s | 0.032s | rewrites to native build-preagg |

On this `100K`-row setup, the current planner boundary is doing the right
practical thing for the heavier grouped `SUM+MIN+MAX` mix: the dense raw-string
AGGJOIN path is clearly useful through about `5K` groups, and a later
planner-side native build-preaggregation widening now takes over cleanly from
`10K` through at least `20K`, with the same rewrite still staying near native
at `50K`. The fused grouped kernel plus the exact `string_t -> build slot`
lookup are what pushed the dense `1K` to `5K` cases down materially; the wider
`10K` to `50K` behavior now comes from the planner rewrite, not from another
runtime-only string tweak.

`bench_varchar_variants.sql` fills in the lighter aggregate mixes on the same
single-key `VARCHAR` path:

| Query shape | Direct query | Native baseline | Planner outcome |
|-------------|--------------|-----------------|-----------------|
| Grouped `COUNT(*)`, 1K keys / 100K rows | 0.013s | 0.184s | fires AGGJOIN |
| Grouped `AVG`, 1K keys / 100K rows | 0.013s | 0.188s | fires AGGJOIN |
| Grouped `AVG`, 5K keys / 100K rows | 0.018s | 0.070s | fires AGGJOIN |
| Grouped `AVG`, 10K keys / 100K rows | 0.016s | 0.052s | rewrites to native build-preagg |

So the current `VARCHAR` boundary is aggregate-dependent:
- grouped `SUM+MIN+MAX` stays on the dense AGGJOIN path through `5K`, then
  benefits from the widened `10K+` native build-preagg rewrite
- grouped `COUNT(*)` stays on the dense AGGJOIN path in the rerun here
- grouped `AVG` is still strong on the dense path through `5K` and now also
  benefits from the same native build-preagg rewrite at `10K`

Two later `VARCHAR` runtime follow-ups were benchmarked after the results above
and then reverted:
- exact packed short-string compare using DuckDB-style 128-bit string packing
- packed short-string hashing for single-key `VARCHAR` joins

Neither materially beat the current committed fused grouped kernel. The dense
`1K` grouped cases were mostly flat, while the `10K+` grouped and ungrouped
boundary cases were flat to slightly worse. So the current practical conclusion
is that the committed narrow grouped `VARCHAR` path had largely captured the
easy runtime win. A later structural follow-up that *did* hold up was much more
targeted: on grouped single-key `VARCHAR` shapes with `MIN/MAX` in the mix and
roughly `<=5K` build keys, exact `string_t -> build slot` lookup improved the
heavier grouped sweep materially without broadening the planner gate. The next
credible step in this area would still need to be more structural than another
raw-string micro-tweak: either planner/runtime integration with internal
`compress_string` projections or a true interned/compressed-key path.

A later planner-side follow-up now keeps a broader grouped boundary win:
probe-side grouped single-key `VARCHAR` numeric aggs can rewrite to the native
build-preaggregation path when the local estimator lands in the current
mid/high-fanout band. On the real sweep workload that keeps the dense `1K` to
`5K` cases on AGGJOIN, rewrites the heavier grouped `SUM+MIN+MAX` cases at
`10K`, `20K`, and `50K` to native build-preaggregation (`0.021s` vs `0.061s`,
`0.028s` vs `0.043s`, `0.039s` vs `0.032s`) and still keeps lighter grouped
`AVG` strong on the dense path through `5K` (`0.018s` vs `0.070s`) before the
same rewrite takes over at `10K` (`0.016s` vs `0.052s`). A targeted boundary
check also moved the old `12K` grouped `SUM+MIN+MAX` gap from about `0.058s`
to `0.026s`.

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
