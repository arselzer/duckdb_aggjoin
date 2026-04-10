# DuckDB AggJoin Extension — Development Guide

## Overview

Optimizer-only DuckDB extension. Automatically detects `Aggregate(Join)` patterns and
replaces with a fused `PhysicalAggJoin` operator. No table functions — pure optimizer.
Supports both grouped and ungrouped aggregates.

Planner and runtime tracing can be enabled without a debug rebuild:
- `AGGJOIN_TRACE=1`: planner fire/bail reasons
- `AGGJOIN_TRACE_STATS=1`: one-line runtime shape stats at source time
  (`path`, planner estimates, build/probe counts, prefilter skips, result groups)

Recent trace matrix findings:
- dataset-backed `dblp` follow-up benchmarks now cover the exact
  `spark-eval` `dblp/path02.sql`, `path03.sql`, and `path04.sql` queries over
  a cached Parquet edge list. On the real `com-dblp.ungraph.txt` graph, the
  current branch runs them at about `0.093s` vs `0.144s`, `0.113s` vs
  `1.473s`, and `0.243s` vs `32.186s` respectively
- the longer-hop `dblp` investigation exposed a real nested-join correctness
  bug: top-join key extraction was using raw `binding.column_index`, which is
  unsafe once a join child is itself a join subtree. Resolving join keys
  through child output bindings fixed both a tiny deterministic reproducer and
  the real `dblp` `path04` count mismatch
- the main numeric benchmark suites are now overwhelmingly exercising direct or
  segmented-direct paths, not the custom hash path
- a focused non-integral single-key benchmark (`bench_hash_nonintegral.sql`)
  was added to track the surviving non-integral single-key shape, but on the
  current build it now rewrites to a native build-preaggregation plan instead
  of exercising the older custom hash path
- on that current `DOUBLE`-key benchmark, the native build-preaggregation
  lowering beats the disabled-extension baseline (`0.062s` vs `0.128s`)
- the weak practical plans found so far are now mostly handled by narrow planner
  gates: low-fanout sparse shapes, non-integral single-key shapes, and large
  build-side-heavy aggregate mixes
- a later score-based planner attempt was discarded: with current estimate
  signals it regressed exactly the shapes the narrow gates already handle well,
  including sparse low-fanout, non-integral single-key, and build-side-heavy
  rollup cases
- a dedicated build-side benchmark family (`bench_build_side_suite.sql`) was
  added; after the native build-preaggregation rewrite, the balanced grouped
  and ungrouped numeric cases are now strong wins, while the asymmetric
  build-heavy case still stays near native because the planner bails it away
- the native build-preaggregation rewrite now also covers a narrow non-numeric
  build-side `MIN/MAX` class: balanced grouped `VARCHAR MIN/MAX` and ungrouped
  `DATE MIN/MAX` can now lower to fully native grouped-preagg subplans and beat
  the extension-disabled baseline on the focused follow-up benchmark
- a later follow-up extended the same build-preagg family to a narrow
  composite-key grouped-by-subset case for build-side `AVG + DATE MIN/MAX`;
  on a focused `1M`-row benchmark grouped by `k1` over join key `(k1,k2)` it
  improved from about `0.180s` native to `0.123s`
- the same native build-preaggregation lowering also covers narrow mixed
  build-side numeric + non-numeric aggregate sets, including grouped
  `SUM + VARCHAR MIN/MAX` and ungrouped `SUM + DATE MIN/MAX`
- a later symmetric native-preaggregation rewrite also covered a narrow grouped
  and ungrouped mixed probe-side + build-side class: grouped-by-join-key or
  ungrouped, single-key, balanced large inputs with supported
  `SUM/COUNT/AVG/MIN/MAX` payloads on both sides can now lower to two native
  per-side preaggs plus a join, which beat the disabled-extension baseline on
  the focused mixed probe/build benchmark; the current follow-up numbers are
  roughly `0.093s` vs `0.192s` for grouped probe `SUM` + build
  `SUM+VARCHAR MIN/MAX`, `0.047s` vs `0.138s` for the grouped `DATE` variant,
  `0.036s` vs `0.073s` for the ungrouped `DATE` variant, and `0.053s` vs
  `0.144s` for grouped probe/build `AVG + DATE MIN/MAX`
- a later follow-up confirmed that the same native mixed-side rewrite already
  covers probe-side nonnumeric `DATE/VARCHAR MIN/MAX` plus build-side `SUM` on
  the same narrow balanced single-key grouped and ungrouped shapes, at roughly
  `0.057s` vs `0.152s`, `0.046s` vs `0.143s`, and `0.029s` vs `0.052s`
- the same native mixed-side rewrite was then extended one notch to a narrow
  grouped composite-key class where `GROUP BY` matches the full join key; on a
  focused `1M`-row `AVG + AVG + DATE MIN/MAX` benchmark it improved from about
  `0.510s` native to `0.332s`
- a later widening kept narrow asymmetric composite mixed cases for
  `AVG + AVG + DATE MIN/MAX`; the current kept envelope now covers full-key,
  subset-key, and ungrouped composite shapes, with focused results around
  `0.067s` vs `1.529s`, `0.035s` vs `0.492s`, and `0.017s` vs `0.245s` on the
  small-probe / large-build side and similarly strong wins on the
  large-probe / small-build side
- a follow-up benchmark confirmed the same asymmetric composite mixed rewrite
  already covers a richer case where both sides contribute `AVG` plus
  nonnumeric `DATE MIN/MAX`; the kept envelope already includes full-key,
  subset-key, and ungrouped composite shapes, so no extra optimizer widening
  was needed there
- another follow-up confirmed the same asymmetric composite mixed rewrite also
  already covers probe-side `VARCHAR MIN/MAX` with build-side
  `AVG + DATE MIN/MAX`, again across full-key, subset-key, and ungrouped
  composite shapes without another optimizer change
- the analogous single-key pure nonnumeric family is asymmetric: grouped
  probe-heavy `VARCHAR MIN/MAX` + `DATE MIN/MAX` already wins strongly via the
  existing native mixed-side rewrite, but grouped build-heavy and both
  ungrouped variants stay near native parity, so there is no reason to widen
  that family further either
- the next adjacent asymmetric composite boundary did not hold: pure
  nonnumeric-on-both-sides composite shapes like probe `VARCHAR MIN/MAX` plus
  build `DATE MIN/MAX` stayed near native parity or slightly worse, so that
  family should remain native-first rather than widening the rewrite again
- a later follow-up extended that mixed-side family one notch further to a
  narrow grouped composite-key case where `GROUP BY` is an ordered subset of
  the join key rather than the full key; on a focused `1M`-row
  `SUM + SUM + DATE MIN/MAX` benchmark grouped by `k1` over join key `(k1,k2)`
  it improved from about `0.210s` native to `0.169s`
- the same mixed-side family now also covers the ungrouped balanced
  composite-key case; on a focused `1M`-row `AVG + AVG + DATE MIN/MAX`
  benchmark over join key `(k1,k2)` it improved from about `0.182s` native to
  `0.151s`
- a later follow-up confirmed that the same composite mixed-side family already
  covers probe-side nonnumeric `DATE/VARCHAR MIN/MAX` plus build-side `SUM` on
  the balanced composite-key full-key, subset-key, and ungrouped shapes; the
  focused benchmarks were extremely positive because the disabled-extension
  native baselines were very slow on those exact shapes
- a further widening kept narrow asymmetric composite cases for probe-side
  nonnumeric `DATE MIN/MAX` plus build-side `SUM`; the current kept envelope
  now covers full-key, subset-key, and ungrouped composite shapes, with focused
  results around `0.030s` vs `0.846s`, `0.038s` vs `0.409s`, and `0.015s` vs
  `0.231s` on the small-probe / large-build side and similarly strong wins on
  the large-probe / small-build side
- a later asymmetric follow-up showed the mixed-side rewrite should still stay
  native-first for build-heavy asymmetric single-key shapes (about `0.288s` vs
  `0.277s` on a `100K x 5M` `SUM + SUM + DATE MIN/MAX` case), but widening it
  for probe-heavy single-key grouped shapes was worthwhile (`0.102s` vs
  `0.286s` on the corresponding `5M x 100K` case)
- a direct-family build-side payload accumulation attempt was benchmarked and
  reverted; it only moved the older build-side path case from `0.549s` to
  `0.531s` and did not fix the broader build-side suite
- the kept native build-preaggregation rewrite wins by lowering narrow
  build-heavy shapes to a fully native plan (`HASH_GROUP_BY -> HASH_JOIN ->
  HASH_GROUP_BY`), not by forcing AGGJOIN back into the post-preagg plan
- a narrow single-key `VARCHAR` path now works for grouped-by-join-key and
  ungrouped numeric `SUM/COUNT/AVG/MIN/MAX` by stripping the top string-decompress
  projection and keeping raw strings inside AGGJOIN's hash path
- grouped dense single-key `VARCHAR` now uses the `build_slot_hash` fast path
  instead of the generic result hash table, and now stores exact single-key
  string payloads on build entries so the hot probe path can compare `string_t`
  directly instead of boxing through `Value`
- the grouped string-key path now also has a fused single-pass `build_slot_hash`
  kernel for common all-`DOUBLE` `SUM/COUNT/AVG/MIN/MAX` mixes, avoiding one
  full rescan of the matched probe rows per aggregate
- a later structural follow-up did hold up for the heavier grouped `VARCHAR`
  shapes: for grouped single-key `VARCHAR` mixes that include `MIN/MAX` and
  stay within about `5K` build keys, finalize now materializes an exact
  `string_t -> build slot` lookup table and probe reuses that slot directly
  instead of rediscovering the matching bucket through the generic string-key
  hash path
- the dense direct `VARCHAR` benchmark (`bench_varchar_dense.sql`) is a real win:
  grouped `SUM` at `1K` keys / `100K` rows is about `22x` faster than the
  same-binary native baseline, grouped `SUM+MIN+MAX` is now about `23x`
  faster, and ungrouped `SUM` is still several times faster too
- the new `VARCHAR` path should stay narrow: on a grouped `SUM+MIN+MAX` sweep
  over `100K` rows it is now clearly useful through about `5K` groups and
  remains near parity much farther out (`10K`, `20K`, even `50K`), rather than
  dropping sharply after the dense region
- a later `bench_varchar_variants.sql` follow-up showed that grouped `COUNT(*)`
  and `AVG` stay competitive farther out on the same path: `AVG` is still a
  clear win at `5K` groups and around parity at `10K`, with only a small loss
  by `20K+`; so the current grouped `VARCHAR` boundary is aggregate-dependent,
  not a single hard fanout cutoff
- two later `VARCHAR` follow-up runtime prototypes were benchmarked and
  reverted: one compared exact short strings via DuckDB-style packed 128-bit
  codes before falling back to `memcmp`, and the other hashed those packed
  codes directly for single-key `VARCHAR` joins; neither materially beat the
  current fused grouped kernel, and both were flat-to-worse on the `10K+`
  grouped or ungrouped boundary cases
- a later grouped-`VARCHAR` dense-id/interned-key prototype was also
  benchmarked and reverted: it remapped grouped single-key `VARCHAR` build
  buckets to contiguous dense ids and accumulated by dense id instead of sparse
  build bucket slot, but it was slower across the board (`1K` grouped `SUM`
  drifted from about `0.010s` to `0.012s`, `5K` grouped `SUM+MIN+MAX` from
  about `0.022s` to `0.024s`, and `10K` grouped `AVG` from about `0.047s` to
  `0.085s`)
- a later phase-1 structural follow-up also tried a custom open-addressed
  dense-id table plus compact per-id aggregate arrays for the same grouped
  single-key `VARCHAR` `MIN/MAX` envelope, but that also failed to beat the
  current committed semantic slot-lookup path: dense grouped `SUM+MIN+MAX`
  drifted from about `0.012s` to `0.016s`, the `5K` grouped `SUM+MIN+MAX`
  sweep from about `0.014s` to `0.023s`, while lighter grouped `AVG` at `10K`
  stayed roughly flat around parity; so the next real broader-varlen step is
  not another runtime-only dense-id table
- a later packed/compressed-key follow-up was also benchmarked and reverted:
  it reused DuckDB `compress_string` metadata to hash and compare grouped
  single-key `VARCHAR` joins on fixed-width packed values before falling back
  to raw strings, but it regressed the already-fast grouped path (`1K` grouped
  `SUM` rose from about `0.010s` to `0.021s`, `5K` grouped `SUM+MIN+MAX` from
  about `0.022s` to `0.023s`, and `10K` grouped `AVG` from about `0.047s` to
  `0.082s`)
- the practical implication is that the current committed grouped `VARCHAR`
  path has likely captured the easy runtime win outside this small denser
  `MIN/MAX` envelope; the next credible step is no longer another raw-string
  storage/hash micro-optimization, but either planner/runtime integration with
  internal `compress_string` projections or a true interned/compressed-key path
- a later upper-bound benchmark (`bench_varchar_surrogate_gap.sql`) compared
  the current grouped `VARCHAR` `SUM+MIN+MAX` path against the exact same
  workload on precomputed integer surrogate ids. That showed there is still
  meaningful headroom for a true semantic-id varlen project: `1K` keys was
  about `0.010s` on strings vs `0.006s` on integer ids, `5K` keys about
  `0.015s` vs `0.009s`, and the `10K` near-parity boundary about `0.056s` vs
  `0.021s`. So broader varlen support still looks worthwhile, but it will need
  planner-to-runtime semantic ids rather than another runtime-only lookup tweak
- a later explicit dictionary-lowering benchmark
  (`bench_varchar_dictionary_gap.sql`) initially suggested that broader varlen
  support might pay off around the old `10K` grouped boundary: building a
  dictionary and encoding both sides was slower than the current narrow
  `VARCHAR` path at `1K` keys (`0.025s` vs `0.010s`), slightly slower at `5K`
  (`0.026s` vs `0.023s`), and on the old branch looked better by `10K`.
  However, after widening the native-preagg band, that same benchmark no longer
  wins: `10K` grouped `SUM+MIN+MAX` is now about `0.019s` on the current path
  versus `0.029s` on the explicit dictionary lowering. So the earlier
  dictionary-gap signal was mostly exposing an estimator hole in the existing
  rewrite rather than proving that a new runtime dictionary path should replace
  the current grouped `VARCHAR` strategy.
- a later planner-side follow-up did hold up for exactly that wider grouped
  boundary: probe-side grouped single-key `VARCHAR` numeric aggs can now
  rewrite to the native build-preaggregation path in the current mid-fanout
  estimator band. On the real sweep workload it keeps the dense `1K` to `5K`
  cases on the older AGGJOIN path, rewrites the `10K` grouped
  `SUM+MIN+MAX` case from about `0.056s` to `0.019s`, rewrites grouped
  `MIN+MAX` at `10K` from about `0.059s` to `0.016s`, rewrites grouped `AVG`
  at `10K` from about `0.046s` to `0.016s`, and still lets `20K+` grouped
  shapes fall back to native through the existing variable-width gate
- a later broader-varlen investigation found that the remaining grouped
  `VARCHAR` gap was not a new semantic-id runtime path, but an estimator hole
  in that same native-preagg rewrite. The current local sweep now keeps the
  dense `1K` to `5K` cases on the raw-string AGGJOIN path, but rewrites the
  heavier grouped `SUM+MIN+MAX` cases through a much wider band:
  `10K` (`0.021s` vs `0.061s`), `20K` (`0.028s` vs `0.043s`), and `50K`
  (`0.039s` vs `0.032s`, near native). A targeted `12K` boundary check moved
  from about `0.058s` to `0.026s`, which confirmed that the old cutoff was
  simply missing a good existing rewrite. The lighter grouped `AVG` path stayed
  strong on the dense side through `5K` and still rewrites cleanly at `10K`
  (`0.016s` vs `0.052s`); a targeted `20K` `AVG` check also improved from
  about `0.039s` to `0.030s`. So the practical next step here is no longer
  “another small varlen runtime tweak”; it is a larger semantic-id project, if
  broader varlen coverage still matters.
- a later broader-varlen follow-up did *not* hold up beyond that narrow `10K`
  boundary once final string emit was included: explicit dictionary/id lowering
  was slightly worse than the current path for grouped `SUM+MIN+MAX` at `20K`
  (`0.049s` vs `0.045s`) and `50K` (`0.044s` vs `0.035s`), slightly worse for
  grouped `AVG` at `20K` (`0.046s` vs `0.038s`), and only marginally better for
  grouped `COUNT(*)` at `20K` (`0.029s` vs `0.037s`) where the current plan was
  already near the disabled-extension baseline (`0.034s`). So the present
  broader-varlen project does not have another good narrow patch right now.
- a later phase-0 larger-row semantic-id matrix
  (`bench_varchar_semantic_id_matrix.sql`) also failed to expose a real win
  region beyond the current split strategy. On the current branch, grouped
  `SUM+MIN+MAX` at `500K` rows / `10K` keys ran in about `0.032s` on the
  current plan versus `0.036s` on the explicit dictionary lowering and
  `0.247s` on same-binary native; at `1M` rows / `20K` keys the current plan
  was about `0.052s` versus `0.069s` on the explicit dictionary lowering and
  `0.304s` on native. Grouped `AVG` at `500K` rows / `10K` keys was about
  `0.016s` current versus `0.034s` dictionary and `0.178s` native; grouped
  `COUNT(*)` at `500K` rows / `20K` keys was about `0.024s` current versus
  `0.036s` dictionary and `0.136s` native. So after widening the grouped
  `VARCHAR` native-preagg band, the current branch no longer shows a clear
  semantic-id win region even on the larger-row matrix. That pushes broader
  semantic-id varlen work firmly into “larger redesign” territory rather than
  another justified narrow patch.
- the older quick bridge ideas were not enough by themselves: the real blocker
  was the `compress_string` / `decompress_string` projection chain, not just the
  planner gate
- a broader score-based planner replacement was also retried and reverted again;
  current estimate signals are still too weak to outperform the existing narrow
  planner gates on sparse, non-integral, and build-heavy weak shapes
- a simple mixed-mode admission experiment for probe-side non-numeric `MIN/MAX`
  was also reverted; it made AGGJOIN fire, but the current Value-heavy execution
  path was much slower than native on the workloads it targeted

Current practical conclusion:
- the current rule-based gates are better than a generic scorer given the
  planner-time signals available today
- the next worthwhile work is probably not "another planner heuristic"
- planner-side lowerings to fully native subplans can be better than trying to
  extend AGGJOIN's direct-family build-side execution further
- better runtime signals, targeted fallback cleanup tied to reproducible weak
  benchmarks, and richer plan validation are more credible next steps than more
  direct-path build-side tuning

## Building

### Prerequisites

Clone DuckDB source into `duckdb/` (gitignored):

```bash
git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb
```

Full build takes ~5 minutes (compiles DuckDB from C++ source). Incremental rebuilds
of just the extension are fast (~5s).

### Build commands

```bash
make                # Release build
make debug          # Debug build
make test           # Build + run tests
make clean          # Remove build artifacts
```

### Build gotchas (IMPORTANT)

These issues caused build failures during development. The Makefile handles all of
them, but if building manually you MUST address:

1. **UPPERCASE cmake variables**: DuckDB v1.5.1 requires `DUCKDB_EXTENSION_AGGJOIN_PATH`
   (not lowercase `aggjoin`). The Makefile computes this via `EXT_NAME_UPPER`.

2. **Include path**: Must pass `-DDUCKDB_EXTENSION_AGGJOIN_INCLUDE_PATH=.../src/include`.
   Without this, the generated extension loader fails:
   `fatal error: aggjoin_extension.hpp: No such file or directory`

3. **Test path**: Tests only discovered with both:
   `-DDUCKDB_EXTENSION_AGGJOIN_TEST_PATH=.../test`
   `-DDUCKDB_EXTENSION_AGGJOIN_LOAD_TESTS=1`

4. **CMakeLists.txt**: Uses `${CMAKE_CURRENT_SOURCE_DIR}/src/include` (not relative
   `src/include`) because it's built as a subdirectory of the DuckDB source tree.

5. **Test filter**: `make test` uses the full file path as filter, not `aggjoin/*`
   (tests register under `[sql]` group, not `[aggjoin]`).

6. **CRITICAL: Stale binaries**: `cmake --build` may silently skip recompilation if
   it thinks the source hasn't changed. ALWAYS run `touch src/aggjoin_optimizer.cpp`
   before building after code changes to force recompilation. This caused a 2-hour
   debugging session where "incorrect" results were actually from a stale binary.
   ```bash
   # After ANY source edit, ALWAYS do:
   touch src/aggjoin_optimizer.cpp
   cd build/Release && cmake --build . --config Release -j$(nproc)
   ```

### WASM (for DuckDB-WASM)

**Dynamic extension loading works** in DuckDB-WASM v1.4.3 via `LOAD aggjoin`.
The frontend sets `self.runtime.whereToLoad` on the worker to map extension
names to URLs. `LOAD` triggers `whereToLoad("aggjoin")` → XMLHttpRequest →
`dlopen`. Build with `-DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1`.

**Extension metadata footer**: The `.duckdb_extension.wasm` file must have a
512-byte footer matching the official format (8 × 32-byte fields in the first
256 bytes, reversed by the parser):
- Field 7 (offset 224): magic = "4" + 31 null bytes
- Field 6 (offset 192): platform = "wasm_eh"
- Field 5 (offset 160): duckdb_version = "v1.4.3"
- Field 4 (offset 128): extension_version = "v1.4.3"
- Field 3 (offset 96): ABI type = "CPP"
- Fields 0-2: unused (zeros)

Patch metadata after building:
```python
# Script to patch extension metadata in-place
with open('aggjoin.duckdb_extension.wasm', 'r+b') as f:
    data = bytearray(f.read())
    meta_start = len(data) - 512  # first 256 of last 512
    def w(off, val):
        b = val.encode() + b'\x00' * (32 - len(val))
        data[meta_start+off:meta_start+off+32] = b
    w(224, "4"); w(192, "wasm_eh"); w(160, "v1.4.3")
    w(128, "v1.4.3"); w(96, "CPP")
    f.seek(0); f.write(data)
```

See main repo `CLAUDE.md` section "4. DuckDB aggjoin extension (WASM)" for build steps.

Key cmake flags for WASM loadable extension:
```bash
-DWASM_LOADABLE_EXTENSIONS=1
-DBUILD_EXTENSIONS_ONLY=1
-DBUILD_EXTENSIONS="aggjoin"
```

**Approaches that DON'T work**:
- `INSTALL FROM` → no-op in WASM
- `registerFileBuffer()` + `LOAD 'path'` → WASM LOAD uses XMLHttpRequest, not virtual FS
- `custom_extension_repository` config → only affects INSTALL

**Emscripten**: Must use 3.1.71. Newer versions use native i64 (WASM ABI mismatch).

## Optimization roadmap

See `OPTIMIZATION_PLANS.md` for the prioritized plan:
- **Phase 1** (done): Fix rewrite coverage, add tests
- **Phase 2** (next): Remove GetValue boxing, eliminate double lookup, reuse scratch buffers
- **Phase 3**: PK/FK fast path, ungrouped direct mode, typed group storage

## Architecture

```
src/aggjoin_extension.cpp       Extension entry and test hooks
src/aggjoin_optimizer.cpp       Thin optimizer entry/registration
src/aggjoin_rewrites.cpp        Planner dispatch
src/aggjoin_rewrite_build.cpp   Native build-preagg rewrite family
src/aggjoin_rewrite_mixed.cpp   Native mixed-side rewrite family
src/aggjoin_sink.cpp            Build-side sink/finalize
src/aggjoin_source*.cpp         Probe-side execution families
src/aggjoin_emit*.cpp           Result emission families
test/sql/aggjoin_*.test         Split sqllogictest suite
benchmarks/                     Reproducible benchmark scripts + results
OPTIMIZATION_PLANS.md           Detailed plans for future optimizations
```

See `ARCHITECTURE.md` for the current module map and responsibilities.

### Key components in the runtime layer

| Component | Description |
|-----------|-------------|
| `OpenHT<E>` | Open-addressing hash table, 75% load, linear probing, prefetch |
| `FlatResultHT` | Hash mode result storage with sum_data, count_data (AVG), min/max |
| `PhysicalAggJoin` | `CachingPhysicalOperator` with Sink pipeline |
| `WalkAndReplace()` | Pattern matcher: finds `Aggregate(Projection*(Join))` |
| `TraceProjectionChain()` | Maps column indices through DuckDB's Projection layers |
| `resolveJoinCol()` | Resolves pruned Join output to scan column indices |
| `IsAggregate()` | Validates aggregate compatibility (SUM/COUNT/COUNT_STAR/MIN/MAX/AVG) |
| `FindCompressInChain()` | Detects `CompressedMaterialization` Projections |
| `StripDecompressProjections()` | Cleans up decompress functions after replacement |

### Pipeline model

1. **Sink (build)**: Inner child pushes chunks → per-key frequency counts.
   Detects direct mode (dense integer keys, adaptive range limit).
2. **Execute (probe)**: Outer chunks probe inner HT, compute multiplicity
   (`outer_freq × inner_freq`), accumulate aggregates per group.
3. **GetData (emit)**: Output final grouped results as DataChunks.
   AVG uses separate count tracking and divides sum/count during emit.

### Direct mode

When join key is a single integer column (signed or unsigned) with dense range:
- Adaptive limit: `min(10M, 2M / num_aggs)`, floor 500K keys
  (keeps working set in L3 cache; at 5M keys direct mode was 0.8x due to cache thrashing)
- Column-major accumulator layout: `sums[agg * range + key]`
  (each aggregate has contiguous array; 9% faster at 1M keys vs row-major)
- Two-phase probe: extract keys into scratch buffer first, then per-aggregate loops
- `group_is_key` optimization: reconstruct group values from array index
- NOT used for ungrouped aggregates (single result row, hash mode used instead)
- Handles unsigned types (UINT8/16/32/64) from CompressedMaterialization

### Hash mode (fallback)

When direct mode doesn't fire (non-integer keys, range > limit):
- `FlatResultHT` with open-addressing, sum/count/min/max per slot
- **Known performance issue**: >100x slower than direct mode at >1M keys
  (see OPTIMIZATION_PLANS.md for native GroupedAggregateHashTable replacement plan)
- Prefetching on build HT probe (4 rows ahead)

## When it fires / doesn't fire

**Fires on:**
- `SELECT r.x, SUM(r.val) FROM r JOIN s ON r.x = s.x GROUP BY r.x` (probe-side GROUP BY)
- `SELECT SUM(r.val) FROM r JOIN s ON r.x = s.x` (ungrouped)
- `SELECT r.x, COUNT(*) FROM r JOIN s ON r.x = s.x GROUP BY r.x` (COUNT_STAR)
- `SELECT r.x, AVG(r.val) ...` (correct: divides sum by count)
- `SELECT r.x, SUM(r.val), MIN(r.val), MAX(r.val) ...` (multi-aggregate fusion)
- Multi-key joins: `r.x = s.x AND r.y = s.y` (tested, works)
- 3-table joins: fuses outer Aggregate+Join, inner join stays native
- Within CTEs where `_freq` is `CAST(COUNT(*) AS DOUBLE)`
- Build-side aggregates: `SELECT s.y, SUM(r.val) GROUP BY s.y` (r on build side)
- Balanced build-heavy grouped numeric `SUM/COUNT/AVG/MIN/MAX` shapes, plus
  ungrouped `SUM/COUNT/AVG`, can rewrite to a fully native build-preaggregation
  plan instead of AGGJOIN

**Bails out for:**
- `SUM(integer)` → HUGEINT return type (can't store as double)
- most VARCHAR-key shapes outside the current narrow dense single-key
  numeric `SUM/COUNT/AVG` path
- VARCHAR MIN/MAX, DECIMAL aggregates
- Non-equi joins
- CAST in aggregate input: `SUM(CAST(r.val AS DOUBLE))` — column tracing bug
- some DuckDB v1.5.1 `CompressedMaterialization` plans where the required
  Projection chain is not exposed cleanly enough to rewrite safely

**Earlier repeated-execution crash**
This was an earlier issue and is not part of the current profile anymore. A
same-connection stress rerun on the current branch completed cleanly for both
repeated single-join AGGJOIN queries and repeated final-bag rewrite queries.

## DuckDB version compatibility

| Feature | v1.4.3 | v1.5.1 |
|---------|--------|--------|
| Registration | `config.optimizer_extensions` | `ExtensionCallbackManager` |
| Virtual method | `GetData` | `GetDataInternal` |
| Detection | `#if __has_include("duckdb/main/extension_callback_manager.hpp")` |

The `AGGJOIN_GETDATA` macro auto-selects the correct method.

## Testing

```bash
make test
```

Or manually after building:

```bash
./scripts/run_sqllogictests.sh
```

For a cheap regression sweep that also exercises a few representative benchmark
modes with timeouts:

```bash
make smoke
./scripts/run_regression_smoke.sh ./build/Release
```

The current split suite covers basic correctness, guards/collisions, single-key
mixed shapes, composite shapes, and VARCHAR-key coverage.

## Performance benchmarks

DuckDB v1.5.1, single-threaded AGGJOIN vs 4-thread native, 10M rows:

### Probe-side (scaling curve)

| Keys | AGGJOIN | Native | Speedup |
|------|---------|--------|---------|
| 1K | 158ms | 407ms | **2.6x** |
| 10K | 307ms | 569ms | **1.9x** |
| 100K | 748ms | 1005ms | **1.3x** |
| 500K | 1414ms | 1991ms | **1.4x** |
| 1M | 2071ms | 2955ms | **1.4x** |
| 2M | 3848ms | 5055ms | **1.3x** |

### Build-side (new)

| Query | AGGJOIN | Native | Speedup |
|-------|---------|--------|---------|
| Build-side SUM 100K keys | 1138ms | 7380ms | **6.5x** |
| Asymmetric 10M×100K | 100ms | 809ms | **8.1x** |

Native DuckDB shows zero parallel scaling for aggregate-over-join
(1T: 1393ms, 2T: 1407ms, 4T: 1557ms — gets slower with threads).

## Optimizations tried

### Implemented (kept)

| Optimization | Impact | Details |
|---|---|---|
| **Column-major layout** | -9% at 1M keys (na=1) | Transpose `sums[key*na+agg]` → `sums[agg*range+key]`. Per-aggregate contiguous arrays with two-phase probe (extract keys first, then per-agg loops). |
| **Unsigned integer keys** | >240x at 1.5M keys | Handle UINT8/16/32/64 from CompressedMaterialization. Was falling to hash mode (>180s → 0.75s). |
| **Adaptive direct limit** | Prevents 5M key regression | `min(10M, 2M/num_aggs)` keeps working set in L3 cache. 5M keys was 0.8x with flat 10M limit. |
| **AVG correctness** | Bug fix | Track per-group count separately, divide sum/count in emit. Was returning accumulated sum. |
| **COUNT_STAR support** | New pattern | Recognize `COUNT_STAR` function name, normalize to `COUNT`, handle BIGINT output. |
| **Ungrouped aggregates** | New pattern | Remove groups.empty() check, constant hash for single result slot, empty-result default row. |

### Tried and reverted

| Optimization | Result | Why |
|---|---|---|
| **Software prefetch (direct mode)** | **+8% slower** | Arrays fit in L3 at practical sizes. Prefetch overhead exceeds benefit. Hardware prefetcher already handles sequential input scan. |
| **Parallel probe (thread-local accumulators)** | Crashed | DuckDB `OperatorState` lifetime doesn't match `GlobalSinkState`. Per-thread state pointers become dangling between queries. Single-threaded AGGJOIN already beats 4-thread native by 1.5-1.9x via fusion. |
| **Native GroupedAggregateHashTable** | **Slower than FlatResultHT** | Implemented full integration (NativeAggSlot mapping, COUNT→SUM/AVG→2×SUM conversion, catalog function lookup). Correctness verified at small scale. At 2.5M keys: timeout >30s — per-row overhead from aggregate state management, function pointer calls, TupleDataLayout too high. At 2M keys: direct mode (965ms) ≈ native DuckDB (956ms), no gap to close. |

### Analyzed but not implemented

| Optimization | Expected impact | Why skipped |
|---|---|---|
| **SIMD accumulation** | ~2-3% | Memory-bound scatter-add doesn't benefit from SIMD. No AVX2 scatter. WASM only 2-wide. |
| **Build HT as flat array** | <5% | Sink is <10% of total time. Can't pre-allocate without knowing key range. |

## Correctness fixes applied

| Issue | Severity | Fix |
|---|---|---|
| **Hash-mode group resolution** | HIGH | Removed `BuildEntry.result_slot` caching. Now resolves result slot per-row from group hash. Was merging distinct groups when GROUP BY not functionally determined by join key. |
| **Null key filtering** | HIGH | Skip rows with NULL join keys in both build sink and probe. Inner join semantics: NULLs never match. |
| **Unsigned group capture** | MED | Direct mode group capture now handles UINT8/16/32/64 via `GetValue()` fallback. Was skipping unsigned types, leaving `direct_group_init` unset. |
| **Pre-allocated scratch buffers** | MED | Hash mode probe uses pre-allocated `probe_bc`/`probe_slots` on `AggJoinSinkState` instead of per-chunk `unsafe_vector` allocation. |

## Known limitations / future work

- **Blocking operator / poor `LIMIT` latency**: This is now the clearest
  remaining non-varlen limitation. `bench_latency.sql` still shows effectively
  full-work latency for `LIMIT 1`, so any real improvement here is a larger
  streaming/early-output project rather than another local planner tweak. A
  first ordered-input `LIMIT 1` prototype was reverted: explicit `ORDER BY`
  probe subqueries do survive under `AGGJOIN`, so early emit is feasible in
  principle, but the large ordered shape that mattered was already taking the
  native build-preagg rewrite and running around `0.07s` to `0.08s` versus
  native around `0.20s` to `0.22s`, so the narrow ordered fast path did not
  produce a better kept result.
- **Broader semantic-key varlen support**: The dense grouped single-key
  `VARCHAR` path and the narrow `10K` native-preagg boundary rewrite are both
  good, but broader grouped/ungrouped/composite varlen support still needs a
  true end-to-end semantic-id design. Runtime-only string map, packed-string,
  and explicit dictionary follow-ups are mostly flat or worse beyond the kept
  dense and `10K` envelopes.
- **Mixed-side native-preagg family is near its useful boundary**: Single-key,
  composite, subset-key, ungrouped, richer asymmetric, and pure-nonnumeric
  boundaries are now broadly mapped. Further widening should only happen if a
  new benchmark shows a clear gap.
- **Build-side AGGJOIN-internal nonnumeric support**: The old Value-heavy
  AGGJOIN path is still not worth reviving. Narrow balanced build-side and
  mixed-side nonnumeric cases are now better handled by native preaggregation
  rewrites instead.
- **Generic planner scorer**: Still not promising with today’s planner-time
  signals. Narrow gates plus selective native lowerings remain better than the
  broader scorer attempts tried so far.
- **WASM rebuild needed**: Deployed WASM (v1.4.3) still lacks the current local
  improvements.

## Possible further improvements

### High impact

1. **Streaming / partial-output AGGJOIN**
   The main remaining non-varlen opportunity is a non-blocking execution path.
   That would matter much more than another local throughput tweak on the
   current benchmark set. The current `PhysicalAggJoin` is a sink+source
   `CachingPhysicalOperator`, so DuckDB does not call `GetData` until the sink
   phase has completed. More importantly, exact early output is not generally
   possible for the current grouped shapes on an unsorted probe stream: to emit
   a final group early, the operator would need to know no later probe rows for
   that key still exist. So a real latency project likely needs either:
   - a different physical operator shape than the current sink/source model
   - or a narrower ordered-input/projected-key design where group completion can
     be proven before full probe consumption
   The first narrow ordered-input attempt confirmed that `ExecuteInternal`
   could in principle be the emission point, but it was not worth keeping:
   activation was tricky, and the main large ordered probe-side case was
   already well served by the existing native build-preagg rewrite.

2. **True semantic-id varlen path**
   If broader `VARCHAR` support is still a goal, the next real project is not
   another raw-string micro-optimization. It is a planner-to-runtime semantic-id
   design that keeps the current dense path for small grouped cases and uses ids
   only where the wider grouped boundary benefits.

3. **New lowering families only after manual-rewrite benchmarks**
   The recent wins all came from selective native lowerings. The right pattern
   is still: benchmark a manual lowering, keep it only if it clearly beats both
   the current plan and the disabled-extension baseline. The newest kept family
   is the narrow native final-bag rewrite: `bench_final_bag.sql` and
   `bench_final_bag_ungrouped.sql` now run the grouped mixed chain at about
   `0.185s` vs native `0.470s`, the grouped numeric chain at about `0.154s`
   vs native `0.400s`, the ungrouped mixed chain at about `0.156s` vs native
   `0.228s`, and the ungrouped numeric chain at about `0.142s` vs native
   `0.213s`, with planner trace confirming `native final-bag preagg`.
   A later follow-up added `bench_final_bag_asymmetric.sql` and tightened the
   ungrouped gate: final-bag still wins on grouped asymmetry and on the good
   ungrouped numeric shape, but ungrouped asymmetric cases now bail more
   aggressively when the matched head is too small for the downstream side.

### Lower priority

4. **Parallel execution**
   Still possible, but much less compelling than streaming or better varlen-key
   support. Current fused single-thread performance is already strong on the
   main workloads.

5. **WASM extension rebuild**
   Build process work rather than optimizer/runtime work.
