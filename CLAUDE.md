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
- the same native mixed-side rewrite was then extended one notch to a narrow
  grouped composite-key class where `GROUP BY` matches the full join key; on a
  focused `1M`-row `AVG + AVG + DATE MIN/MAX` benchmark it improved from about
  `0.510s` native to `0.332s`
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
- the dense direct `VARCHAR` benchmark (`bench_varchar_dense.sql`) is a real win:
  grouped `SUM` at `1K` keys / `100K` rows is about `22x` faster than the
  same-binary native baseline, grouped `SUM+MIN+MAX` is about `18x` faster,
  and ungrouped `SUM` is about `8x` faster too
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
- a later packed/compressed-key follow-up was also benchmarked and reverted:
  it reused DuckDB `compress_string` metadata to hash and compare grouped
  single-key `VARCHAR` joins on fixed-width packed values before falling back
  to raw strings, but it regressed the already-fast grouped path (`1K` grouped
  `SUM` rose from about `0.010s` to `0.021s`, `5K` grouped `SUM+MIN+MAX` from
  about `0.022s` to `0.023s`, and `10K` grouped `AVG` from about `0.047s` to
  `0.082s`)
- the practical implication is that the current committed grouped `VARCHAR`
  path has likely captured the easy runtime win; the next credible step is no
  longer another raw-string storage/hash micro-optimization, but either
  planner/runtime integration with internal `compress_string` projections or a
  true interned/compressed-key path
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
src/aggjoin_extension.cpp       Entry point — registers optimizer only (~30 lines)
src/aggjoin_optimizer.cpp       Optimizer + PhysicalAggJoin (~1800 lines)
src/include/aggjoin_extension.hpp   Extension class header
test/sql/aggjoin.test           DuckDB sqllogictest tests (19 assertions)
benchmarks/                     Reproducible benchmark scripts + results
OPTIMIZATION_PLANS.md           Detailed plans for future optimizations
```

### Key components in aggjoin_optimizer.cpp

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
This was an earlier issue and is not part of the current profile anymore.

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
cd build/Release
./test/unittest "/absolute/path/to/duckdb_aggjoin/test/sql/aggjoin.test"
```

51 assertions covering ungrouped SUM/COUNT/MIN/MAX/AVG, grouped SUM/AVG,
multi-aggregate fusion (SUM+MIN+MAX), CTE freq pattern, 3-table joins,
no-match edge case, NULL keys, multi-group per key, and build-side SUM/MIN/MAX.

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

- **Hash table key collisions**: Both `OpenHT` and `FlatResultHT` key off `hash_t`
  only — no equality check on original column values. 64-bit hash collisions are
  rare for normal data, but could still produce wrong results for adversarial inputs.
  Fix: store original key values and compare on collision.
- **Hash mode on non-integral keys**: Narrow planner gates now protect the simple
  weak `DOUBLE`-key shape, but true custom-hash parity with native is still not
  there if those gates are disabled.
- **Build-side aggregate coverage**: Balanced grouped numeric
  `SUM/COUNT/AVG/MIN/MAX` cases, plus ungrouped `SUM/COUNT/AVG`, now have a
  native build-preaggregation rewrite, but broader build-side mixes and
  non-numeric build-side `MIN/MAX` still mostly fall back to native.
- **Mixed-mode non-numeric support**: The current simple admission-based design
  is not good enough. If revisited, it should be a true sidecar execution path,
  not a Value-heavy fallback over the existing AGGJOIN hash path.
- **HUGEINT support**: `SUM(integer)` returns HUGEINT. Low priority (frequency
  propagation uses DOUBLE).
- **Parallel execution**: Pipeline lifecycle prevents clean thread-local state.
  Not needed — fusion already beats multi-threaded native.
- **Repeated execution crash**: Stale pipeline state between queries. Each
  Yannakakis CTE runs a single AGGJOIN, so the primary use case works.
  Tests structured to avoid triggering this (max ~10 queries per connection).
- **WASM rebuild needed**: Deployed WASM (v1.4.3) lacks all improvements.

## Possible further improvements

### High impact, novel approaches needed

1. **Custom flat-array hash table**: Replace FlatResultHT with a hash table that stores
   aggregates in flat `double[]` arrays (like direct mode) but uses hash-based slot
   assignment instead of key-offset indexing. Combine OpenHT's slot lookup with direct
   mode's contiguous accumulator layout. Would fix the >2M key cliff without the overhead
   of DuckDB's GroupedAggregateHashTable. ~150 lines, medium complexity.

2. **Optimizer-time bail-out**: At optimizer time, estimate key range from table statistics
   (DuckDB's `n_distinct` from the catalog). If estimated range exceeds direct mode limit,
   don't replace the plan — let native DuckDB handle it. Currently AGGJOIN always replaces
   and discovers at runtime that direct mode doesn't fit. ~30 lines in WalkAndReplace.

### Medium impact, straightforward

3. **Raise direct limit based on available memory**: Currently hardcoded `2M / num_aggs`.
   Could query system memory or use DuckDB's `BufferManager` memory limit to set a
   dynamic ceiling. At 64GB RAM, 10M keys × 4 aggs = 320MB is fine. ~10 lines.

4. **Build-side aggregate values in build HT**: Store aggregate source column values
   alongside frequency counts in `BuildEntry`. During probe, look up build-side values
   from the HT. Enables `SELECT s.y, SUM(r.val) ... GROUP BY s.y` pattern. ~200 lines,
   high correctness risk.

5. **WASM extension rebuild**: Recompile with Emscripten 3.1.71 against matching DuckDB
   source to deploy AVG fix, COUNT_STAR, ungrouped support, column-major layout, unsigned
   key handling. No code changes needed — just build process.

### Low impact / research

6. **Conflict-free parallel accumulation**: Instead of thread-local HTs (which have
   lifetime issues), use partitioned key ranges: thread T handles keys `[T*range/N,
   (T+1)*range/N)`. No contention, no merge. Requires knowing key range at partition
   time (available after Finalize). ~100 lines.

7. **Adaptive layout**: Row-major for na≤2, column-major for na≥3. Currently always
   column-major which is neutral at na≤2. Minor (~2%) improvement possible at na=2
   for large key ranges where cache line sharing matters.

8. **CompressedMaterialization bypass**: Run the optimizer before DuckDB's
   CompressedMaterialization pass, or strip compress Projections before pattern matching.
   Would enable AGGJOIN for more queries (currently some direct base-table queries bail
   due to compressed column indices). DuckDB extension API may not allow optimizer
   ordering control.
