# DuckDB AggJoin Extension

An experimental DuckDB optimizer extension for aggregate-over-join rewrites and
specialized AGGJOIN execution. It targets a narrow but important class of
queries where planner-side preaggregation or a fused aggregate-over-join path
can avoid paying for the full join result.

## Two Examples

The extension accelerates different query families through different
mechanisms. In some cases it emits the fused `PhysicalAggJoin` operator
directly; in others it keeps the plan in native DuckDB operators and applies a
planner-side rewrite.

### Example 1: dense grouped `VARCHAR` join with `PhysicalAggJoin`

A representative exact same-query fused-operator example is the dense grouped
single-key `VARCHAR` case with `COUNT(*)`:

```sql
select r_vdense_c.x, count(*) as c
from r_vdense_c join s_vdense_c on r_vdense_c.x = s_vdense_c.x
group by r_vdense_c.x;
```

For a denser local `100`-key / `100K`-row setup:
[`bench_varchar_dense.sql`](./benchmarks/bench_varchar_dense.sql):

```sql
create table r_vdense_c as
select 'k' || lpad(cast(i % 100 as varchar), 3, '0') as x
from generate_series(1, 100000) t(i);

create table s_vdense_c as
select 'k' || lpad(cast(i % 100 as varchar), 3, '0') as x
from generate_series(1, 100000) t(i);
```

the plan becomes:

```text
AGGJOIN
  SEQ_SCAN r_vdense_c
  SEQ_SCAN s_vdense_c
```

In this shape, the optimizer replaces the aggregate-over-join with the fused
operator. On the current local build, this exact same query gives:

| Current plan | Native baseline | Result |
|--------------|-----------------|--------|
| `0.008s` | `1.316s` | **164.5x faster** |

This example therefore illustrates a material `PhysicalAggJoin`-based
improvement with exact agreement against the native baseline.

### Example 2: `dblp` query with native rewrite acceleration

The exact `spark-eval` `dblp/path02.sql` query is:

```sql
select count(*)
from dblp p1, dblp p2, dblp p3
where p1.toNode = p2.fromNode
  and p2.toNode = p3.fromNode;
```

On the real `com-dblp.ungraph.txt` graph staged to Parquet, the extension does
**not** use `PhysicalAggJoin` here. Instead it rewrites the query into a native
mixed-side preaggregation plan. The rough `EXPLAIN` shape is:

```text
PROJECTION
  UNGROUPED_AGGREGATE  sum(#0)
    PROJECTION  "*"(#1, #3)
      HASH_JOIN
        HASH_GROUP_BY  count_star()
          HASH_JOIN
            SEQ_SCAN dblp
            SEQ_SCAN dblp
        HASH_GROUP_BY  count_star()
          SEQ_SCAN dblp
```

With the extension disabled, DuckDB stays on the more direct left-deep join:

```text
UNGROUPED_AGGREGATE  count_star()
  HASH_JOIN
    HASH_JOIN
      SEQ_SCAN dblp
      SEQ_SCAN dblp
    SEQ_SCAN dblp
```

Representative local results on the cached `dblp` Parquet graph:

| Query | Count | Current plan | Native baseline | Result |
|-------|-------|--------------|-----------------|--------|
| `dblp/path02.sql` | `67,520,431` | `0.104s` | `0.162s` | **1.6x faster** |
| `dblp/path03.sql` | `835,509,083` | `0.113s` | `1.473s` | **13.0x faster** |
| `dblp/path04.sql` | `12,025,691,265` | `0.243s` | `32.186s` | **132.5x faster** |
| `dblp/path05.sql` | `179,284,658,061` | `0.392s` | `120s+` | **>=306.1x faster** |

This example demonstrates extension-driven acceleration through a **planner
rewrite**, not through `PhysicalAggJoin`.

## Overview

The extension registers an **optimizer extension** that detects narrow
`Aggregate(Join)` patterns and rewrites them either to:

- a fused `PhysicalAggJoin` operator, or
- a native DuckDB preaggregation lowering that is cheaper than the original
  plan shape

It fires transparently on every query that matches its current envelopes; no
SQL changes are needed.

## Status

This project is best viewed as a **benchmark-backed research/engineering
prototype**, not as a general-purpose replacement for DuckDB planning.

What that means in practice:

- it is strong on specific query families that have been benchmarked and gated
  carefully
- it intentionally bails to native DuckDB for many nearby shapes where the win
  is weak, uncertain, or absent
- it contains both positive and negative results, and those negative results
  are part of the design story rather than something hidden

In its current form:

- **implemented today**: fused AGGJOIN execution for selected dense single-join
  cases, plus several native preaggregation rewrite families
- **intentionally narrow**: many variable-width, composite, asymmetric, and
  weak-latency cases stay native-first
- **appropriate for evaluation**: workloads with aggregate-over-join shapes
  similar to the benchmark matrix in
  [benchmarks/README.md](./benchmarks/README.md)

## What It Accelerates

The current implementation is strongest on:

- grouped and ungrouped aggregate-over-single-join shapes with `SUM/COUNT/AVG`
  and selected `MIN/MAX`
- build-heavy and mixed probe/build aggregate shapes that are better served by
  native preaggregation than by a fused executor
- narrow 3-way "final-bag" chains where preaggregating the tail of the join
  graph changes the cost materially
- dense single-key integer workloads, where the fused AGGJOIN operator can use
  direct indexing instead of a generic hash path

It is not trying to be universally best for all joins or all aggregates.

### When it fires

The optimizer matches plans of the form:

```
Aggregate(GROUP BY g, SUM/COUNT/MIN/MAX/AVG)   -- or ungrouped aggregate
  <- Projection* (zero or more)
    <- Join(equi-join)
```

And replaces with:

```
PhysicalAggJoin(build=inner, probe=outer)
  <- Scan(probe child)
  <- Scan(build child)
```

**Supported aggregates:** grouped and ungrouped `SUM`, `COUNT`, `MIN`, `MAX`, `AVG`
on the numeric path, with planner fallback to native DuckDB for weaker or
unsupported shapes.

**Supports both grouped and ungrouped aggregates** (`SELECT SUM(x) FROM r JOIN s ...` works).

**Bails out or prefers native for:**
- non-equi joins
- most variable-width key shapes and some wider composite-key plans that the
  current planner gate considers near-parity
- non-numeric build-side `MIN/MAX`
- many build-side aggregate mixes that fall outside the current direct fast path
- some type combinations such as `HUGEINT`/`DECIMAL` aggregate paths
- some DuckDB v1.5.1 `CompressedMaterialization` plans where the fused shape is
  not exposed cleanly to the optimizer

**Narrow variable-width support:** single-key `VARCHAR` grouped-by-join-key or
ungrouped numeric `SUM/COUNT/AVG/MIN/MAX` shapes can now use a narrow hash path
when the planner expects them to be dense enough to matter. The current grouped
path uses a build-slot hash fast path with direct string-key comparison and
now also uses a fused single-pass kernel for common all-`DOUBLE`
`SUM/COUNT/AVG/MIN/MAX` mixes. For the heavier grouped `MIN/MAX`-containing
mixes, finalize now also builds an exact `string_t -> build slot` lookup when
the grouped single-key `VARCHAR` shape stays small enough to fit the current
dense envelope (about `<=5K` build keys in the local sweep). A later
planner-side native build-preaggregation widening now covers more of the next
grouped single-key boundary too: on the local `100K`-row sweep, heavier grouped
`MIN/MAX`-containing mixes and lighter grouped `AVG` cases now rewrite cleanly
through the `10K` to `20K` band instead of riding the older near-parity
raw-string path, and the same rewrite still stays near native around `50K`
groups. The dense `<=5K` cases still stay on the narrow raw-string AGGJOIN
path.

### Performance snapshot

Current benchmark results are tracked in
[benchmarks/README.md](./benchmarks/README.md). On DuckDB `v1.5.1`, 10M probe
rows, the current build is strong on numeric single-key workloads. The dense
`VARCHAR`, build-heavy, and final-bag follow-up numbers in that README were
refreshed on April 10, 2026. The current local same-query snapshot was
collected on an AMD Ryzen 9 3900X host (12 cores / 24 threads) with 62 GiB RAM
running Ubuntu 22.04 / Linux 6.8:

| Scenario | Current plan | Same-query native baseline | Result |
|----------|--------------|----------------------------|--------|
| Grouped build-side `SUM+COUNT+AVG`, 100K keys / 1M rows | 0.160s | 0.293s | **1.8x faster** |
| Grouped probe `SUM` + build `SUM + DATE MIN+MAX`, 100K keys / 1M rows | 0.063s | 0.160s | **2.5x faster** |
| Final-bag grouped mixed chain, `100K x / 80K y`, `1M` rows each | 0.185s | 0.470s | **2.5x faster** |
| Final-bag ungrouped numeric chain, `100K x / 80K y`, `1M` rows each | 0.142s | 0.213s | **1.5x faster** |
| Dense `VARCHAR` grouped `SUM`, 1K keys / 100K rows | 0.009s | 0.183s | **20.3x faster** |
| Dense `VARCHAR` grouped `SUM+MIN+MAX`, 1K keys / 100K rows | 0.015s | 0.265s | **17.7x faster** |

Historical probe-side vs build-side stress studies were moved to
[shape_comparisons/README.md](./shape_comparisons/README.md). Those files are
still useful for studying favorable vs unfavorable plan shapes, but they are
not same-query extension-on vs extension-off baselines. The optimizer does have
a limited probe/build swap for matched AGGJOIN shapes when all `GROUP BY`
columns sit on one side, but that is just a normalization step inside the
matched rewrite envelope. It does not make the historical probe-side and
build-side study queries equivalent benchmark formulations, and many of those
studies still differ in grouping key choice and output cardinality.

For a public evaluation, the benchmark README is the main source of truth:

- benchmark files are split by family
- historical shape-comparison studies are separated out
- host specs for the published local numbers are recorded there

The public performance claims in this README use only true same-query
extension-on vs extension-off results.

Current limitations:
- sparse workloads are still only near parity
- composite-key and most variable-width key workloads are also near parity, and
  often intentionally left to native DuckDB by the planner gate
- build-heavy numeric cases are now split more deliberately:
  balanced grouped single-key `SUM/COUNT/AVG/MIN/MAX` and ungrouped
  `SUM/COUNT/AVG` shapes can rewrite to a fully native build-preaggregation
  plan and win clearly, while more asymmetric build-heavy shapes are still
  intentionally left to native DuckDB by the planner gate
- build-heavy non-numeric build-side `MIN/MAX` is now partially covered by the
  same native build-preaggregation lowering on narrow balanced grouped/ungrouped
  shapes, while broader build-side aggregate mixes still mostly fall back to native;
  a later follow-up also added a narrow composite-key grouped-by-subset case for
  build-side `AVG + DATE MIN/MAX`
- the same native lowering now also covers narrow mixed build-side numeric +
  non-numeric aggregate sets, such as grouped `SUM + VARCHAR MIN/MAX` and
  ungrouped `SUM + DATE MIN/MAX`
- a second narrow native lowering now also covers grouped and ungrouped mixed
  probe-side + build-side aggregate sets, for example probe `SUM` plus build
  `SUM + VARCHAR/DATE MIN/MAX`, and grouped probe/build `AVG` variants on the
  same narrow single-key balanced shape; a follow-up benchmark also confirmed
  that the same rewrite already covers probe-side `DATE/VARCHAR MIN/MAX` plus
  build-side `SUM` on that same narrow balanced single-key envelope; a later
  widening kept probe-heavy asymmetric mixed grouped shapes while still leaving
  build-heavy asymmetric shapes native-first
- that same mixed probe/build lowering now also has narrow grouped composite-key
  extensions for both full-key `GROUP BY` shapes and ordered subset-of-key
  grouped shapes, and now also covers the ungrouped balanced composite-key case;
  a follow-up benchmark also confirmed that the same composite family already
  covers probe-side `DATE/VARCHAR MIN/MAX` plus build-side `SUM`; the current
  asymmetric composite envelope also covers both richer mixed
  `AVG + AVG + DATE MIN/MAX`, probe-side `VARCHAR MIN/MAX` plus
  build-side `AVG + DATE MIN/MAX`, and probe-side nonnumeric `MIN/MAX` plus
  build-side `SUM`, including full-key, subset-key, and ungrouped composite
  envelopes; the adjacent single-key pure-nonnumeric case only clearly wins on
  the grouped probe-heavy side and otherwise stays near native parity
- the operator is still blocking, so it does not improve `LIMIT` latency

## Comparison to Yannakakis-Style Rewriting

For longer `dblp` paths, a manual frequency-propagation rewrite is often more
important than anything the extension can add afterward. The comparison below
uses a normalized `COUNT(*)` seed stage so the staged rewrite remains exact in
DuckDB. A representative stage looks like this:

```sql
with _yw_cte_1 as (
  select p6.fromnode, p6.tonode, count(*)::hugeint as _freq
  from dblp p6
  join dblp p7 on p6.tonode = p7.fromnode
  group by p6.fromnode, p6.tonode
),
_yw_cte_2 as (
  select p5.fromnode, p5.tonode, sum(_yw_cte_1_agg._freq) as _freq
  from dblp p5
  join (
    select fromnode, sum(_freq) as _freq
    from _yw_cte_1
    group by fromnode
  ) _yw_cte_1_agg on p5.tonode = _yw_cte_1_agg.fromnode
  group by p5.fromnode, p5.tonode
)
...
select cast(coalesce(sum(_freq), 0) as hugeint) as count_star
from _yw_cte_6;
```

On the raw path queries, the extension can still be very effective. But once
the query has already been rewritten into a staged grouped-summary pipeline,
DuckDB's native `HASH_GROUP_BY` + `HASH_JOIN` execution is already very
competitive:

| Query | Raw query current plan | Raw query native baseline | Manual frequency-propagation CTE |
|-------|------------------------|---------------------------|----------------------------------|
| `dblp/path01` | `0.032s` | `0.068s` | `0.026s` |
| `dblp/path02` | `0.095s` | `0.140s` | `0.131s` |
| `dblp/path03` | `0.113s` | `1.463s` | `0.162s` |
| `dblp/path04` | `0.257s` | `31.664s` | `0.212s` |
| `dblp/path05` | `0.376s` | `120s+` | `0.244s` |

This crossover is the important result:

- on shorter paths, the current raw-query optimization is already as good as or
  better than the manual rewrite
- by `path04`, the manual rewrite is slightly ahead of the current raw plan and
  far ahead of the native baseline
- by `path05`, the manual rewrite is clearly the best option among the three

The important boundary case is that the extension does **not** currently turn
these staged CTEs into `PhysicalAggJoin`. Some shorter stages can still pick
up native preaggregation rewrites, but the longer staged pipelines remain
native `HASH_GROUP_BY` + `HASH_JOIN` plans, and enabled/disabled runs on the
rewritten CTEs stay near parity. For example:

| Query | Rewritten CTE, extension enabled | Rewritten CTE, extension disabled |
|-------|----------------------------------|-----------------------------------|
| normalized `dblp/path06` CTE | `0.343s` | `0.302s` |
| normalized `dblp/path07` CTE | `0.457s` | `0.447s` |

So the staged rewrite itself is valuable, but matching the rewritten form is a
lower-priority optimization than deriving that rewrite from the raw query in
the first place.

## Design lessons

This repo is useful beyond the raw speedups because it documents which
optimization ideas actually held up.

Main lessons so far:

- **planner-side native lowerings paid out more reliably than deeper executor
  complexity**
- **the profitable region is often real but narrow**, so gating matters as much
  as implementation
- **many negative results were still valuable** because they showed where a
  seemingly plausible rewrite or runtime path flattened out
- **specialized aggregate-over-join optimization is not obviously too
  expensive**; the hard part is semantics and cost confidence, not raw planner
  overhead

That is why the codebase now includes both:

- benchmark-backed kept rewrite families
- documented reverted attempts in [CLAUDE.md](./CLAUDE.md)

### Planner tracing

For local debugging, the extension can explain why AGGJOIN fired or bailed:

```bash
AGGJOIN_TRACE=1 build/Release/duckdb
```

To also log runtime path and observed row/group counts:

```bash
AGGJOIN_TRACE=1 AGGJOIN_TRACE_STATS=1 build/Release/duckdb
```

## Building

### Prerequisites

DuckDB source is required (not included). Clone it into the `duckdb/` directory
(gitignored):

```bash
git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb
```

### Build

```bash
make                    # Release build (~5min first time, compiles DuckDB from source)
make debug              # Debug build
make test               # Build + run tests
make smoke              # Build + split sqllogictests + benchmark smoke harness
make clean              # Remove build artifacts
```

The smoke harness runs the split sqllogictest suite plus a few representative
benchmark cases with timeouts. It is meant to catch regressions in the main
execution modes cheaply, without rerunning the full benchmark matrix:

```bash
./scripts/run_regression_smoke.sh ./build/Release
```

The Makefile handles all cmake configuration including:
- **UPPERCASE cmake variables** required by DuckDB v1.5.1 (`DUCKDB_EXTENSION_AGGJOIN_PATH`)
- **Include path** (`src/include`) for the generated extension loader
- **Test path** for sqllogictest discovery

### Build gotchas

These issues were encountered during development and are now handled by the
Makefile, but be aware of them if building manually:

1. **Uppercase cmake variables**: DuckDB v1.5.1 requires `DUCKDB_EXTENSION_AGGJOIN_PATH`
   (not lowercase `aggjoin`). The extension build system uses `EXT_NAME_UPPERCASE`
   internally.

2. **Include path**: The generated extension loader (`generated_extension_headers.hpp`)
   includes `aggjoin_extension.hpp`. You must pass
   `-DDUCKDB_EXTENSION_AGGJOIN_INCLUDE_PATH=.../src/include` or the build fails with
   `fatal error: aggjoin_extension.hpp: No such file or directory`.

3. **Test path**: Tests are only discovered if `-DDUCKDB_EXTENSION_AGGJOIN_TEST_PATH`
   and `-DDUCKDB_EXTENSION_AGGJOIN_LOAD_TESTS=1` are set.

4. **CMakeLists.txt include path**: Uses `${CMAKE_CURRENT_SOURCE_DIR}/src/include`
   (not relative `src/include`) so it resolves correctly when built as a subdirectory
   of the DuckDB source tree.

### Manual cmake (if not using Makefile)

```bash
mkdir -p build/Release && cd build/Release
cmake -DCMAKE_BUILD_TYPE=Release \
      -DEXTENSION_STATIC_BUILD=1 \
      -DDUCKDB_EXTENSION_NAMES="aggjoin" \
      -DDUCKDB_EXTENSION_AGGJOIN_PATH="/absolute/path/to/duckdb_aggjoin" \
      -DDUCKDB_EXTENSION_AGGJOIN_INCLUDE_PATH="/absolute/path/to/duckdb_aggjoin/src/include" \
      -DDUCKDB_EXTENSION_AGGJOIN_TEST_PATH="/absolute/path/to/duckdb_aggjoin/test" \
      -DDUCKDB_EXTENSION_AGGJOIN_LOAD_TESTS=1 \
      -DDUCKDB_EXTENSION_AGGJOIN_SHOULD_LINK=1 \
      ../../duckdb
cmake --build . --config Release -j
```

### WASM (for DuckDB-WASM)

The extension is loaded dynamically in the browser via `LOAD aggjoin`. The build
produces a `.duckdb_extension.wasm` file that gets fetched via XMLHttpRequest at
runtime (no static linking needed).

**Quick build** (requires Emscripten 3.1.71 + DuckDB source):

```bash
make wasm                     # Build + patch metadata + deploy to frontend
make wasm-build-only          # Build only (no deploy)
```

**Setup** (one-time):

```bash
# 1. Install Emscripten 3.1.71 (MUST be this exact version)
cd /tmp && git clone https://github.com/emscripten-core/emsdk.git
cd emsdk && ./emsdk install 3.1.71 && ./emsdk activate 3.1.71
source emsdk_env.sh

# 2. Clone DuckDB source matching the browser ABI
cd duckdb_aggjoin
git clone --depth 1 --branch v1.4.3 https://github.com/duckdb/duckdb.git duckdb-wasm
```

The `make wasm` target runs `scripts/build_wasm.sh` which:
1. Links the extension into DuckDB's source tree
2. Configures cmake with `WASM_LOADABLE_EXTENSIONS=1 BUILD_EXTENSIONS_ONLY=1`
3. Builds with `emmake`
4. Patches the metadata footer (via `scripts/patch_metadata.py`)
5. Copies to `frontend/public/duckdb/extensions/v1.4.3/wasm_eh/`

The browser build now defaults to `duckdb-wasm/` and refuses to compile if the
checked-out DuckDB tag does not exactly match `DUCKDB_VERSION`. Metadata patching
alone does not make a `v1.5.1` binary loadable in a `v1.4.3` DuckDB-WASM runtime.

**Why Emscripten 3.1.71?** The `@duckdb/duckdb-wasm@1.32.0` npm package was built
with Emscripten 3.1.71, which legalizes i64 to i32 pairs. Newer versions use native
i64, causing ABI mismatch (`WebAssembly.instantiate` errors).

**Extension metadata footer**: The `.duckdb_extension.wasm` file needs a 512-byte
metadata footer that DuckDB parses on load. The `patch_metadata.py` script writes
the correct fields (magic, platform, version, ABI type) matching the official
`extensions.duckdb.org` format. Without this, you get `"Unknown ABI type"` errors.

**Manual patching** (if not using `make wasm`):

```bash
python3 scripts/patch_metadata.py aggjoin.duckdb_extension.wasm \
  --platform wasm_eh --version v1.4.3 --abi CPP
```

## Usage

Load the extension. The optimizer fires automatically:

```sql
INSTALL aggjoin;
LOAD aggjoin;

-- Grouped aggregate (fires AGGJOIN):
SELECT r.x, SUM(r.val)
FROM r JOIN s ON r.x = s.x
GROUP BY r.x;

-- Ungrouped aggregate (fires AGGJOIN):
SELECT SUM(r.val)
FROM r JOIN s ON r.x = s.x;

-- Multi-aggregate fusion (single pass):
SELECT r.x, SUM(r.val), MIN(r.val), MAX(r.val), AVG(r.val)
FROM r JOIN s ON r.x = s.x
GROUP BY r.x;

-- Within CTEs (frequency propagation pattern):
WITH cte_1 AS (
    SELECT s.y, CAST(COUNT(*) AS DOUBLE) AS _freq
    FROM r JOIN s ON r.x = s.x
    GROUP BY s.y
)
SELECT SUM(cte_1._freq) AS count_star
FROM cte_1 JOIN t ON cte_1.y = t.y;

-- 3-table joins (fuses outer Aggregate+Join):
SELECT r.x, SUM(r.val)
FROM r JOIN s ON r.x = s.x JOIN t ON s.y = t.y
GROUP BY r.x;
```

## Testing

```bash
make test
# Or manually:
./scripts/run_sqllogictests.sh
```

The sqllogictest suite is split across `test/sql/aggjoin_*.test` by theme
instead of one monolithic file. The current suite covers basic correctness,
collision/guard paths, single-key mixed shapes, composite shapes, and
VARCHAR-key paths.

For a cheap regression check:

```bash
make smoke
```

That runs the split sqllogictests plus a few representative benchmark smoke
cases under timeout.

## DuckDB version compatibility

| Feature | v1.4.3 | v1.5.1 |
|---------|--------|--------|
| Registration | `config.optimizer_extensions` | `ExtensionCallbackManager` |
| Virtual method | `GetData` | `GetDataInternal` |

Compile-time detection via `__has_include("duckdb/main/extension_callback_manager.hpp")`.
The `AGGJOIN_GETDATA` macro selects the correct method.

## Architecture

The extension is now split by layer rather than keeping optimizer, runtime, and
emit logic in one translation unit. See [ARCHITECTURE.md](./ARCHITECTURE.md)
for the full module map.

### Pipeline model

PhysicalAggJoin uses DuckDB's `CachingPhysicalOperator` + Sink pipeline:

1. **Sink (build)**: Scan build child, accumulate per-key frequency counts. Detect direct mode (dense integer keys, range < 1M).
2. **Execute (probe)**: For each probe chunk, look up build frequency, compute multiplicity (probe_freq × build_freq), accumulate aggregates per group.
3. **GetData (emit)**: Output final grouped results. AVG divides accumulated sum by count.

### Optimizations

- **Direct mode**: Flat array indexing for dense integer keys (signed and unsigned).
  Adaptive range limit (`min(10M, 2M/num_aggs)`) keeps working set in L3 cache.
- **Column-major layout**: `sums[agg * range + key]` with two-phase probe — extract
  keys into scratch buffer first, then per-aggregate contiguous array loops.
- **Multi-aggregate fusion**: Per-aggregate loops over contiguous arrays. Key extracted
  once per chunk. O(n) per aggregate, not O(n_agg × n).
- **Typed vector access**: All hot paths use `FlatVector::GetData<T>()` instead of
  `GetValue()` boxing. Covers build sink, hash-mode probe (SUM/MIN/MAX), and
  direct-mode fallback paths. GetValue only used for rare unsupported types.
- **Inline build aggregates**: `BuildAggValues` stored directly on `BuildEntry`,
  eliminating a separate `unordered_map` and its hash lookup during probe.
- **PK/FK fast path**: When all build keys have count=1 (`all_bc_one`), skips
  `bc_buf` allocation and multiply-by-1.0 operations.
- **Typed group storage**: Hash-mode group values stored as column-major `int64_t`/
  `double` arrays instead of `vector<Value>`. Eliminates Value boxing on both
  group init (probe) and group output (emit).
- **Ungrouped scalar path**: Ungrouped aggregates accumulate into running scalars
  during probe (O(1) emit) instead of per-key arrays with O(krange) reduction.
- **Compress-aware**: Handles DuckDB's `CompressedMaterialization` unsigned types
  (UINT8/16/32/64) in both build and probe key extraction.
- **`group_is_key`**: When GROUP BY column equals join key, reconstruct from array
  index (zero storage).
- **Cached build pointers**: Hash-mode probe caches `BuildEntry*` per row to avoid
  re-probing the build HT in Phase 3 build-side aggregate accumulation.
- **Open-addressing HT**: Power-of-2, 70% load, linear probing with `__builtin_prefetch`
  (hash mode fallback).
- **Native planner lowerings**: Narrow planner-side rewrites now cover several
  weak shapes better than direct AGGJOIN execution, including build-side,
  mixed probe/build, and grouped/ungrouped 3-way final-bag chains.

### What was tried

| Optimization | Result | Status |
|---|---|---|
| Column-major layout | -9% at 1M keys | **Kept** |
| Unsigned integer keys | >240x fix (was falling to hash mode) | **Kept** |
| Adaptive direct limit | Prevents 5M key regression | **Kept** |
| Remove GetValue() boxing | 2x faster at 1M direct, 24-40% hash | **Kept** |
| Inline BuildAggValues | 3.3x faster hash mode (eliminated double HT lookup) | **Kept** |
| PK/FK fast path (all_bc_one) | 3.9x faster for PK/FK joins | **Kept** |
| Typed group storage | 24-40% faster hash mode (no Value boxing) | **Kept** |
| Ungrouped scalar path | O(1) emit instead of O(krange) | **Kept** |
| Native final-bag preagg rewrite | 1.5-2.6x faster than native on grouped/ungrouped 3-way chains | **Kept** |
| Software prefetch (direct mode) | +8% slower (arrays in L3) | **Reverted** |
| Parallel probe (thread-local HTs) | Crashed (OperatorState lifetime) | **Reverted** |
| SIMD accumulation | Not attempted (memory-bound scatter) | **Skipped** |

### Pattern matching

`WalkAndReplace()` traverses the logical plan looking for `Aggregate(Projection*(Join))`:

1. `resolveJoinCol()` — traces column bindings through DuckDB's pruned join output
2. `TraceProjectionChain()` — maps indices through intermediate Projections (including compress)
3. `IsAggregate()` — validates function compatibility (SUM/COUNT/COUNT_STAR/MIN/MAX/AVG)
4. Can do a limited planner-side probe/build swap when all `GROUP BY` columns are
   on one side; this keeps matched AGGJOIN shapes in the operator's preferred
   orientation, but it is not a generic claim that probe-side and build-side
   study queries are interchangeable
5. `StripDecompressProjections()` — removes spurious decompress functions after replacement

### Known limitations

- **Hash mode performance cliff**: `FlatResultHT` is >100x slower than direct mode at
  >1M keys. Planned fix: replace with native `GroupedAggregateHashTable` (see
  `OPTIMIZATION_PLANS.md`).
- **Build-side aggregate inputs**: Operator can't access build-side values during
  streaming probe. Would require storing aggregate values in the build HT.
- **HUGEINT**: `SUM(integer)` returns HUGEINT. Use DOUBLE columns or explicit CAST.

## License

MIT
