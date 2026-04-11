# AggJoin Shape-Comparison Studies

These files are historical **shape-comparison studies**, not same-query
extension-enabled vs disabled benchmarks.

They compare:

- a **probe-side aggregate shape** that is favorable to AGGJOIN
- a **build-side comparison shape** that intentionally bails to native DuckDB

Those queries are algebraically related and were useful for stress-testing the
executor and understanding direct/segmented-direct/hash regime changes, but they
often differ in `GROUP BY` key choice and output cardinality. They should not be
presented as plain "same query with aggjoin on/off" benchmark numbers.

The optimizer can do a limited planner-side probe/build swap for matched
AGGJOIN shapes when all `GROUP BY` columns are on one side. That does not make
these historical probe-side/build-side study files interchangeable with a true
same-query on/off methodology.

For true same-query benchmark results, see
[benchmarks/README.md](../benchmarks/README.md).

## Setup

```bash
cd duckdb_aggjoin
git clone --depth 1 --branch v1.5.1 https://github.com/duckdb/duckdb.git duckdb
make
```

All commands below assume `build/Release/duckdb` was built by this repo's
`make` target. That binary has `aggjoin` statically linked in, so these SQL
files do not need `LOAD aggjoin`.

## Running shape-comparison studies

```bash
build/Release/duckdb < shape_comparisons/core.sql
build/Release/duckdb < shape_comparisons/scaling.sql
build/Release/duckdb < shape_comparisons/asymmetric.sql

build/Release/duckdb < shape_comparisons/core_direct_100k.sql
build/Release/duckdb < shape_comparisons/core_direct_100k_probe_side.sql
build/Release/duckdb < shape_comparisons/core_direct_100k_build_side.sql

build/Release/duckdb < shape_comparisons/scaling_1k.sql
build/Release/duckdb < shape_comparisons/scaling_1k_probe_side.sql
build/Release/duckdb < shape_comparisons/scaling_1k_build_side.sql
```

On constrained hosts, use the timeout runner for slow build-side comparison
cases:

```bash
benchmarks/run_with_timeout.sh shape_comparisons/scaling_1k_build_side.sql 120
benchmarks/run_with_timeout.sh shape_comparisons/core_high_blowup_build_side.sql 300
```

## Methodology

- `core.sql`, `scaling.sql`, and `asymmetric.sql` benchmark the aggregate query
  directly via `COPY (...) TO ...`; they no longer wrap the query in
  `COUNT(*) FROM (...)`.
- `core_*.sql` and `scaling_*.sql` are exact per-case splits of the monolithic
  suites.
- `*_probe_side.sql` runs the AGGJOIN-friendly probe-side shape.
- `*_build_side.sql` runs the build-side comparison shape that intentionally
  stays native.
- These are stress tests for favorable vs unfavorable plan shapes. They are not
  same-query native baselines.

## Core snapshot

10M probe rows, single-key `SUM` unless noted otherwise.

These numbers are the latest local split-run snapshot on this host.

| # | Scenario | Probe-side shape | Build-side comparison shape | Probe-side / build-side ratio |
|---|----------|------------------|-----------------------------|-------------------------------|
| 1 | Direct mode, 100K keys | 0.246s | 13.750s | **55.9x** |
| 2 | Direct mode, 1M keys | 0.334s | 1.948s | **5.8x** |
| 3 | Hash mode, 3M keys | 0.412s | 1.076s | **2.6x** |
| 4 | Zipf-skewed, 100K keys | 0.170s | `>60s` timed out | **>=352x** |
| 5 | Sparse, 100K rows over 10M range | 0.025s | 0.025s | parity |
| 6 | High blowup, 10K keys (1000x) | 0.085s | 253.094s | **2977x** |
| 7 | Multi-agg `SUM+MIN+MAX+AVG`, 1M keys | 0.566s | 3.877s | **6.9x** |

## Scaling snapshot

10M probe rows, probe-side `GROUP BY`.

These numbers use the same shape-comparison methodology as the core suite.

| Keys | Mode | Probe-side shape | Build-side comparison shape | Probe-side / build-side ratio |
|------|------|------------------|-----------------------------|-------------------------------|
| 1K | Direct | 0.079s | `>45s` timed out | **>=569x** |
| 10K | Direct | 0.081s | `>45s` timed out | **>=556x** |
| 100K | Direct | 0.241s | 13.852s | **57.5x** |
| 500K | Direct | 0.264s | 3.148s | **11.9x** |
| 1M | Direct | 0.332s | 1.963s | **5.9x** |
| 2M | Direct boundary | 0.354s | 1.178s | **3.3x** |
| 3M | Segmented direct | 0.416s | 1.061s | **2.5x** |
| 5M | Segmented direct | 0.576s | 0.919s | **1.6x** |

These suites are still useful for:

- direct vs segmented-direct vs hash regime studies
- skew and blowup sensitivity
- understanding why probe-side aggregate ownership matters

They are not the right numbers to use for "same query, extension on vs off"
claims.
