# AggJoin Architecture

This extension is organized around four layers:

1. extension registration
2. logical/planner analysis and rewrites
3. physical sink/source execution
4. shared runtime/state utilities

## Module map

### Extension entry

- `src/aggjoin_extension.cpp`
- `src/include/aggjoin_extension.hpp`

Registers the optimizer extension and test-only hooks.

### Optimizer entry

- `src/aggjoin_optimizer.cpp`

Owns optimizer registration, env/test knobs, and the thin entry point into the rewrite pipeline.

### Logical planning and rewrites

- `src/aggjoin_logical.cpp`
- `src/aggjoin_rewrites.cpp`
- `src/aggjoin_rewrite_build.cpp`
- `src/aggjoin_rewrite_mixed.cpp`
- `src/include/aggjoin_rewrites_internal.hpp`

Responsibilities:

- pattern matching on aggregate-over-join shapes
- cost/gating logic
- native build-preagg rewrites
- native mixed-side rewrites
- construction of the logical extension operator

### Physical operator shell

- `src/aggjoin_physical.cpp`
- `src/include/aggjoin_physical.hpp`

Owns the `PhysicalAggJoin` declaration, constructor, operator-state factory, pipeline wiring, and the physical-plan factory.

### Sink side

- `src/aggjoin_sink.cpp`

Responsibilities:

- build-side hash table population
- build-side aggregate accumulation
- fast-path mode selection and initialization
- bloom/range/build-slot/string lookup setup

### Source side

- `src/aggjoin_source.cpp`
- `src/aggjoin_source_segmented.cpp`
- `src/aggjoin_source_direct.cpp`
- `src/aggjoin_source_result_hash.cpp`
- `src/include/aggjoin_source_internal.hpp`

Responsibilities:

- source-side dispatch
- segmented direct execution
- flat direct execution
- generic result-hash/native fallback execution

### Emit side

- `src/aggjoin_emit.cpp`
- `src/aggjoin_emit_direct.cpp`
- `src/include/aggjoin_emit_internal.hpp`

Responsibilities:

- final result production
- direct/segmented/build-slot emission
- native aggregate hash table scan emission
- flat result-hash emission

### Shared runtime and state

- `src/include/aggjoin_optimizer_shared.hpp`
- `src/include/aggjoin_runtime.hpp`
- `src/include/aggjoin_state.hpp`

Responsibilities:

- shared planner/runtime metadata
- build/result hash table implementations
- key payload handling and equality
- compression metadata helpers
- sink/source state structures

## Execution model

`PhysicalAggJoin` is a DuckDB `CachingPhysicalOperator` with a sink/source lifecycle:

1. Sink:
   - consume build rows
   - populate `build_ht`
   - optionally precompute build-side aggregates
   - choose execution mode

2. ExecuteInternal:
   - consume probe chunks
   - choose one of the execution families:
     - segmented direct
     - flat direct
     - build-slot hash
     - generic result-hash
     - native aggregate HT

3. GetData:
   - emit grouped or ungrouped results from the selected execution state

## Rewrite families

The optimizer currently has two major native-lowering families:

- build-side native preaggregation
- mixed-side native preaggregation

These are kept separate because they have different planner envelopes and benchmarks.

## Fast-path families

The runtime currently has four meaningful fast-path families:

- segmented direct
- flat direct
- build-slot hash
- generic result-hash/native fallback

They are intentionally split by mode because they have different data layout assumptions and different maintenance risk.

## Testing layout

The sqllogictest suite lives in `test/sql/aggjoin_*.test` and is run by:

- `make test`
- `scripts/run_sqllogictests.sh`

Themes are split by behavior family instead of keeping one monolithic test file.

## Current design intent

- keep rewrite families benchmark-driven and narrow
- keep execution families separated by data-layout assumptions
- prefer explicit module boundaries over generic abstraction in hot loops
- treat broader varlen support and latency/parallelism work as larger projects, not small local tweaks
