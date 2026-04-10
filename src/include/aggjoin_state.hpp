#pragma once

#include "aggjoin_runtime.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include <unordered_map>

namespace duckdb {
struct AggJoinSinkState : public GlobalSinkState {
    OpenHT<BuildEntry> build_ht;
    unique_ptr<GroupedAggregateHashTable> agg_ht;
    FlatResultHT result_ht;
    bool use_native_ht = false;
    // Pre-allocated per-chunk buffers (avoid allocation in hot path)
    bool finalized = false;
    // Pre-allocated per-chunk scratch buffers (avoid per-chunk allocation)
    mutable unsafe_vector<double> probe_bc;       // build_count per row (hash mode)
    mutable unsafe_vector<idx_t> probe_slots;     // result HT slot per row (hash mode)
    mutable unsafe_vector<BuildEntry*> probe_build_ptrs; // cached build entry pointers (hash mode)
    mutable unsafe_vector<int64_t> prefilter_keys_buf; // typed key scratch for range prefilter
    mutable unsafe_vector<idx_t> direct_key_buf;  // key offsets per row (direct mode)
    mutable unsafe_vector<double> direct_bc_buf;  // build_count per row (direct mode, non-PK/FK)
    idx_t build_agg_slots = 0;                    // number of build-side aggregates

    // ── Build-side aggregate values for direct mode (transferred in Finalize) ──
    vector<double> direct_build_sums;    // [ba * range + key_offset]
    vector<double> direct_build_mins;
    vector<double> direct_build_maxs;
    vector<double> direct_build_counts;
    vector<uint8_t> direct_build_has;    // validity: was MIN/MAX initialized for this key?

    // ── Direct mode: flat arrays indexed by integer key ──
    // Eliminates ALL hash table lookups when keys are dense integers.
    bool direct_mode = false;
    int64_t key_min = 0;        // Min key value (offset for array indexing)
    idx_t key_range = 0;        // key_max - key_min + 1
    vector<idx_t> build_counts; // [key - key_min] → frequency count (0 = no match)
    vector<double> direct_sums; // [key_offset * num_aggs + agg] → accumulated sum
    vector<double> direct_counts; // [key_offset * num_aggs + agg] → count for AVG
    bool has_avg = false;         // Whether any aggregate is AVG
    // Direct mode MIN/MAX: flat arrays indexed by key offset
    vector<double> direct_mins; // [key_offset * num_aggs + agg]
    vector<double> direct_maxs;
    vector<uint8_t> direct_has; // [key_offset * num_aggs + agg] — whether min/max initialized
    bool has_min_max = false;   // Whether any aggregate is MIN or MAX
    idx_t num_aggs = 0;
    bool track_active_keys = false;          // Track matched key offsets for grouped direct emit
    vector<uint8_t> direct_key_seen;         // [key_offset] → whether key was seen on probe side
    vector<idx_t> direct_active_keys;        // matched key offsets for grouped direct emit
    bool segmented_direct_mode = false;      // Segment-backed direct path for simple grouped shapes
    bool segmented_multi_direct_mode = false; // Segment-backed fused path for narrow grouped multi-agg shapes
    bool build_slot_hash_mode = false;       // Non-integral/varlen GROUP BY join_key path keyed by build bucket slot
    idx_t segmented_shift = 16;              // segment size = 1 << shift
    idx_t segmented_size = 1 << 16;
    idx_t segmented_mask = (1 << 16) - 1;
    vector<unsafe_vector<uint32_t>> segmented_build_counts; // [segment][local_key]
    vector<unsafe_vector<double>> segmented_sums;           // [segment][local_key]
    vector<unsafe_vector<double>> segmented_avg_counts;     // [segment][local_key] for AVG
    vector<unsafe_vector<double>> segmented_multi_accums;   // [segment][local_key * accum_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_avg_counts; // [segment][local_key * avg_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_mins;     // [segment][local_key * min_slots + slot]
    vector<unsafe_vector<double>> segmented_multi_maxs;     // [segment][local_key * max_slots + slot]
    vector<unsafe_vector<uint8_t>> segmented_multi_min_has; // [segment][local_key * min_slots + slot]
    vector<unsafe_vector<uint8_t>> segmented_multi_max_has; // [segment][local_key * max_slots + slot]
    vector<idx_t> segmented_accum_index;                    // [agg] -> accum slot or INVALID_INDEX
    vector<idx_t> segmented_avg_index;                      // [agg] -> avg count slot or INVALID_INDEX
    vector<idx_t> segmented_min_index;                      // [agg] -> min slot or INVALID_INDEX
    vector<idx_t> segmented_max_index;                      // [agg] -> max slot or INVALID_INDEX
    idx_t segmented_accum_slots = 0;
    idx_t segmented_avg_slots = 0;
    idx_t segmented_min_slots = 0;
    idx_t segmented_max_slots = 0;
    vector<uint64_t> segmented_active_bits;                // 1 bit per key offset
    vector<idx_t> segmented_active_keys;                   // matched key offsets for emit
    // Group values stored as typed flat arrays (one per group column).
    // For the common case of GROUP BY key, group_is_key=true and values
    // are reconstructed from the array index (k + key_min) — no storage needed.
    bool group_is_key = false;  // True when single group col == probe key col
    vector<vector<int64_t>> direct_group_ints;   // [group_col][key_offset] for integer groups
    vector<vector<double>>  direct_group_doubles; // [group_col][key_offset] for double groups
    vector<vector<Value>>   direct_group_vals;   // [key_offset] → fallback for non-int/double groups
    vector<bool> direct_group_init;              // [key_offset] → whether group values stored
    bool all_bc_one = false;                     // True when all build counts == 1 (PK join)

    // ── Hash-mode prefilters: skip build HT probe for non-matching keys ──
    // 1. Min/max range check (single integer keys only)
    bool has_range_prefilter = false;
    int64_t build_key_min = 0;
    int64_t build_key_max = 0;
    // 2. Bloom filter over build hashes (works for all key types)
    vector<uint64_t> bloom_filter;  // bit array, size = bloom_bits / 64
    idx_t bloom_bits = 0;           // total bits (power of 2)
    idx_t bloom_mask = 0;           // bloom_bits - 1

    // ── Ungrouped direct mode: running scalar accumulators ──
    // Maintained during probe to avoid O(krange) emit loop.
    mutable vector<double> ungrouped_sum;   // [agg] running sum (SUM, COUNT, AVG numerator)
    mutable vector<double> ungrouped_count; // [agg] running count (AVG denominator)
    mutable vector<double> ungrouped_min;   // [agg] running MIN
    mutable vector<double> ungrouped_max;   // [agg] running MAX
    mutable vector<uint8_t> ungrouped_has;  // [agg] whether MIN/MAX initialized

    // ── Build-slot hash mode: accumulators keyed directly by build_ht bucket ──
    vector<double> build_slot_sums;
    vector<double> build_slot_counts;
    vector<double> build_slot_mins;
    vector<double> build_slot_maxs;
    vector<uint8_t> build_slot_has;
    vector<uint8_t> build_slot_seen;
    bool build_string_lookup_mode = false;
    unordered_map<string_t, idx_t> build_string_slots;

    // ── Optional runtime trace stats ──
    idx_t build_rows_seen = 0;
    idx_t build_rows_kept = 0;
    idx_t probe_rows_seen = 0;
    idx_t hash_match_rows = 0;
    idx_t range_prefilter_skips = 0;
    idx_t bloom_prefilter_skips = 0;
    bool stats_emitted = false;
};

struct AggJoinSourceState : public GlobalSourceState {
    vector<idx_t> slot_indices; // For HT mode
    vector<idx_t> direct_keys;  // For direct mode: key offsets with data
    idx_t pos = 0;
    bool initialized = false;
    AggregateHTScanState scan_state;
    idx_t MaxThreads() override { return 1; }
};

} // namespace duckdb
