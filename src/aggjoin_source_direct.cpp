#include "aggjoin_source_internal.hpp"

namespace duckdb {

static bool HasDirectBuildAggStorage(const AggJoinSinkState &sink) {
    return sink.build_agg_slots > 0 &&
           (!sink.direct_build_sums.empty() || !sink.direct_build_counts.empty() ||
            !sink.direct_build_mins.empty() || !sink.direct_build_maxs.empty());
}

static const double *DirectBuildDoublePtr(const vector<double> &values, const vector<idx_t> &indexes,
                                          idx_t build_agg_index, idx_t range) {
    if (build_agg_index >= indexes.size()) {
        return nullptr;
    }
    auto slot = indexes[build_agg_index];
    return slot == DConstants::INVALID_INDEX ? nullptr : values.data() + slot * range;
}

static const uint8_t *DirectBuildHasPtr(const AggJoinSinkState &sink, idx_t build_agg_index, idx_t range) {
    if (build_agg_index >= sink.direct_build_has_index.size()) {
        return nullptr;
    }
    auto slot = sink.direct_build_has_index[build_agg_index];
    return slot == DConstants::INVALID_INDEX ? nullptr : sink.direct_build_has.data() + slot * range;
}

static double *DirectDoublePtr(vector<double> &values, const vector<idx_t> &indexes, idx_t agg_index, idx_t range) {
    if (agg_index >= indexes.size()) {
        return nullptr;
    }
    auto slot = indexes[agg_index];
    return slot == DConstants::INVALID_INDEX ? nullptr : values.data() + slot * range;
}

static uint8_t *DirectHasPtr(vector<uint8_t> &values, const vector<idx_t> &indexes, idx_t agg_index, idx_t range) {
    if (agg_index >= indexes.size()) {
        return nullptr;
    }
    auto slot = indexes[agg_index];
    return slot == DConstants::INVALID_INDEX ? nullptr : values.data() + slot * range;
}

bool TryExecutePlannedDirectParallelSourcePath(const PhysicalAggJoin &op, DataChunk &input, DataChunk &chunk,
                                               AggJoinSinkState &sink, AggJoinOperatorState &state,
                                               idx_t n, idx_t na) {
    auto &col = op.col;
    bool ungrouped = col.group_cols.empty();
    bool grouped_by_key = col.group_cols.size() == 1 && col.group_cols[0] == col.probe_key_cols[0];
    if (!sink.direct_build_without_ht || (!ungrouped && !grouped_by_key)) {
        return false;
    }
    for (idx_t a = 0; a < na; a++) {
        bool on_build = col.agg_on_build.size() > a && col.agg_on_build[a];
        if (on_build && !HasDirectBuildAggStorage(sink)) {
            return false;
        }
        auto &fn = col.agg_funcs[a];
        auto ai = col.agg_input_cols[a];
        if (on_build) {
            if (fn == "COUNT" || fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") {
                continue;
            }
            return false;
        }
        if (fn == "COUNT") {
            if (ai != DConstants::INVALID_INDEX && ai >= input.ColumnCount()) {
                return false;
            }
            if (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) {
                input.data[ai].Flatten(n);
            }
            continue;
        }
        if (fn != "SUM" && fn != "AVG" && fn != "MIN" && fn != "MAX") {
            return false;
        }
        if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
            return false;
        }
        auto payload_type = input.data[ai].GetType().InternalType();
        if (payload_type != PhysicalType::DOUBLE && payload_type != PhysicalType::FLOAT) {
            return false;
        }
        input.data[ai].Flatten(n);
    }

    if (!state.parallel_direct_initialized) {
        state.parallel_direct_active = true;
        state.parallel_direct_initialized = true;
        state.parallel_direct_grouped = grouped_by_key;
        if (grouped_by_key) {
            auto krange = sink.key_range;
            idx_t bytes_per_key = sizeof(double) * sink.direct_accum_slots +
                                  sizeof(double) * sink.direct_avg_slots +
                                  sizeof(double) * (sink.direct_min_slots + sink.direct_max_slots) +
                                  sizeof(uint8_t) * sink.direct_has_slots;
            __int128 local_bytes = (__int128)krange * (__int128)bytes_per_key;
            state.parallel_direct_sparse_grouped = local_bytes > (__int128)16 * 1024 * 1024;
            if (state.parallel_direct_sparse_grouped) {
                auto reserve_keys = std::min<idx_t>(krange, n);
                state.parallel_direct_sparse_slot_lookup.assign(krange, 0);
                state.parallel_direct_sparse_keys.reserve(reserve_keys);
                state.parallel_sparse_sums.reserve(reserve_keys * sink.direct_accum_slots);
                if (sink.direct_avg_slots) {
                    state.parallel_sparse_counts.reserve(reserve_keys * sink.direct_avg_slots);
                }
                if (sink.direct_min_slots) {
                    state.parallel_sparse_mins.reserve(reserve_keys * sink.direct_min_slots);
                }
                if (sink.direct_max_slots) {
                    state.parallel_sparse_maxs.reserve(reserve_keys * sink.direct_max_slots);
                }
                if (sink.direct_has_slots) {
                    state.parallel_sparse_has.reserve(reserve_keys * sink.direct_has_slots);
                }
            } else {
                state.parallel_direct_sums.assign(sink.direct_accum_slots * krange, 0.0);
                if (sink.direct_avg_slots) {
                    state.parallel_direct_counts.assign(sink.direct_avg_slots * krange, 0.0);
                }
                if (sink.direct_min_slots) {
                    state.parallel_direct_mins.assign(sink.direct_min_slots * krange, std::numeric_limits<double>::max());
                }
                if (sink.direct_max_slots) {
                    state.parallel_direct_maxs.assign(sink.direct_max_slots * krange, std::numeric_limits<double>::lowest());
                }
                if (sink.direct_has_slots) {
                    state.parallel_direct_has.assign(sink.direct_has_slots * krange, 0);
                }
                state.parallel_direct_key_seen.assign(krange, 0);
                state.parallel_direct_active_keys.reserve(std::min<idx_t>(krange, n));
            }
        } else {
            state.parallel_ungrouped_sum.assign(na, 0.0);
            state.parallel_ungrouped_count.assign(na, 0.0);
            state.parallel_ungrouped_min.assign(na, std::numeric_limits<double>::max());
            state.parallel_ungrouped_max.assign(na, std::numeric_limits<double>::lowest());
            state.parallel_ungrouped_has.assign(na, 0);
        }
    }

    struct AggSlot {
        enum Kind {
            SUM_VAL,
            AVG_VAL,
            COUNT_STAR,
            COUNT_COL,
            MIN_VAL,
            MAX_VAL,
            BUILD_SUM,
            BUILD_AVG,
            BUILD_COUNT,
            BUILD_MIN,
            BUILD_MAX
        } kind;
        const double *double_vals = nullptr;
        const float *float_vals = nullptr;
        const uint64_t *validity = nullptr;
        const double *build_sums = nullptr;
        const double *build_counts = nullptr;
        const double *build_mins = nullptr;
        const double *build_maxs = nullptr;
        const uint8_t *build_has = nullptr;
        idx_t accum_idx = DConstants::INVALID_INDEX;
        idx_t avg_idx = DConstants::INVALID_INDEX;
        idx_t min_idx = DConstants::INVALID_INDEX;
        idx_t max_idx = DConstants::INVALID_INDEX;
        idx_t has_idx = DConstants::INVALID_INDEX;

        double Value(idx_t row) const {
            return double_vals ? double_vals[row] : (double)float_vals[row];
        }
    };
    vector<AggSlot> slots(na);
    idx_t build_agg_index = 0;
    for (idx_t a = 0; a < na; a++) {
        auto &fn = col.agg_funcs[a];
        auto ai = col.agg_input_cols[a];
        bool on_build = col.agg_on_build.size() > a && col.agg_on_build[a];
        slots[a].accum_idx = sink.direct_accum_index.size() > a ? sink.direct_accum_index[a] : DConstants::INVALID_INDEX;
        slots[a].avg_idx = sink.direct_avg_index.size() > a ? sink.direct_avg_index[a] : DConstants::INVALID_INDEX;
        slots[a].min_idx = sink.direct_min_index.size() > a ? sink.direct_min_index[a] : DConstants::INVALID_INDEX;
        slots[a].max_idx = sink.direct_max_index.size() > a ? sink.direct_max_index[a] : DConstants::INVALID_INDEX;
        slots[a].has_idx = sink.direct_has_index.size() > a ? sink.direct_has_index[a] : DConstants::INVALID_INDEX;
        if (on_build) {
            auto ba = build_agg_index++;
            slots[a].build_sums = DirectBuildDoublePtr(sink.direct_build_sums, sink.direct_build_sum_index, ba, sink.key_range);
            slots[a].build_counts = DirectBuildDoublePtr(sink.direct_build_counts, sink.direct_build_count_index, ba, sink.key_range);
            slots[a].build_mins = DirectBuildDoublePtr(sink.direct_build_mins, sink.direct_build_min_index, ba, sink.key_range);
            slots[a].build_maxs = DirectBuildDoublePtr(sink.direct_build_maxs, sink.direct_build_max_index, ba, sink.key_range);
            slots[a].build_has = DirectBuildHasPtr(sink, ba, sink.key_range);
            if (fn == "SUM") {
                slots[a].kind = AggSlot::BUILD_SUM;
                if (!slots[a].build_sums || !slots[a].build_has) return false;
            } else if (fn == "AVG") {
                slots[a].kind = AggSlot::BUILD_AVG;
                if (!slots[a].build_sums || !slots[a].build_counts) return false;
            } else if (fn == "COUNT") {
                slots[a].kind = AggSlot::BUILD_COUNT;
                if (!slots[a].build_counts) return false;
            } else if (fn == "MIN") {
                slots[a].kind = AggSlot::BUILD_MIN;
                if (!slots[a].build_mins || !slots[a].build_has) return false;
            } else {
                slots[a].kind = AggSlot::BUILD_MAX;
                if (!slots[a].build_maxs || !slots[a].build_has) return false;
            }
        } else if (fn == "COUNT" && ai == DConstants::INVALID_INDEX) {
            slots[a].kind = AggSlot::COUNT_STAR;
        } else if (fn == "COUNT") {
            slots[a].kind = AggSlot::COUNT_COL;
            slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else {
            auto payload_type = input.data[ai].GetType().InternalType();
            if (payload_type == PhysicalType::DOUBLE) {
                slots[a].double_vals = FlatVector::GetData<double>(input.data[ai]);
            } else {
                slots[a].float_vals = FlatVector::GetData<float>(input.data[ai]);
            }
            slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
            if (fn == "SUM") {
                slots[a].kind = AggSlot::SUM_VAL;
            } else if (fn == "AVG") {
                slots[a].kind = AggSlot::AVG_VAL;
            } else if (fn == "MIN") {
                slots[a].kind = AggSlot::MIN_VAL;
            } else {
                slots[a].kind = AggSlot::MAX_VAL;
            }
        }
    }

    auto pki = col.probe_key_cols[0];
    input.data[pki].Flatten(n);
    auto ptype = input.data[pki].GetType().InternalType();
    auto *key_validity = FlatVector::Validity(input.data[pki]).GetData();
    auto kmin = sink.key_min;
    auto krange = sink.key_range;
    auto *bc = sink.build_counts.data();

    auto ensure_sparse_slot = [&](idx_t k) -> idx_t {
        auto marker = state.parallel_direct_sparse_slot_lookup[k];
        if (marker != 0) {
            return (idx_t)(marker - 1);
        }
        auto slot = (idx_t)state.parallel_direct_sparse_keys.size();
        state.parallel_direct_sparse_slot_lookup[k] = (uint32_t)(slot + 1);
        state.parallel_direct_sparse_keys.push_back(k);
        auto active_count = slot + 1;
        if (sink.direct_accum_slots) {
            state.parallel_sparse_sums.resize(active_count * sink.direct_accum_slots, 0.0);
        }
        if (sink.direct_avg_slots) {
            state.parallel_sparse_counts.resize(active_count * sink.direct_avg_slots, 0.0);
        }
        if (sink.direct_min_slots) {
            state.parallel_sparse_mins.resize(active_count * sink.direct_min_slots, std::numeric_limits<double>::max());
        }
        if (sink.direct_max_slots) {
            state.parallel_sparse_maxs.resize(active_count * sink.direct_max_slots, std::numeric_limits<double>::lowest());
        }
        if (sink.direct_has_slots) {
            state.parallel_sparse_has.resize(active_count * sink.direct_has_slots, 0);
        }
        return slot;
    };

#define AGGJOIN_PARALLEL_DIRECT_UNGROUPED(KTYPE)                                                         \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || bc[k] == 0) continue;                                                     \
            auto bcount = (double)bc[k];                                                                 \
            for (idx_t a = 0; a < na; a++) {                                                             \
                auto &slot = slots[a];                                                                   \
                auto *validity = slot.validity;                                                          \
                switch (slot.kind) {                                                                     \
                case AggSlot::COUNT_STAR:                                                                \
                    state.parallel_ungrouped_sum[a] += bcount;                                           \
                    break;                                                                               \
                case AggSlot::COUNT_COL:                                                                 \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1))                               \
                        state.parallel_ungrouped_sum[a] += bcount;                                       \
                    break;                                                                               \
                case AggSlot::SUM_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        state.parallel_ungrouped_sum[a] += slot.Value(r) * bcount;                        \
                        state.parallel_ungrouped_has[a] = 1;                                             \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::AVG_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        state.parallel_ungrouped_sum[a] += slot.Value(r) * bcount;                        \
                        state.parallel_ungrouped_count[a] += bcount;                                     \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MIN_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        if (!state.parallel_ungrouped_has[a] || v < state.parallel_ungrouped_min[a]) {   \
                            state.parallel_ungrouped_min[a] = v;                                         \
                            state.parallel_ungrouped_has[a] = 1;                                         \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MAX_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        if (!state.parallel_ungrouped_has[a] || v > state.parallel_ungrouped_max[a]) {   \
                            state.parallel_ungrouped_max[a] = v;                                         \
                            state.parallel_ungrouped_has[a] = 1;                                         \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::BUILD_SUM: {                                                               \
                    state.parallel_ungrouped_sum[a] += slot.build_sums[k];                               \
                    if (slot.build_has[k]) state.parallel_ungrouped_has[a] = 1;                          \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_AVG: {                                                               \
                    state.parallel_ungrouped_sum[a] += slot.build_sums[k];                               \
                    state.parallel_ungrouped_count[a] += slot.build_counts[k];                           \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_COUNT: {                                                             \
                    state.parallel_ungrouped_sum[a] += slot.build_counts[k];                             \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MIN: {                                                               \
                    if (slot.build_has[k] &&                                                             \
                        (!state.parallel_ungrouped_has[a] || slot.build_mins[k] < state.parallel_ungrouped_min[a])) { \
                        state.parallel_ungrouped_min[a] = slot.build_mins[k];                            \
                        state.parallel_ungrouped_has[a] = 1;                                             \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MAX: {                                                               \
                    if (slot.build_has[k] &&                                                             \
                        (!state.parallel_ungrouped_has[a] || slot.build_maxs[k] > state.parallel_ungrouped_max[a])) { \
                        state.parallel_ungrouped_max[a] = slot.build_maxs[k];                            \
                        state.parallel_ungrouped_has[a] = 1;                                             \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }

#define AGGJOIN_PARALLEL_DIRECT_GROUPED_DENSE(KTYPE)                                                     \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *sums = state.parallel_direct_sums.data();                                                  \
        auto *counts = state.parallel_direct_counts.empty() ? nullptr : state.parallel_direct_counts.data(); \
        auto *mins = state.parallel_direct_mins.empty() ? nullptr : state.parallel_direct_mins.data();   \
        auto *maxs = state.parallel_direct_maxs.empty() ? nullptr : state.parallel_direct_maxs.data();   \
        auto *has = state.parallel_direct_has.empty() ? nullptr : state.parallel_direct_has.data();      \
        auto *seen = state.parallel_direct_key_seen.data();                                              \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || bc[k] == 0) continue;                                                     \
            if (!seen[k]) {                                                                              \
                seen[k] = 1;                                                                             \
                state.parallel_direct_active_keys.push_back(k);                                          \
            }                                                                                            \
            auto bcount = (double)bc[k];                                                                 \
            for (idx_t a = 0; a < na; a++) {                                                             \
                auto &slot = slots[a];                                                                   \
                auto *validity = slot.validity;                                                          \
                switch (slot.kind) {                                                                     \
                case AggSlot::COUNT_STAR:                                                                \
                    sums[slot.accum_idx * krange + k] += bcount;                                         \
                    break;                                                                               \
                case AggSlot::COUNT_COL:                                                                 \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1))                               \
                        sums[slot.accum_idx * krange + k] += bcount;                                     \
                    break;                                                                               \
                case AggSlot::SUM_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        sums[slot.accum_idx * krange + k] += slot.Value(r) * bcount;                     \
                        if (has) has[slot.has_idx * krange + k] = 1;                                     \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::AVG_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        sums[slot.accum_idx * krange + k] += slot.Value(r) * bcount;                     \
                        counts[slot.avg_idx * krange + k] += bcount;                                     \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MIN_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        auto mm_off = slot.min_idx * krange + k;                                         \
                        auto has_off = slot.has_idx * krange + k;                                        \
                        if (!has[has_off] || v < mins[mm_off]) {                                         \
                            mins[mm_off] = v;                                                            \
                            has[has_off] = 1;                                                            \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MAX_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        auto mm_off = slot.max_idx * krange + k;                                         \
                        auto has_off = slot.has_idx * krange + k;                                        \
                        if (!has[has_off] || v > maxs[mm_off]) {                                         \
                            maxs[mm_off] = v;                                                            \
                            has[has_off] = 1;                                                            \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::BUILD_SUM: {                                                               \
                    sums[slot.accum_idx * krange + k] += slot.build_sums[k];                             \
                    if (has && slot.build_has[k]) has[slot.has_idx * krange + k] = 1;                    \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_AVG: {                                                               \
                    sums[slot.accum_idx * krange + k] += slot.build_sums[k];                             \
                    counts[slot.avg_idx * krange + k] += slot.build_counts[k];                           \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_COUNT: {                                                             \
                    sums[slot.accum_idx * krange + k] += slot.build_counts[k];                           \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MIN: {                                                               \
                    auto mm_off = slot.min_idx * krange + k;                                             \
                    auto has_off = slot.has_idx * krange + k;                                            \
                    if (slot.build_has[k] && (!has[has_off] || slot.build_mins[k] < mins[mm_off])) {     \
                        mins[mm_off] = slot.build_mins[k];                                               \
                        has[has_off] = 1;                                                                \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MAX: {                                                               \
                    auto mm_off = slot.max_idx * krange + k;                                             \
                    auto has_off = slot.has_idx * krange + k;                                            \
                    if (slot.build_has[k] && (!has[has_off] || slot.build_maxs[k] > maxs[mm_off])) {     \
                        maxs[mm_off] = slot.build_maxs[k];                                               \
                        has[has_off] = 1;                                                                \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }

#define AGGJOIN_PARALLEL_DIRECT_GROUPED_SPARSE(KTYPE)                                                    \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || bc[k] == 0) continue;                                                     \
            auto slot_idx = ensure_sparse_slot(k);                                                       \
            auto bcount = (double)bc[k];                                                                 \
            for (idx_t a = 0; a < na; a++) {                                                             \
                auto &slot = slots[a];                                                                   \
                auto *validity = slot.validity;                                                          \
                switch (slot.kind) {                                                                     \
                case AggSlot::COUNT_STAR:                                                                \
                    state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += bcount; \
                    break;                                                                               \
                case AggSlot::COUNT_COL:                                                                 \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1))                               \
                        state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += bcount; \
                    break;                                                                               \
                case AggSlot::SUM_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += slot.Value(r) * bcount; \
                        if (!state.parallel_sparse_has.empty())                                          \
                            state.parallel_sparse_has[slot_idx * sink.direct_has_slots + slot.has_idx] = 1; \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::AVG_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += slot.Value(r) * bcount; \
                        state.parallel_sparse_counts[slot_idx * sink.direct_avg_slots + slot.avg_idx] += bcount; \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MIN_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        auto mm_off = slot_idx * sink.direct_min_slots + slot.min_idx;                   \
                        auto has_off = slot_idx * sink.direct_has_slots + slot.has_idx;                  \
                        if (!state.parallel_sparse_has[has_off] || v < state.parallel_sparse_mins[mm_off]) { \
                            state.parallel_sparse_mins[mm_off] = v;                                      \
                            state.parallel_sparse_has[has_off] = 1;                                      \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::MAX_VAL:                                                                   \
                    if (!validity || ((validity[r / 64] >> (r % 64)) & 1)) {                             \
                        auto v = slot.Value(r);                                                          \
                        auto mm_off = slot_idx * sink.direct_max_slots + slot.max_idx;                   \
                        auto has_off = slot_idx * sink.direct_has_slots + slot.has_idx;                  \
                        if (!state.parallel_sparse_has[has_off] || v > state.parallel_sparse_maxs[mm_off]) { \
                            state.parallel_sparse_maxs[mm_off] = v;                                      \
                            state.parallel_sparse_has[has_off] = 1;                                      \
                        }                                                                                \
                    }                                                                                    \
                    break;                                                                               \
                case AggSlot::BUILD_SUM: {                                                               \
                    state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += slot.build_sums[k]; \
                    if (!state.parallel_sparse_has.empty() && slot.build_has[k])                          \
                        state.parallel_sparse_has[slot_idx * sink.direct_has_slots + slot.has_idx] = 1;  \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_AVG: {                                                               \
                    state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += slot.build_sums[k]; \
                    state.parallel_sparse_counts[slot_idx * sink.direct_avg_slots + slot.avg_idx] += slot.build_counts[k]; \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_COUNT: {                                                             \
                    state.parallel_sparse_sums[slot_idx * sink.direct_accum_slots + slot.accum_idx] += slot.build_counts[k]; \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MIN: {                                                               \
                    auto mm_off = slot_idx * sink.direct_min_slots + slot.min_idx;                       \
                    auto has_off = slot_idx * sink.direct_has_slots + slot.has_idx;                      \
                    if (slot.build_has[k] &&                                                             \
                        (!state.parallel_sparse_has[has_off] || slot.build_mins[k] < state.parallel_sparse_mins[mm_off])) { \
                        state.parallel_sparse_mins[mm_off] = slot.build_mins[k];                         \
                        state.parallel_sparse_has[has_off] = 1;                                          \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                case AggSlot::BUILD_MAX: {                                                               \
                    auto mm_off = slot_idx * sink.direct_max_slots + slot.max_idx;                       \
                    auto has_off = slot_idx * sink.direct_has_slots + slot.has_idx;                      \
                    if (slot.build_has[k] &&                                                             \
                        (!state.parallel_sparse_has[has_off] || slot.build_maxs[k] > state.parallel_sparse_maxs[mm_off])) { \
                        state.parallel_sparse_maxs[mm_off] = slot.build_maxs[k];                         \
                        state.parallel_sparse_has[has_off] = 1;                                          \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }

#define AGGJOIN_PARALLEL_DIRECT_GROUPED(KTYPE)                                                           \
    {                                                                                                    \
        if (state.parallel_direct_sparse_grouped) {                                                       \
            AGGJOIN_PARALLEL_DIRECT_GROUPED_SPARSE(KTYPE);                                               \
        } else {                                                                                         \
            AGGJOIN_PARALLEL_DIRECT_GROUPED_DENSE(KTYPE);                                                \
        }                                                                                                \
    }

    switch (ptype) {
    case PhysicalType::INT8:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(int8_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(int8_t);
        break;
    case PhysicalType::INT16:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(int16_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(int16_t);
        break;
    case PhysicalType::INT32:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(int32_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(int32_t);
        break;
    case PhysicalType::INT64:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(int64_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(int64_t);
        break;
    case PhysicalType::UINT8:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(uint8_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(uint8_t);
        break;
    case PhysicalType::UINT16:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(uint16_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(uint16_t);
        break;
    case PhysicalType::UINT32:
        if (grouped_by_key) AGGJOIN_PARALLEL_DIRECT_GROUPED(uint32_t) else AGGJOIN_PARALLEL_DIRECT_UNGROUPED(uint32_t);
        break;
    default:
        return false;
    }
#undef AGGJOIN_PARALLEL_DIRECT_GROUPED
#undef AGGJOIN_PARALLEL_DIRECT_GROUPED_SPARSE
#undef AGGJOIN_PARALLEL_DIRECT_GROUPED_DENSE
#undef AGGJOIN_PARALLEL_DIRECT_UNGROUPED

    state.parallel_probe_rows_seen += n;
    chunk.SetCardinality(0);
    return true;
}

bool TryExecuteDirectSourcePath(const PhysicalAggJoin &op, DataChunk &input, DataChunk &chunk, AggJoinSinkState &sink,
                                idx_t n, idx_t na) {
    auto &col = op.col;
    if (TryExecuteSegmentedSourcePath(op, input, chunk, sink, n, na)) {
        return true;
    }

    if (!sink.direct_mode) {
        return false;
    }

    auto pki = col.probe_key_cols[0];
    input.data[pki].Flatten(n);
    for (auto i : col.agg_input_cols) {
        if (i != DConstants::INVALID_INDEX && i < input.ColumnCount()) {
            input.data[i].Flatten(n);
        }
    }

    auto ptype = input.data[pki].GetType().InternalType();
    // NULL probe keys never match an inner join; the data under NULL slots is
    // undefined and must not be interpreted as a key offset.
    auto *key_validity = FlatVector::Validity(input.data[pki]).GetData();
    auto kmin = sink.key_min;
    auto krange = sink.key_range;
    auto *bc = sink.build_counts.data();
    auto *sums = sink.direct_sums.data();

    bool all_sum_count_double = true;
    bool has_minmax = sink.has_min_max;
    for (idx_t a = 0; a < na; a++) {
        auto &f = col.agg_funcs[a];
        auto ai = col.agg_input_cols[a];
        if (f == "COUNT" && ai == DConstants::INVALID_INDEX) continue;
        // Invariant: any non-COUNT aggregate reaching here must have a valid probe-side input column.
        // A violation means the planner admitted a shape it shouldn't have (e.g. SUM(a*b) with
        // INVALID input — the STATS-q6 bug class). Debug-assert to catch planner regressions early;
        // release keeps the defensive continue below so wrong results never ship.
        D_ASSERT(ai != DConstants::INVALID_INDEX && ai < input.ColumnCount());
        if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;
        // MIN/MAX inputs are included: the fast path reads every input via
        // FlatVector::GetData<double>, which is only valid for DOUBLE vectors.
        if (input.data[ai].GetType().InternalType() != PhysicalType::DOUBLE) {
            all_sum_count_double = false;
            break;
        }
    }

    struct AggSlot {
        enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL, SKIP } kind;
        const double *vals = nullptr;
        const uint64_t *validity = nullptr;
    };
    auto *avg_counts = sink.has_avg ? sink.direct_counts.data() : nullptr;
    vector<AggSlot> agg_slots(na);
    for (idx_t a = 0; a < na; a++) {
        auto &f = col.agg_funcs[a];
        auto ai = col.agg_input_cols[a];
        // Build-side aggregates also carry an INVALID probe input column; they
        // are accumulated by the dedicated build-agg loops and must not be
        // treated as COUNT(*) here (that would double-count them).
        bool on_build = (col.agg_on_build.size() > a && col.agg_on_build[a]);
        if (f == "COUNT" && ai == DConstants::INVALID_INDEX && !on_build) {
            agg_slots[a].kind = AggSlot::COUNT_STAR;
        } else if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
            agg_slots[a].kind = AggSlot::SKIP;
        } else if (f == "AVG") {
            agg_slots[a].kind = AggSlot::AVG_VAL;
            if (all_sum_count_double) agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
            agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else if (f == "SUM") {
            agg_slots[a].kind = AggSlot::SUM_VAL;
            if (all_sum_count_double) agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
            agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else if (f == "COUNT") {
            agg_slots[a].kind = AggSlot::COUNT_COL;
            agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else if (f == "MIN") {
            agg_slots[a].kind = AggSlot::MIN_VAL;
            if (all_sum_count_double) agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
            agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else if (f == "MAX") {
            agg_slots[a].kind = AggSlot::MAX_VAL;
            if (all_sum_count_double) agg_slots[a].vals = FlatVector::GetData<double>(input.data[ai]);
            agg_slots[a].validity = FlatVector::Validity(input.data[ai]).GetData();
        } else {
            agg_slots[a].kind = AggSlot::SKIP;
        }
    }

    auto *mins = has_minmax ? sink.direct_mins.data() : nullptr;
    auto *maxs = has_minmax ? sink.direct_maxs.data() : nullptr;
    auto *has_arr = sink.direct_has.empty() ? nullptr : sink.direct_has.data();

    auto is_int_key = (ptype == PhysicalType::INT8 || ptype == PhysicalType::INT16 ||
                       ptype == PhysicalType::INT32 || ptype == PhysicalType::INT64 ||
                       ptype == PhysicalType::UINT32 || ptype == PhysicalType::UINT64 ||
                       ptype == PhysicalType::UINT16 || ptype == PhysicalType::UINT8);
    if (col.group_cols.empty() && all_sum_count_double && is_int_key && !sink.ungrouped_sum.empty()) {
#define UNGROUPED_EXTRACT(KTYPE)                                                                         \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k < krange && bc[k] > 0) {                                                               \
                double bcount = (double)bc[k];                                                           \
                for (idx_t a = 0; a < na; a++) {                                                         \
                    auto &slot = agg_slots[a];                                                           \
                    switch (slot.kind) {                                                                 \
                    case AggSlot::COUNT_STAR:                                                            \
                        sink.ungrouped_sum[a] += bcount;                                                 \
                        break;                                                                           \
                    case AggSlot::SUM_VAL: {                                                             \
                        auto *v = slot.validity;                                                         \
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) {                                       \
                            sink.ungrouped_sum[a] += slot.vals[r] * bcount;                              \
                            sink.ungrouped_has[a] = 1;                                                   \
                        }                                                                                \
                        break;                                                                           \
                    }                                                                                    \
                    case AggSlot::AVG_VAL: {                                                             \
                        auto *v = slot.validity;                                                         \
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) {                                       \
                            sink.ungrouped_sum[a] += slot.vals[r] * bcount;                              \
                            sink.ungrouped_count[a] += bcount;                                           \
                        }                                                                                \
                        break;                                                                           \
                    }                                                                                    \
                    case AggSlot::COUNT_COL: {                                                           \
                        auto *v = slot.validity;                                                         \
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) sink.ungrouped_sum[a] += bcount;       \
                        break;                                                                           \
                    }                                                                                    \
                    case AggSlot::MIN_VAL: {                                                             \
                        auto *v = slot.validity;                                                         \
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) {                                       \
                            double dv = slot.vals[r];                                                    \
                            if (!sink.ungrouped_has[a] || dv < sink.ungrouped_min[a]) {                 \
                                sink.ungrouped_min[a] = dv;                                              \
                                sink.ungrouped_has[a] = 1;                                               \
                            }                                                                            \
                        }                                                                                \
                        break;                                                                           \
                    }                                                                                    \
                    case AggSlot::MAX_VAL: {                                                             \
                        auto *v = slot.validity;                                                         \
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) {                                       \
                            double dv = slot.vals[r];                                                    \
                            if (!sink.ungrouped_has[a] || dv > sink.ungrouped_max[a]) {                 \
                                sink.ungrouped_max[a] = dv;                                              \
                                sink.ungrouped_has[a] = 1;                                               \
                            }                                                                            \
                        }                                                                                \
                        break;                                                                           \
                    }                                                                                    \
                    default:                                                                             \
                        break;                                                                           \
                    }                                                                                    \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
        switch (ptype) {
        case PhysicalType::INT8: UNGROUPED_EXTRACT(int8_t); break;
        case PhysicalType::INT16: UNGROUPED_EXTRACT(int16_t); break;
        case PhysicalType::INT32: UNGROUPED_EXTRACT(int32_t); break;
        case PhysicalType::INT64: UNGROUPED_EXTRACT(int64_t); break;
        case PhysicalType::UINT32: UNGROUPED_EXTRACT(uint32_t); break;
        case PhysicalType::UINT64: UNGROUPED_EXTRACT(uint64_t); break;
        case PhysicalType::UINT16: UNGROUPED_EXTRACT(uint16_t); break;
        case PhysicalType::UINT8: UNGROUPED_EXTRACT(uint8_t); break;
        default: break;
        }
#undef UNGROUPED_EXTRACT

        if (HasDirectBuildAggStorage(sink)) {
            idx_t ba = 0;
            for (idx_t a = 0; a < na; a++) {
                if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                auto &f = col.agg_funcs[a];
                auto *bsums = DirectBuildDoublePtr(sink.direct_build_sums, sink.direct_build_sum_index, ba, krange);
                auto *bcnts = DirectBuildDoublePtr(sink.direct_build_counts, sink.direct_build_count_index, ba, krange);
                auto *bmins = DirectBuildDoublePtr(sink.direct_build_mins, sink.direct_build_min_index, ba, krange);
                auto *bmaxs = DirectBuildDoublePtr(sink.direct_build_maxs, sink.direct_build_max_index, ba, krange);
                auto *bhas = DirectBuildHasPtr(sink, ba, krange);
#define UNGROUPED_BUILD_AGG(KTYPE)                                                                       \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (f == "SUM" && bsums && bhas) {                                                           \
                sink.ungrouped_sum[a] += bsums[k];                                                       \
                if (bhas[k]) sink.ungrouped_has[a] = 1;                                                  \
            }                                                                                            \
            else if (f == "AVG" && bsums && bcnts) {                                                     \
                sink.ungrouped_sum[a] += bsums[k];                                                       \
                sink.ungrouped_count[a] += bcnts[k];                                                     \
            } else if (f == "COUNT" && bcnts) sink.ungrouped_sum[a] += bcnts[k];                         \
            else if (f == "MIN" && bmins && bhas && bhas[k]) {                                           \
                auto bv = bmins[k];                                                                      \
                if (!sink.ungrouped_has[a] || bv < sink.ungrouped_min[a]) {                              \
                    sink.ungrouped_min[a] = bv;                                                          \
                    sink.ungrouped_has[a] = 1;                                                           \
                }                                                                                        \
            } else if (f == "MAX" && bmaxs && bhas && bhas[k]) {                                         \
                auto bv = bmaxs[k];                                                                      \
                if (!sink.ungrouped_has[a] || bv > sink.ungrouped_max[a]) {                              \
                    sink.ungrouped_max[a] = bv;                                                          \
                    sink.ungrouped_has[a] = 1;                                                           \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
                switch (ptype) {
                case PhysicalType::INT32: UNGROUPED_BUILD_AGG(int32_t); break;
                case PhysicalType::INT64: UNGROUPED_BUILD_AGG(int64_t); break;
                case PhysicalType::UINT32: UNGROUPED_BUILD_AGG(uint32_t); break;
                case PhysicalType::UINT64: UNGROUPED_BUILD_AGG(uint64_t); break;
                case PhysicalType::UINT16: UNGROUPED_BUILD_AGG(uint16_t); break;
                case PhysicalType::UINT8: UNGROUPED_BUILD_AGG(uint8_t); break;
                default: break;
                }
#undef UNGROUPED_BUILD_AGG
                ba++;
            }
        }

        chunk.SetCardinality(0);
        return true;
    }

    if (all_sum_count_double && is_int_key) {
        sink.direct_key_buf.resize(n);
        auto &key_buf = sink.direct_key_buf;
        bool pkfk = sink.all_bc_one;
        auto &bc_buf = sink.direct_bc_buf;
        auto track_active_keys = sink.track_active_keys;
        auto *seen_keys = track_active_keys ? sink.direct_key_seen.data() : nullptr;
        if (!pkfk) {
            bc_buf.resize(n);
            std::fill(bc_buf.begin(), bc_buf.end(), 0.0);
        }
#define EXTRACT_KEYS(KTYPE)                                                                              \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        if (pkfk) {                                                                                      \
            for (idx_t r = 0; r < n; r++) {                                                              \
                if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) {                        \
                    key_buf[r] = krange; /* sentinel: NULL key never matches */                          \
                    continue;                                                                            \
                }                                                                                        \
                auto k = (idx_t)((int64_t)keys[r] - kmin);                                               \
                if (k >= krange || !bc[k]) {                                                             \
                    key_buf[r] = krange; /* sentinel: no build match for this key */                     \
                    continue;                                                                            \
                }                                                                                        \
                key_buf[r] = k;                                                                          \
                if (track_active_keys && !seen_keys[k]) {                                                \
                    seen_keys[k] = 1;                                                                    \
                    sink.direct_active_keys.push_back(k);                                                \
                }                                                                                        \
            }                                                                                            \
        } else {                                                                                         \
            for (idx_t r = 0; r < n; r++) {                                                              \
                if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) {                        \
                    key_buf[r] = krange;                                                                 \
                    bc_buf[r] = 0.0;                                                                     \
                    continue;                                                                            \
                }                                                                                        \
                auto k = (idx_t)((int64_t)keys[r] - kmin);                                               \
                key_buf[r] = k;                                                                          \
                bc_buf[r] = (k < krange) ? (double)bc[k] : 0.0;                                          \
                if (track_active_keys && bc_buf[r] != 0.0 && !seen_keys[k]) {                           \
                    seen_keys[k] = 1;                                                                    \
                    sink.direct_active_keys.push_back(k);                                                \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
        switch (ptype) {
        case PhysicalType::INT8: EXTRACT_KEYS(int8_t); break;
        case PhysicalType::INT16: EXTRACT_KEYS(int16_t); break;
        case PhysicalType::INT32: EXTRACT_KEYS(int32_t); break;
        case PhysicalType::INT64: EXTRACT_KEYS(int64_t); break;
        case PhysicalType::UINT32: EXTRACT_KEYS(uint32_t); break;
        case PhysicalType::UINT64: EXTRACT_KEYS(uint64_t); break;
        case PhysicalType::UINT16: EXTRACT_KEYS(uint16_t); break;
        case PhysicalType::UINT8: EXTRACT_KEYS(uint8_t); break;
        default: break;
        }
#undef EXTRACT_KEYS

        for (idx_t a = 0; a < na; a++) {
            auto &slot = agg_slots[a];
            double *agg_sums = DirectDoublePtr(sink.direct_sums, sink.direct_accum_index, a, krange);
            switch (slot.kind) {
            case AggSlot::COUNT_STAR:
                if (!agg_sums) break;
                if (pkfk) {
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (k < krange) agg_sums[k] += 1.0;
                    }
                } else {
                    for (idx_t r = 0; r < n; r++) {
                        if (bc_buf[r] != 0.0) agg_sums[key_buf[r]] += bc_buf[r];
                    }
                }
                break;
            case AggSlot::SUM_VAL: {
                auto *v = slot.validity;
                uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                if (!agg_sums || !agg_has) break;
                if (pkfk) {
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (k >= krange) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[k] += slot.vals[r];
                        agg_has[k] = 1;
                    }
                } else {
                    for (idx_t r = 0; r < n; r++) {
                        if (bc_buf[r] == 0.0) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[key_buf[r]] += slot.vals[r] * bc_buf[r];
                        agg_has[key_buf[r]] = 1;
                    }
                }
                break;
            }
            case AggSlot::AVG_VAL: {
                double *agg_counts = DirectDoublePtr(sink.direct_counts, sink.direct_avg_index, a, krange);
                auto *v = slot.validity;
                if (!agg_sums || !agg_counts) break;
                if (pkfk) {
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (k >= krange) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[k] += slot.vals[r];
                        agg_counts[k] += 1.0;
                    }
                } else {
                    for (idx_t r = 0; r < n; r++) {
                        if (bc_buf[r] == 0.0) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[key_buf[r]] += slot.vals[r] * bc_buf[r];
                        agg_counts[key_buf[r]] += bc_buf[r];
                    }
                }
                break;
            }
            case AggSlot::COUNT_COL: {
                auto *v = slot.validity;
                if (!agg_sums) break;
                if (pkfk) {
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (k >= krange) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[k] += 1.0;
                    }
                } else {
                    for (idx_t r = 0; r < n; r++) {
                        if (bc_buf[r] == 0.0) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[key_buf[r]] += bc_buf[r];
                    }
                }
                break;
            }
            case AggSlot::MIN_VAL: {
                double *agg_mins = DirectDoublePtr(sink.direct_mins, sink.direct_min_index, a, krange);
                uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                auto *v = slot.validity;
                if (!agg_mins || !agg_has) break;
                for (idx_t r = 0; r < n; r++) {
                    auto k = key_buf[r];
                    if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                    if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                    auto dv = slot.vals[r];
                    if (!agg_has[k] || dv < agg_mins[k]) {
                        agg_mins[k] = dv;
                        agg_has[k] = 1;
                    }
                }
                break;
            }
            case AggSlot::MAX_VAL: {
                double *agg_maxs = DirectDoublePtr(sink.direct_maxs, sink.direct_max_index, a, krange);
                uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                auto *v = slot.validity;
                if (!agg_maxs || !agg_has) break;
                for (idx_t r = 0; r < n; r++) {
                    auto k = key_buf[r];
                    if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                    if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                    auto dv = slot.vals[r];
                    if (!agg_has[k] || dv > agg_maxs[k]) {
                        agg_maxs[k] = dv;
                        agg_has[k] = 1;
                    }
                }
                break;
            }
            default:
                break;
            }
        }

        if (HasDirectBuildAggStorage(sink)) {
            idx_t ba = 0;
            for (idx_t a = 0; a < na; a++) {
                if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                auto &f = col.agg_funcs[a];
                double *agg_sums_a = DirectDoublePtr(sink.direct_sums, sink.direct_accum_index, a, krange);
                if (f == "SUM") {
                    auto *bsums = DirectBuildDoublePtr(sink.direct_build_sums, sink.direct_build_sum_index, ba, krange);
                    auto *bhas = DirectBuildHasPtr(sink, ba, krange);
                    uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                    if (!agg_sums_a || !bsums || !bhas || !agg_has) { ba++; continue; }
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        agg_sums_a[k] += bsums[k];
                        if (bhas[k]) agg_has[k] = 1;
                    }
                } else if (f == "MIN" && mins) {
                    auto *bmins = DirectBuildDoublePtr(sink.direct_build_mins, sink.direct_build_min_index, ba, krange);
                    auto *bhas = DirectBuildHasPtr(sink, ba, krange);
                    uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                    double *agg_mins = DirectDoublePtr(sink.direct_mins, sink.direct_min_index, a, krange);
                    if (!bmins || !bhas || !agg_has || !agg_mins) { ba++; continue; }
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        if (bhas[k] && (!agg_has[k] || bmins[k] < agg_mins[k])) {
                            agg_mins[k] = bmins[k];
                            agg_has[k] = 1;
                        }
                    }
                } else if (f == "MAX" && maxs) {
                    auto *bmaxs = DirectBuildDoublePtr(sink.direct_build_maxs, sink.direct_build_max_index, ba, krange);
                    auto *bhas = DirectBuildHasPtr(sink, ba, krange);
                    uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                    double *agg_maxs = DirectDoublePtr(sink.direct_maxs, sink.direct_max_index, a, krange);
                    if (!bmaxs || !bhas || !agg_has || !agg_maxs) { ba++; continue; }
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        if (bhas[k] && (!agg_has[k] || bmaxs[k] > agg_maxs[k])) {
                            agg_maxs[k] = bmaxs[k];
                            agg_has[k] = 1;
                        }
                    }
                } else if (f == "COUNT") {
                    auto *bcnts = DirectBuildDoublePtr(sink.direct_build_counts, sink.direct_build_count_index, ba, krange);
                    if (!agg_sums_a || !bcnts) { ba++; continue; }
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        agg_sums_a[k] += bcnts[k];
                    }
                } else if (f == "AVG" && avg_counts) {
                    auto *bsums = DirectBuildDoublePtr(sink.direct_build_sums, sink.direct_build_sum_index, ba, krange);
                    auto *bcnts = DirectBuildDoublePtr(sink.direct_build_counts, sink.direct_build_count_index, ba, krange);
                    double *agg_counts_a = DirectDoublePtr(sink.direct_counts, sink.direct_avg_index, a, krange);
                    if (!agg_sums_a || !bsums || !bcnts || !agg_counts_a) { ba++; continue; }
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        agg_sums_a[k] += bsums[k];
                        agg_counts_a[k] += bcnts[k];
                    }
                }
                ba++;
            }
        }
    } else {
        if (col.group_cols.empty()) {
            // Slow-path loops accumulate per key; emit folds them once.
            sink.ungrouped_per_key = true;
        }
        if (sink.track_active_keys) {
            // The slow-path probe loops below index keys directly without the
            // shared key_buf; record probe-matched keys here so grouped direct
            // emit stays gated on probe matches (not build-side presence).
            auto *seen = sink.direct_key_seen.data();
#define MARK_ACTIVE_KEYS(KTYPE)                                                                          \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k] || seen[k]) continue;                                              \
            seen[k] = 1;                                                                                 \
            sink.direct_active_keys.push_back(k);                                                        \
        }                                                                                                \
    }
            switch (ptype) {
            case PhysicalType::INT8: MARK_ACTIVE_KEYS(int8_t); break;
            case PhysicalType::INT16: MARK_ACTIVE_KEYS(int16_t); break;
            case PhysicalType::INT32: MARK_ACTIVE_KEYS(int32_t); break;
            case PhysicalType::INT64: MARK_ACTIVE_KEYS(int64_t); break;
            case PhysicalType::UINT8: MARK_ACTIVE_KEYS(uint8_t); break;
            case PhysicalType::UINT16: MARK_ACTIVE_KEYS(uint16_t); break;
            case PhysicalType::UINT32: MARK_ACTIVE_KEYS(uint32_t); break;
            case PhysicalType::UINT64: MARK_ACTIVE_KEYS(uint64_t); break;
            default:
                for (idx_t r = 0; r < n; r++) {
                    auto kv = input.data[pki].GetValue(r);
                    if (kv.IsNull()) continue;
                    auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                    if (k >= krange || !bc[k] || seen[k]) continue;
                    seen[k] = 1;
                    sink.direct_active_keys.push_back(k);
                }
                break;
            }
#undef MARK_ACTIVE_KEYS
        }
        for (idx_t a = 0; a < na; a++) {
            auto ai = col.agg_input_cols[a];
            auto &f = col.agg_funcs[a];
            bool is_build_agg = (col.agg_on_build.size() > a && col.agg_on_build[a]);

            if (f == "COUNT" && ai == DConstants::INVALID_INDEX && !is_build_agg) {
                double *agg_s = DirectDoublePtr(sink.direct_sums, sink.direct_accum_index, a, krange);
                if (!agg_s) continue;
#define DIRECT_COUNT_LOOP(TYPE)                                                                          \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<TYPE>(input.data[pki]);                                         \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k < krange && bc[k]) agg_s[k] += (double)bc[k];                                          \
        }                                                                                                \
    }
                switch (ptype) {
                case PhysicalType::INT8: DIRECT_COUNT_LOOP(int8_t); break;
                case PhysicalType::INT16: DIRECT_COUNT_LOOP(int16_t); break;
                case PhysicalType::INT32: DIRECT_COUNT_LOOP(int32_t); break;
                case PhysicalType::INT64: DIRECT_COUNT_LOOP(int64_t); break;
                case PhysicalType::UINT8: DIRECT_COUNT_LOOP(uint8_t); break;
                case PhysicalType::UINT16: DIRECT_COUNT_LOOP(uint16_t); break;
                case PhysicalType::UINT32: DIRECT_COUNT_LOOP(uint32_t); break;
                case PhysicalType::UINT64: DIRECT_COUNT_LOOP(uint64_t); break;
                default: break;
                }
#undef DIRECT_COUNT_LOOP
                continue;
            }

            if (is_build_agg && HasDirectBuildAggStorage(sink)) {
                idx_t ba = 0;
                for (idx_t i = 0; i < a; i++) {
                    if (col.agg_on_build.size() > i && col.agg_on_build[i]) ba++;
                }
                auto *bsums = DirectBuildDoublePtr(sink.direct_build_sums, sink.direct_build_sum_index, ba, krange);
                auto *bcnts = DirectBuildDoublePtr(sink.direct_build_counts, sink.direct_build_count_index, ba, krange);
                auto *bmins = DirectBuildDoublePtr(sink.direct_build_mins, sink.direct_build_min_index, ba, krange);
                auto *bmaxs = DirectBuildDoublePtr(sink.direct_build_maxs, sink.direct_build_max_index, ba, krange);
                auto *build_has = DirectBuildHasPtr(sink, ba, krange);
                double *agg_s = DirectDoublePtr(sink.direct_sums, sink.direct_accum_index, a, krange);
                double *agg_c = DirectDoublePtr(sink.direct_counts, sink.direct_avg_index, a, krange);
                double *agg_min = DirectDoublePtr(sink.direct_mins, sink.direct_min_index, a, krange);
                double *agg_max = DirectDoublePtr(sink.direct_maxs, sink.direct_max_index, a, krange);
                uint8_t *agg_has = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
#define DIRECT_BUILD_AGG_LOOP(KTYPE)                                                                     \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (f == "SUM" && agg_s && bsums && build_has) {                                             \
                agg_s[k] += bsums[k];                                                                    \
                if (agg_has && build_has[k]) agg_has[k] = 1;                                             \
            }                                                                                            \
            else if (f == "AVG" && agg_s && bsums && bcnts) {                                            \
                agg_s[k] += bsums[k];                                                                    \
                if (agg_c) agg_c[k] += bcnts[k];                                                         \
            }                                                                                            \
            else if (f == "COUNT" && agg_s && bcnts) agg_s[k] += bcnts[k];                              \
            else if (f == "MIN" && has_minmax && bmins && build_has && agg_min && agg_has) {             \
                if (!build_has[k]) continue;                                                             \
                auto bv = bmins[k];                                                                      \
                if (!agg_has[k] || bv < agg_min[k]) { agg_min[k] = bv; agg_has[k] = 1; }                \
            } else if (f == "MAX" && has_minmax && bmaxs && build_has && agg_max && agg_has) {           \
                if (!build_has[k]) continue;                                                             \
                auto bv = bmaxs[k];                                                                      \
                if (!agg_has[k] || bv > agg_max[k]) { agg_max[k] = bv; agg_has[k] = 1; }                \
            }                                                                                            \
        }                                                                                                \
    }
                switch (ptype) {
                case PhysicalType::INT8: DIRECT_BUILD_AGG_LOOP(int8_t); break;
                case PhysicalType::INT16: DIRECT_BUILD_AGG_LOOP(int16_t); break;
                case PhysicalType::INT32: DIRECT_BUILD_AGG_LOOP(int32_t); break;
                case PhysicalType::INT64: DIRECT_BUILD_AGG_LOOP(int64_t); break;
                case PhysicalType::UINT8: DIRECT_BUILD_AGG_LOOP(uint8_t); break;
                case PhysicalType::UINT16: DIRECT_BUILD_AGG_LOOP(uint16_t); break;
                case PhysicalType::UINT32: DIRECT_BUILD_AGG_LOOP(uint32_t); break;
                case PhysicalType::UINT64: DIRECT_BUILD_AGG_LOOP(uint64_t); break;
                default: {
                    for (idx_t r = 0; r < n; r++) {
                        auto kv = input.data[pki].GetValue(r);
                        if (kv.IsNull()) continue;
                        auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                        if (k >= krange || !bc[k]) continue;
                        if (f == "SUM" && agg_s && bsums && build_has) {
                            agg_s[k] += bsums[k];
                            if (agg_has && build_has[k]) agg_has[k] = 1;
                        } else if (f == "AVG" && agg_s && bsums && bcnts) {
                            agg_s[k] += bsums[k];
                            if (agg_c) agg_c[k] += bcnts[k];
                        }
                        else if (f == "COUNT" && agg_s && bcnts) agg_s[k] += bcnts[k];
                        else if (f == "MIN" && has_minmax && bmins && build_has && agg_min && agg_has) {
                            if (!build_has[k]) continue;
                            auto bv = bmins[k];
                            if (!agg_has[k] || bv < agg_min[k]) { agg_min[k] = bv; agg_has[k] = 1; }
                        } else if (f == "MAX" && has_minmax && bmaxs && build_has && agg_max && agg_has) {
                            if (!build_has[k]) continue;
                            auto bv = bmaxs[k];
                            if (!agg_has[k] || bv > agg_max[k]) { agg_max[k] = bv; agg_has[k] = 1; }
                        }
                    }
                    break;
                }
                }
#undef DIRECT_BUILD_AGG_LOOP
                continue;
            }
            if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;

            if (f == "SUM" || f == "AVG" || f == "COUNT") {
                auto vtype = input.data[ai].GetType().InternalType();
                auto *validity = FlatVector::Validity(input.data[ai]).GetData();
                bool is_avg = (f == "AVG");
                // COUNT(col) counts matched rows with non-NULL values — it must
                // never accumulate the values themselves.
                bool is_count = (f == "COUNT");
                auto *dcounts = is_avg ? DirectDoublePtr(sink.direct_counts, sink.direct_avg_index, a, krange) : nullptr;
                uint8_t *agg_h = (f == "SUM" && has_arr) ? DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange) : nullptr;
                double *agg_s = DirectDoublePtr(sink.direct_sums, sink.direct_accum_index, a, krange);
                if (!agg_s || (is_avg && !dcounts) || (f == "SUM" && !agg_h)) {
                    continue;
                }
#define DIRECT_SUM_LOOP(KTYPE, VTYPE)                                                                    \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]);                                         \
        double *agg_c = dcounts;                                                                         \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                            \
            agg_s[k] += is_count ? (double)bc[k] : (double)vals[r] * (double)bc[k];                      \
            if (agg_c) agg_c[k] += (double)bc[k];                                                        \
            if (agg_h) agg_h[k] = 1;                                                                     \
        }                                                                                                \
    }
                bool ran_typed = true;
#define DIRECT_SUM_KEY_SWITCH(VTYPE)                                                                     \
    switch (ptype) {                                                                                     \
    case PhysicalType::INT8: DIRECT_SUM_LOOP(int8_t, VTYPE); break;                                     \
    case PhysicalType::INT16: DIRECT_SUM_LOOP(int16_t, VTYPE); break;                                   \
    case PhysicalType::INT32: DIRECT_SUM_LOOP(int32_t, VTYPE); break;                                   \
    case PhysicalType::INT64: DIRECT_SUM_LOOP(int64_t, VTYPE); break;                                   \
    case PhysicalType::UINT8: DIRECT_SUM_LOOP(uint8_t, VTYPE); break;                                   \
    case PhysicalType::UINT16: DIRECT_SUM_LOOP(uint16_t, VTYPE); break;                                 \
    case PhysicalType::UINT32: DIRECT_SUM_LOOP(uint32_t, VTYPE); break;                                 \
    case PhysicalType::UINT64: DIRECT_SUM_LOOP(uint64_t, VTYPE); break;                                 \
    default: ran_typed = false; break;                                                                   \
    }
                switch (vtype) {
                case PhysicalType::DOUBLE: DIRECT_SUM_KEY_SWITCH(double); break;
                case PhysicalType::FLOAT: DIRECT_SUM_KEY_SWITCH(float); break;
                case PhysicalType::INT64: DIRECT_SUM_KEY_SWITCH(int64_t); break;
                case PhysicalType::INT32: DIRECT_SUM_KEY_SWITCH(int32_t); break;
                case PhysicalType::INT16: DIRECT_SUM_KEY_SWITCH(int16_t); break;
                case PhysicalType::INT8: DIRECT_SUM_KEY_SWITCH(int8_t); break;
                case PhysicalType::UINT64: DIRECT_SUM_KEY_SWITCH(uint64_t); break;
                case PhysicalType::UINT32: DIRECT_SUM_KEY_SWITCH(uint32_t); break;
                case PhysicalType::UINT16: DIRECT_SUM_KEY_SWITCH(uint16_t); break;
                case PhysicalType::UINT8: DIRECT_SUM_KEY_SWITCH(uint8_t); break;
                default: ran_typed = false; break;
                }
#undef DIRECT_SUM_KEY_SWITCH
                if (!ran_typed) {
                    double *agg_c = dcounts;
                    for (idx_t r = 0; r < n; r++) {
                        auto kv = input.data[pki].GetValue(r);
                        if (kv.IsNull()) continue;
                        auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                        if (k >= krange || !bc[k]) continue;
                        auto v = input.data[ai].GetValue(r);
                        if (v.IsNull()) continue;
                        agg_s[k] += is_count ? (double)bc[k] : v.GetValue<double>() * (double)bc[k];
                        if (agg_c) agg_c[k] += (double)bc[k];
                        if (agg_h) agg_h[k] = 1;
                    }
                }
#undef DIRECT_SUM_LOOP
            } else if (f == "MIN" || f == "MAX") {
                bool is_min = (f == "MIN");
                double *agg_m = is_min ? DirectDoublePtr(sink.direct_mins, sink.direct_min_index, a, krange)
                                       : DirectDoublePtr(sink.direct_maxs, sink.direct_max_index, a, krange);
                uint8_t *agg_h = DirectHasPtr(sink.direct_has, sink.direct_has_index, a, krange);
                if (!agg_m || !agg_h) {
                    continue;
                }
                auto vtype = input.data[ai].GetType().InternalType();
                auto *mm_validity = FlatVector::Validity(input.data[ai]).GetData();
#define DIRECT_MINMAX_LOOP(KTYPE, VTYPE)                                                                 \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]);                                         \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (mm_validity && !((mm_validity[r / 64] >> (r % 64)) & 1)) continue;                      \
            if (has_minmax) {                                                                            \
                double dv = (double)vals[r];                                                             \
                if (is_min) {                                                                            \
                    if (!agg_h[k] || dv < agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; }                    \
                } else {                                                                                 \
                    if (!agg_h[k] || dv > agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; }                    \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
                if (has_minmax) {
                    bool ran_typed = true;
#define DIRECT_MINMAX_KEY_SWITCH(VTYPE)                                                                  \
    switch (ptype) {                                                                                     \
    case PhysicalType::INT8: DIRECT_MINMAX_LOOP(int8_t, VTYPE); break;                                  \
    case PhysicalType::INT16: DIRECT_MINMAX_LOOP(int16_t, VTYPE); break;                                \
    case PhysicalType::INT32: DIRECT_MINMAX_LOOP(int32_t, VTYPE); break;                                \
    case PhysicalType::INT64: DIRECT_MINMAX_LOOP(int64_t, VTYPE); break;                                \
    case PhysicalType::UINT8: DIRECT_MINMAX_LOOP(uint8_t, VTYPE); break;                                \
    case PhysicalType::UINT16: DIRECT_MINMAX_LOOP(uint16_t, VTYPE); break;                              \
    case PhysicalType::UINT32: DIRECT_MINMAX_LOOP(uint32_t, VTYPE); break;                              \
    case PhysicalType::UINT64: DIRECT_MINMAX_LOOP(uint64_t, VTYPE); break;                              \
    default: ran_typed = false; break;                                                                   \
    }
                    switch (vtype) {
                    case PhysicalType::DOUBLE: DIRECT_MINMAX_KEY_SWITCH(double); break;
                    case PhysicalType::FLOAT: DIRECT_MINMAX_KEY_SWITCH(float); break;
                    case PhysicalType::INT64: DIRECT_MINMAX_KEY_SWITCH(int64_t); break;
                    case PhysicalType::INT32: DIRECT_MINMAX_KEY_SWITCH(int32_t); break;
                    case PhysicalType::INT16: DIRECT_MINMAX_KEY_SWITCH(int16_t); break;
                    case PhysicalType::INT8: DIRECT_MINMAX_KEY_SWITCH(int8_t); break;
                    case PhysicalType::UINT64: DIRECT_MINMAX_KEY_SWITCH(uint64_t); break;
                    case PhysicalType::UINT32: DIRECT_MINMAX_KEY_SWITCH(uint32_t); break;
                    case PhysicalType::UINT16: DIRECT_MINMAX_KEY_SWITCH(uint16_t); break;
                    case PhysicalType::UINT8: DIRECT_MINMAX_KEY_SWITCH(uint8_t); break;
                    default: ran_typed = false; break;
                    }
#undef DIRECT_MINMAX_KEY_SWITCH
                    if (!ran_typed) {
                        for (idx_t r = 0; r < n; r++) {
                            auto kv = input.data[pki].GetValue(r);
                            if (kv.IsNull()) continue;
                            auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                            if (k >= krange || !bc[k]) continue;
                            auto v = input.data[ai].GetValue(r);
                            if (v.IsNull()) continue;
                            auto dv = v.GetValue<double>();
                            if (is_min) {
                                if (!agg_h[k] || dv < agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; }
                            } else {
                                if (!agg_h[k] || dv > agg_m[k]) { agg_m[k] = dv; agg_h[k] = 1; }
                            }
                        }
                    }
                }
#undef DIRECT_MINMAX_LOOP
            }
        }
    }

    if (!sink.group_is_key && !col.group_cols.empty()) {
#define DIRECT_GROUP_CAPTURE(KTYPE)                                                                      \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (key_validity && !((key_validity[r / 64] >> (r % 64)) & 1)) continue;                    \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k] || sink.direct_group_init[k]) continue;                            \
            sink.direct_group_init[k] = true;                                                            \
            for (auto gi : col.group_cols) sink.direct_group_vals[k].push_back(input.data[gi].GetValue(r)); \
        }                                                                                                \
    }
        switch (ptype) {
        case PhysicalType::INT8: DIRECT_GROUP_CAPTURE(int8_t); break;
        case PhysicalType::INT16: DIRECT_GROUP_CAPTURE(int16_t); break;
        case PhysicalType::INT32: DIRECT_GROUP_CAPTURE(int32_t); break;
        case PhysicalType::INT64: DIRECT_GROUP_CAPTURE(int64_t); break;
        case PhysicalType::UINT8: DIRECT_GROUP_CAPTURE(uint8_t); break;
        case PhysicalType::UINT16: DIRECT_GROUP_CAPTURE(uint16_t); break;
        case PhysicalType::UINT32: DIRECT_GROUP_CAPTURE(uint32_t); break;
        case PhysicalType::UINT64: DIRECT_GROUP_CAPTURE(uint64_t); break;
        default:
            for (idx_t r = 0; r < n; r++) {
                auto kv = input.data[pki].GetValue(r);
                if (kv.IsNull()) continue;
                auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                if (k >= krange || !bc[k] || sink.direct_group_init[k]) continue;
                sink.direct_group_init[k] = true;
                for (auto gi : col.group_cols) sink.direct_group_vals[k].push_back(input.data[gi].GetValue(r));
            }
            break;
        }
#undef DIRECT_GROUP_CAPTURE
    }

    chunk.SetCardinality(0);
    return true;
}

} // namespace duckdb
