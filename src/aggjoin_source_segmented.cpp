#include "aggjoin_source_internal.hpp"

namespace duckdb {

bool TryExecuteSegmentedSourcePath(const PhysicalAggJoin &op, DataChunk &input, DataChunk &chunk,
                                   AggJoinSinkState &sink, idx_t n, idx_t na) {
    auto &col = op.col;

    if (sink.segmented_multi_direct_mode) {
        auto pki = col.probe_key_cols[0];
        input.data[pki].Flatten(n);
        for (auto i : col.agg_input_cols) {
            if (i != DConstants::INVALID_INDEX && i < input.ColumnCount()) {
                input.data[i].Flatten(n);
            }
        }

        auto ptype = input.data[pki].GetType().InternalType();
        auto kmin = sink.key_min;
        auto seg_shift = sink.segmented_shift;
        auto seg_mask = sink.segmented_mask;
        auto range = sink.key_range;
        bool pkfk = sink.all_bc_one;
        auto *active_bits = sink.segmented_active_bits.data();
        auto mark_active = [&](idx_t key_offset) {
            auto word = key_offset >> 6;
            auto bit = 1ULL << (key_offset & 63);
            if (!(active_bits[word] & bit)) {
                active_bits[word] |= bit;
                sink.segmented_active_keys.push_back(key_offset);
            }
        };

        struct SegAggSlot {
            enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL } kind;
            const double *vals = nullptr;
            const uint64_t *validity = nullptr;
            idx_t accum_idx = DConstants::INVALID_INDEX;
            idx_t avg_idx = DConstants::INVALID_INDEX;
            idx_t mm_idx = DConstants::INVALID_INDEX;
        };
        vector<SegAggSlot> agg_slots(na);
        for (idx_t a = 0; a < na; a++) {
            auto &slot = agg_slots[a];
            auto &f = col.agg_funcs[a];
            auto ai = col.agg_input_cols[a];
            slot.accum_idx = sink.segmented_accum_index[a];
            slot.avg_idx = sink.segmented_avg_index[a];
            if (f == "SUM") slot.kind = SegAggSlot::SUM_VAL;
            else if (f == "AVG") slot.kind = SegAggSlot::AVG_VAL;
            else if (f == "COUNT" && ai == DConstants::INVALID_INDEX) slot.kind = SegAggSlot::COUNT_STAR;
            else if (f == "COUNT") slot.kind = SegAggSlot::COUNT_COL;
            else if (f == "MIN") {
                slot.kind = SegAggSlot::MIN_VAL;
                slot.mm_idx = sink.segmented_min_index[a];
            } else {
                slot.kind = SegAggSlot::MAX_VAL;
                slot.mm_idx = sink.segmented_max_index[a];
            }
            if (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) {
                slot.validity = FlatVector::Validity(input.data[ai]).GetData();
                if (f != "COUNT") slot.vals = FlatVector::GetData<double>(input.data[ai]);
            }
        }

#define SEGMENTED_MULTI_KEY_LOOP(KTYPE)                                                                  \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= range) continue;                                                                    \
            auto seg = k >> seg_shift;                                                                   \
            auto local = k & seg_mask;                                                                   \
            auto &count_seg = sink.segmented_build_counts[seg];                                          \
            if (count_seg.empty()) continue;                                                             \
            auto bcount = count_seg[local];                                                              \
            if (!bcount) continue;                                                                       \
            mark_active(k);                                                                              \
            auto mult = pkfk ? 1.0 : (double)bcount;                                                     \
            auto *accums = sink.segmented_accum_slots ? sink.segmented_multi_accums[seg].data() : nullptr;       \
            auto *avg_counts = sink.segmented_avg_slots ? sink.segmented_multi_avg_counts[seg].data() : nullptr; \
            auto *mins = sink.segmented_min_slots ? sink.segmented_multi_mins[seg].data() : nullptr;             \
            auto *maxs = sink.segmented_max_slots ? sink.segmented_multi_maxs[seg].data() : nullptr;             \
            auto *min_has = sink.segmented_min_slots ? sink.segmented_multi_min_has[seg].data() : nullptr;       \
            auto *max_has = sink.segmented_max_slots ? sink.segmented_multi_max_has[seg].data() : nullptr;       \
            auto accum_base = local * sink.segmented_accum_slots;                                        \
            auto avg_base = local * sink.segmented_avg_slots;                                            \
            auto min_base = local * sink.segmented_min_slots;                                            \
            auto max_base = local * sink.segmented_max_slots;                                            \
            for (idx_t a = 0; a < na; a++) {                                                             \
                auto &slot = agg_slots[a];                                                               \
                switch (slot.kind) {                                                                     \
                case SegAggSlot::COUNT_STAR:                                                             \
                    accums[accum_base + slot.accum_idx] += mult;                                         \
                    break;                                                                               \
                case SegAggSlot::COUNT_COL:                                                              \
                    if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;             \
                    accums[accum_base + slot.accum_idx] += mult;                                         \
                    break;                                                                               \
                case SegAggSlot::SUM_VAL:                                                                \
                    if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;             \
                    accums[accum_base + slot.accum_idx] += slot.vals[r] * mult;                          \
                    break;                                                                               \
                case SegAggSlot::AVG_VAL:                                                                \
                    if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;             \
                    accums[accum_base + slot.accum_idx] += slot.vals[r] * mult;                          \
                    avg_counts[avg_base + slot.avg_idx] += mult;                                         \
                    break;                                                                               \
                case SegAggSlot::MIN_VAL:                                                                \
                    if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;             \
                    if (!min_has[min_base + slot.mm_idx] || slot.vals[r] < mins[min_base + slot.mm_idx]) {      \
                        mins[min_base + slot.mm_idx] = slot.vals[r];                                     \
                        min_has[min_base + slot.mm_idx] = 1;                                             \
                    }                                                                                    \
                    break;                                                                               \
                case SegAggSlot::MAX_VAL:                                                                \
                    if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;             \
                    if (!max_has[max_base + slot.mm_idx] || slot.vals[r] > maxs[max_base + slot.mm_idx]) {      \
                        maxs[max_base + slot.mm_idx] = slot.vals[r];                                     \
                        max_has[max_base + slot.mm_idx] = 1;                                             \
                    }                                                                                    \
                    break;                                                                               \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
        switch (ptype) {
        case PhysicalType::INT8: SEGMENTED_MULTI_KEY_LOOP(int8_t); break;
        case PhysicalType::INT16: SEGMENTED_MULTI_KEY_LOOP(int16_t); break;
        case PhysicalType::INT32: SEGMENTED_MULTI_KEY_LOOP(int32_t); break;
        case PhysicalType::INT64: SEGMENTED_MULTI_KEY_LOOP(int64_t); break;
        case PhysicalType::UINT8: SEGMENTED_MULTI_KEY_LOOP(uint8_t); break;
        case PhysicalType::UINT16: SEGMENTED_MULTI_KEY_LOOP(uint16_t); break;
        case PhysicalType::UINT32: SEGMENTED_MULTI_KEY_LOOP(uint32_t); break;
        case PhysicalType::UINT64: SEGMENTED_MULTI_KEY_LOOP(uint64_t); break;
        default: break;
        }
#undef SEGMENTED_MULTI_KEY_LOOP
        chunk.SetCardinality(0);
        return true;
    }

    if (!sink.segmented_direct_mode) {
        return false;
    }

    auto pki = col.probe_key_cols[0];
    input.data[pki].Flatten(n);
    auto &f0 = col.agg_funcs[0];
    auto ai = col.agg_input_cols[0];
    if (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) input.data[ai].Flatten(n);

    auto ptype = input.data[pki].GetType().InternalType();
    auto kmin = sink.key_min;
    auto seg_shift = sink.segmented_shift;
    auto seg_mask = sink.segmented_mask;
    auto range = sink.key_range;
    bool pkfk = sink.all_bc_one;
    auto *active_bits = sink.segmented_active_bits.data();
    auto mark_active = [&](idx_t key_offset) {
        auto word = key_offset >> 6;
        auto bit = 1ULL << (key_offset & 63);
        if (!(active_bits[word] & bit)) {
            active_bits[word] |= bit;
            sink.segmented_active_keys.push_back(key_offset);
        }
    };

#define SEGMENTED_KEY_LOOP(KTYPE, BODY)                                                                  \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= range) continue;                                                                    \
            auto seg = k >> seg_shift;                                                                   \
            auto local = k & seg_mask;                                                                   \
            auto &count_seg = sink.segmented_build_counts[seg];                                          \
            if (count_seg.empty()) continue;                                                             \
            auto bcount = count_seg[local];                                                              \
            if (!bcount) continue;                                                                       \
            mark_active(k);                                                                              \
            BODY                                                                                         \
        }                                                                                                \
    }
    if (f0 == "COUNT" && ai == DConstants::INVALID_INDEX) {
        switch (ptype) {
        case PhysicalType::INT8: SEGMENTED_KEY_LOOP(int8_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::INT16: SEGMENTED_KEY_LOOP(int16_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::INT32: SEGMENTED_KEY_LOOP(int32_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::INT64: SEGMENTED_KEY_LOOP(int64_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::UINT8: SEGMENTED_KEY_LOOP(uint8_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::UINT16: SEGMENTED_KEY_LOOP(uint16_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::UINT32: SEGMENTED_KEY_LOOP(uint32_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        case PhysicalType::UINT64: SEGMENTED_KEY_LOOP(uint64_t, sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;); break;
        default: break;
        }
#undef SEGMENTED_KEY_LOOP
        chunk.SetCardinality(0);
        return true;
    }

    auto *validity = (ai != DConstants::INVALID_INDEX && ai < input.ColumnCount()) ?
                         FlatVector::Validity(input.data[ai]).GetData() :
                         nullptr;
    if (f0 == "COUNT") {
#define SEGMENTED_COUNT_COL_LOOP(KTYPE)                                                                  \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= range) continue;                                                                    \
            auto seg = k >> seg_shift;                                                                   \
            auto local = k & seg_mask;                                                                   \
            auto &count_seg = sink.segmented_build_counts[seg];                                          \
            if (count_seg.empty()) continue;                                                             \
            auto bcount = count_seg[local];                                                              \
            if (!bcount) continue;                                                                       \
            if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                            \
            mark_active(k);                                                                              \
            sink.segmented_sums[seg][local] += pkfk ? 1.0 : (double)bcount;                             \
        }                                                                                                \
    }
        switch (ptype) {
        case PhysicalType::INT8: SEGMENTED_COUNT_COL_LOOP(int8_t); break;
        case PhysicalType::INT16: SEGMENTED_COUNT_COL_LOOP(int16_t); break;
        case PhysicalType::INT32: SEGMENTED_COUNT_COL_LOOP(int32_t); break;
        case PhysicalType::INT64: SEGMENTED_COUNT_COL_LOOP(int64_t); break;
        case PhysicalType::UINT8: SEGMENTED_COUNT_COL_LOOP(uint8_t); break;
        case PhysicalType::UINT16: SEGMENTED_COUNT_COL_LOOP(uint16_t); break;
        case PhysicalType::UINT32: SEGMENTED_COUNT_COL_LOOP(uint32_t); break;
        case PhysicalType::UINT64: SEGMENTED_COUNT_COL_LOOP(uint64_t); break;
        default: break;
        }
#undef SEGMENTED_COUNT_COL_LOOP
        chunk.SetCardinality(0);
        return true;
    }

    auto vtype = input.data[ai].GetType().InternalType();
#define SEGMENTED_NUMERIC_LOOP(KTYPE, VTYPE)                                                             \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]);                                         \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= range) continue;                                                                    \
            auto seg = k >> seg_shift;                                                                   \
            auto local = k & seg_mask;                                                                   \
            auto &count_seg = sink.segmented_build_counts[seg];                                          \
            if (count_seg.empty()) continue;                                                             \
            auto bcount = count_seg[local];                                                              \
            if (!bcount) continue;                                                                       \
            if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                            \
            mark_active(k);                                                                              \
            sink.segmented_sums[seg][local] += (double)vals[r] * (pkfk ? 1.0 : (double)bcount);         \
            if (f0 == "AVG") sink.segmented_avg_counts[seg][local] += pkfk ? 1.0 : (double)bcount;     \
        }                                                                                                \
    }
    bool ran_typed = true;
#define SEGMENTED_NUMERIC_KEY_SWITCH(VTYPE)                                                              \
    switch (ptype) {                                                                                     \
    case PhysicalType::INT8: SEGMENTED_NUMERIC_LOOP(int8_t, VTYPE); break;                              \
    case PhysicalType::INT16: SEGMENTED_NUMERIC_LOOP(int16_t, VTYPE); break;                            \
    case PhysicalType::INT32: SEGMENTED_NUMERIC_LOOP(int32_t, VTYPE); break;                            \
    case PhysicalType::INT64: SEGMENTED_NUMERIC_LOOP(int64_t, VTYPE); break;                            \
    case PhysicalType::UINT8: SEGMENTED_NUMERIC_LOOP(uint8_t, VTYPE); break;                            \
    case PhysicalType::UINT16: SEGMENTED_NUMERIC_LOOP(uint16_t, VTYPE); break;                          \
    case PhysicalType::UINT32: SEGMENTED_NUMERIC_LOOP(uint32_t, VTYPE); break;                          \
    case PhysicalType::UINT64: SEGMENTED_NUMERIC_LOOP(uint64_t, VTYPE); break;                          \
    default: ran_typed = false; break;                                                                   \
    }
    switch (vtype) {
    case PhysicalType::DOUBLE: SEGMENTED_NUMERIC_KEY_SWITCH(double); break;
    case PhysicalType::FLOAT: SEGMENTED_NUMERIC_KEY_SWITCH(float); break;
    case PhysicalType::INT64: SEGMENTED_NUMERIC_KEY_SWITCH(int64_t); break;
    case PhysicalType::INT32: SEGMENTED_NUMERIC_KEY_SWITCH(int32_t); break;
    case PhysicalType::INT16: SEGMENTED_NUMERIC_KEY_SWITCH(int16_t); break;
    case PhysicalType::INT8: SEGMENTED_NUMERIC_KEY_SWITCH(int8_t); break;
    case PhysicalType::UINT64: SEGMENTED_NUMERIC_KEY_SWITCH(uint64_t); break;
    case PhysicalType::UINT32: SEGMENTED_NUMERIC_KEY_SWITCH(uint32_t); break;
    case PhysicalType::UINT16: SEGMENTED_NUMERIC_KEY_SWITCH(uint16_t); break;
    case PhysicalType::UINT8: SEGMENTED_NUMERIC_KEY_SWITCH(uint8_t); break;
    default: ran_typed = false; break;
    }
#undef SEGMENTED_NUMERIC_KEY_SWITCH
#undef SEGMENTED_NUMERIC_LOOP
    if (!ran_typed) {
        chunk.SetCardinality(0);
        return true;
    }
    chunk.SetCardinality(0);
    return true;
}

} // namespace duckdb
