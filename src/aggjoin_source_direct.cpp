#include "aggjoin_source_internal.hpp"

namespace duckdb {

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
        if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;
        if (f == "MIN" || f == "MAX") continue;
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
        if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
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
    auto *has_arr = has_minmax ? sink.direct_has.data() : nullptr;

    auto is_int_key = (ptype == PhysicalType::INT32 || ptype == PhysicalType::INT64 ||
                       ptype == PhysicalType::UINT32 || ptype == PhysicalType::UINT64 ||
                       ptype == PhysicalType::UINT16 || ptype == PhysicalType::UINT8);
    if (col.group_cols.empty() && all_sum_count_double && is_int_key && !sink.ungrouped_sum.empty()) {
#define UNGROUPED_EXTRACT(KTYPE)                                                                         \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
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
                        if (!v || ((v[r / 64] >> (r % 64)) & 1)) sink.ungrouped_sum[a] += slot.vals[r] * bcount; \
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
        case PhysicalType::INT32: UNGROUPED_EXTRACT(int32_t); break;
        case PhysicalType::INT64: UNGROUPED_EXTRACT(int64_t); break;
        case PhysicalType::UINT32: UNGROUPED_EXTRACT(uint32_t); break;
        case PhysicalType::UINT64: UNGROUPED_EXTRACT(uint64_t); break;
        case PhysicalType::UINT16: UNGROUPED_EXTRACT(uint16_t); break;
        case PhysicalType::UINT8: UNGROUPED_EXTRACT(uint8_t); break;
        default: break;
        }
#undef UNGROUPED_EXTRACT

        if (!sink.direct_build_sums.empty()) {
            idx_t ba = 0;
            for (idx_t a = 0; a < na; a++) {
                if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                auto &f = col.agg_funcs[a];
                auto *bsums = sink.direct_build_sums.data() + ba * krange;
                auto *bcnts = sink.direct_build_counts.data() + ba * krange;
                auto *bhas = sink.direct_build_has.empty() ? nullptr : sink.direct_build_has.data() + ba * krange;
#define UNGROUPED_BUILD_AGG(KTYPE)                                                                       \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (f == "SUM") sink.ungrouped_sum[a] += bsums[k];                                           \
            else if (f == "AVG") {                                                                       \
                sink.ungrouped_sum[a] += bsums[k];                                                       \
                sink.ungrouped_count[a] += bcnts[k];                                                     \
            } else if (f == "COUNT") sink.ungrouped_sum[a] += bcnts[k];                                  \
            else if (f == "MIN" && bhas && bhas[k]) {                                                    \
                auto bv = sink.direct_build_mins[ba * krange + k];                                       \
                if (!sink.ungrouped_has[a] || bv < sink.ungrouped_min[a]) {                              \
                    sink.ungrouped_min[a] = bv;                                                          \
                    sink.ungrouped_has[a] = 1;                                                           \
                }                                                                                        \
            } else if (f == "MAX" && bhas && bhas[k]) {                                                  \
                auto bv = sink.direct_build_maxs[ba * krange + k];                                       \
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
                auto k = (idx_t)((int64_t)keys[r] - kmin);                                               \
                key_buf[r] = k;                                                                          \
                if (track_active_keys && k < krange && !seen_keys[k]) {                                  \
                    seen_keys[k] = 1;                                                                    \
                    sink.direct_active_keys.push_back(k);                                                \
                }                                                                                        \
            }                                                                                            \
        } else {                                                                                         \
            for (idx_t r = 0; r < n; r++) {                                                              \
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
            double *agg_sums = sums + a * krange;
            switch (slot.kind) {
            case AggSlot::COUNT_STAR:
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
                if (pkfk) {
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (k >= krange) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[k] += slot.vals[r];
                    }
                } else {
                    for (idx_t r = 0; r < n; r++) {
                        if (bc_buf[r] == 0.0) continue;
                        if (v && !((v[r / 64] >> (r % 64)) & 1)) continue;
                        agg_sums[key_buf[r]] += slot.vals[r] * bc_buf[r];
                    }
                }
                break;
            }
            case AggSlot::AVG_VAL: {
                double *agg_counts = avg_counts + a * krange;
                auto *v = slot.validity;
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
                double *agg_mins = mins + a * krange;
                uint8_t *agg_has = has_arr + a * krange;
                auto *v = slot.validity;
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
                double *agg_maxs = maxs + a * krange;
                uint8_t *agg_has = has_arr + a * krange;
                auto *v = slot.validity;
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

        if (!sink.direct_build_sums.empty()) {
            idx_t ba = 0;
            for (idx_t a = 0; a < na; a++) {
                if (!(col.agg_on_build.size() > a && col.agg_on_build[a])) continue;
                auto &f = col.agg_funcs[a];
                double *agg_sums_a = sums + a * krange;
                if (f == "SUM") {
                    double *bsums = sink.direct_build_sums.data() + ba * krange;
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        agg_sums_a[k] += bsums[k];
                    }
                } else if (f == "MIN" && mins) {
                    double *bmins = sink.direct_build_mins.data() + ba * krange;
                    uint8_t *bhas = sink.direct_build_has.data() + ba * krange;
                    uint8_t *agg_has = has_arr + a * krange;
                    double *agg_mins = mins + a * krange;
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        if (bhas[k] && (!agg_has[k] || bmins[k] < agg_mins[k])) {
                            agg_mins[k] = bmins[k];
                            agg_has[k] = 1;
                        }
                    }
                } else if (f == "MAX" && maxs) {
                    double *bmaxs = sink.direct_build_maxs.data() + ba * krange;
                    uint8_t *bhas = sink.direct_build_has.data() + ba * krange;
                    uint8_t *agg_has = has_arr + a * krange;
                    double *agg_maxs = maxs + a * krange;
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        if (bhas[k] && (!agg_has[k] || bmaxs[k] > agg_maxs[k])) {
                            agg_maxs[k] = bmaxs[k];
                            agg_has[k] = 1;
                        }
                    }
                } else if (f == "COUNT") {
                    double *bcnts = sink.direct_build_counts.data() + ba * krange;
                    for (idx_t r = 0; r < n; r++) {
                        auto k = key_buf[r];
                        if (pkfk ? (k >= krange) : (bc_buf[r] == 0.0)) continue;
                        agg_sums_a[k] += bcnts[k];
                    }
                } else if (f == "AVG" && avg_counts) {
                    double *bsums = sink.direct_build_sums.data() + ba * krange;
                    double *bcnts = sink.direct_build_counts.data() + ba * krange;
                    double *agg_counts_a = avg_counts + a * krange;
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
        for (idx_t a = 0; a < na; a++) {
            auto ai = col.agg_input_cols[a];
            auto &f = col.agg_funcs[a];

            if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
#define DIRECT_COUNT_LOOP(TYPE)                                                                          \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<TYPE>(input.data[pki]);                                         \
        double *agg_s = sums + a * krange;                                                               \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k < krange) agg_s[k] += (double)bc[k];                                                   \
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

            bool is_build_agg = (col.agg_on_build.size() > a && col.agg_on_build[a]);
            if (is_build_agg && !sink.direct_build_sums.empty()) {
                idx_t ba = 0;
                for (idx_t i = 0; i < a; i++) {
                    if (col.agg_on_build.size() > i && col.agg_on_build[i]) ba++;
                }
                double *bsums = sink.direct_build_sums.data() + ba * krange;
                double *agg_s = sums + a * krange;
                auto *build_has = sink.direct_build_has.empty() ? nullptr : sink.direct_build_has.data() + ba * krange;
#define DIRECT_BUILD_AGG_LOOP(KTYPE)                                                                     \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange || !bc[k]) continue;                                                         \
            if (f == "SUM" || f == "AVG") agg_s[k] += bsums[k];                                          \
            else if (f == "COUNT") agg_s[k] += sink.direct_build_counts[ba * krange + k];               \
            else if (f == "MIN" && has_minmax && build_has) {                                            \
                if (!build_has[k]) continue;                                                             \
                auto bv = sink.direct_build_mins[ba * krange + k];                                       \
                auto s = a * krange + k;                                                                 \
                if (!has_arr[s] || bv < mins[s]) { mins[s] = bv; has_arr[s] = 1; }                      \
            } else if (f == "MAX" && has_minmax && build_has) {                                          \
                if (!build_has[k]) continue;                                                             \
                auto bv = sink.direct_build_maxs[ba * krange + k];                                       \
                auto s = a * krange + k;                                                                 \
                if (!has_arr[s] || bv > maxs[s]) { maxs[s] = bv; has_arr[s] = 1; }                      \
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
                        if (f == "SUM" || f == "AVG") agg_s[k] += bsums[k];
                        else if (f == "COUNT") agg_s[k] += sink.direct_build_counts[ba * krange + k];
                        else if (f == "MIN" && has_minmax && build_has) {
                            if (!build_has[k]) continue;
                            auto bv = sink.direct_build_mins[ba * krange + k];
                            auto s = a * krange + k;
                            if (!has_arr[s] || bv < mins[s]) { mins[s] = bv; has_arr[s] = 1; }
                        } else if (f == "MAX" && has_minmax && build_has) {
                            if (!build_has[k]) continue;
                            auto bv = sink.direct_build_maxs[ba * krange + k];
                            auto s = a * krange + k;
                            if (!has_arr[s] || bv > maxs[s]) { maxs[s] = bv; has_arr[s] = 1; }
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
                auto *dcounts = is_avg ? sink.direct_counts.data() : nullptr;
#define DIRECT_SUM_LOOP(KTYPE, VTYPE)                                                                    \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]);                                         \
        double *agg_s = sums + a * krange;                                                               \
        double *agg_c = dcounts ? dcounts + a * krange : nullptr;                                        \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            auto k = (idx_t)((int64_t)keys[r] - kmin);                                                   \
            if (k >= krange) continue;                                                                   \
            if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                            \
            agg_s[k] += (double)vals[r] * (double)bc[k];                                                 \
            if (agg_c) agg_c[k] += (double)bc[k];                                                        \
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
                    double *agg_s = sums + a * krange;
                    double *agg_c = dcounts ? dcounts + a * krange : nullptr;
                    for (idx_t r = 0; r < n; r++) {
                        auto kv = input.data[pki].GetValue(r);
                        if (kv.IsNull()) continue;
                        auto k = (idx_t)(kv.GetValue<int64_t>() - kmin);
                        if (k >= krange || !bc[k]) continue;
                        auto v = input.data[ai].GetValue(r);
                        if (v.IsNull()) continue;
                        agg_s[k] += v.GetValue<double>() * (double)bc[k];
                        if (agg_c) agg_c[k] += (double)bc[k];
                    }
                }
#undef DIRECT_SUM_LOOP
            } else if (f == "MIN" || f == "MAX") {
                bool is_min = (f == "MIN");
                double *agg_m = is_min ? (mins + a * krange) : (maxs + a * krange);
                uint8_t *agg_h = has_arr + a * krange;
                auto vtype = input.data[ai].GetType().InternalType();
                auto *mm_validity = FlatVector::Validity(input.data[ai]).GetData();
#define DIRECT_MINMAX_LOOP(KTYPE, VTYPE)                                                                 \
    {                                                                                                    \
        auto *keys = FlatVector::GetData<KTYPE>(input.data[pki]);                                        \
        auto *vals = FlatVector::GetData<VTYPE>(input.data[ai]);                                         \
        for (idx_t r = 0; r < n; r++) {                                                                  \
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
