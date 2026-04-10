#include "aggjoin_source_internal.hpp"

namespace duckdb {

OperatorResultType ExecuteResultHashSourcePath(const PhysicalAggJoin &op, ExecutionContext &ctx, DataChunk &input,
                                               DataChunk &chunk, AggJoinSinkState &sink, idx_t n, idx_t na,
                                               bool same_keys, hash_t *h) {
    auto &col = op.col;
    hash_t *gh;
    Vector ghv(LogicalType::HASH, n);
    bool ungrouped = col.group_cols.empty();
    if (ungrouped) {
        ghv.Flatten(n);
        gh = FlatVector::GetData<hash_t>(ghv);
        for (idx_t r = 0; r < n; r++) gh[r] = 0;
    } else if (same_keys) {
        gh = h;
    } else {
        ghv.Flatten(n);
        VectorOperations::Hash(input.data[col.group_cols[0]], ghv, n);
        for (idx_t i = 1; i < col.group_cols.size(); i++) {
            VectorOperations::CombineHash(ghv, input.data[col.group_cols[i]], n);
        }
        gh = FlatVector::GetData<hash_t>(ghv);
        ApplyAggJoinTestHashBits(gh, n);
    }

    auto &rht = sink.result_ht;
    if (!rht.mask) {
        idx_t est_groups = 0;
        if (ungrouped) {
            est_groups = 1;
        } else if (same_keys) {
            est_groups = sink.build_ht.count;
        } else {
            idx_t multiplier = (col.group_cols.size() <= 2) ? 2 : 4;
            est_groups = sink.build_ht.count;
            if (est_groups > std::numeric_limits<idx_t>::max() / multiplier) {
                est_groups = std::numeric_limits<idx_t>::max();
            } else {
                est_groups *= multiplier;
            }
        }
        if (est_groups < 4096) est_groups = 4096;
        idx_t ng = col.group_cols.size();
        vector<bool> g_is_int(ng, true);
        vector<bool> g_use_value(ng, false);
        for (idx_t g = 0; g < ng; g++) {
            auto gt = input.data[col.group_cols[g]].GetType().InternalType();
            g_use_value[g] = NeedsValueGroupStorage(gt);
            g_is_int[g] = !g_use_value[g] && (gt != PhysicalType::DOUBLE && gt != PhysicalType::FLOAT);
        }
        vector<bool> val_mm(na, false);
        for (idx_t a = 0; a < na; a++) {
            auto &f = col.agg_funcs[a];
            // Non-numeric MIN/MAX stays on the hash path only for the narrow cases
            // that already survived planner rewrites; those cases use Value storage
            // intentionally to preserve exact comparison semantics.
            if ((f == "MIN" || f == "MAX") && a < col.agg_is_numeric.size() && !col.agg_is_numeric[a]) {
                val_mm[a] = true;
            }
        }
        rht.Init(est_groups, na, ng, &g_is_int, &val_mm, &g_use_value);
    }

    sink.probe_bc.resize(n);
    sink.probe_slots.resize(n);
    sink.probe_build_ptrs.resize(n);
    std::fill(sink.probe_bc.begin(), sink.probe_bc.end(), 0.0);
    std::fill(sink.probe_slots.begin(), sink.probe_slots.end(), DConstants::INVALID_INDEX);
    std::fill(sink.probe_build_ptrs.begin(), sink.probe_build_ptrs.end(), nullptr);
    auto &row_bc = sink.probe_bc;
    auto &row_slots = sink.probe_slots;
    auto &row_builds = sink.probe_build_ptrs;
    idx_t match_count = 0;

    int64_t *prefilter_keys = nullptr;
    if (sink.has_range_prefilter && col.probe_key_cols.size() == 1) {
        auto pki = col.probe_key_cols[0];
        auto pt = input.data[pki].GetType().InternalType();
        sink.prefilter_keys_buf.resize(n);
        prefilter_keys = sink.prefilter_keys_buf.data();
        switch (pt) {
        case PhysicalType::INT8: {
            auto *k = FlatVector::GetData<int8_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = k[r];
            break;
        }
        case PhysicalType::INT16: {
            auto *k = FlatVector::GetData<int16_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = k[r];
            break;
        }
        case PhysicalType::INT32: {
            auto *k = FlatVector::GetData<int32_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = k[r];
            break;
        }
        case PhysicalType::INT64: {
            auto *k = FlatVector::GetData<int64_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = k[r];
            break;
        }
        case PhysicalType::UINT8: {
            auto *k = FlatVector::GetData<uint8_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = (int64_t)k[r];
            break;
        }
        case PhysicalType::UINT16: {
            auto *k = FlatVector::GetData<uint16_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = (int64_t)k[r];
            break;
        }
        case PhysicalType::UINT32: {
            auto *k = FlatVector::GetData<uint32_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = (int64_t)k[r];
            break;
        }
        case PhysicalType::UINT64: {
            auto *k = FlatVector::GetData<uint64_t>(input.data[pki]);
            for (idx_t r = 0; r < n; r++) prefilter_keys[r] = (int64_t)k[r];
            break;
        }
        default:
            prefilter_keys = nullptr;
            break;
        }
    }

    auto *bf = sink.bloom_filter.empty() ? nullptr : sink.bloom_filter.data();
    auto bf_mask = sink.bloom_mask;

    for (idx_t r = 0; r < n; r++) {
        if (prefilter_keys && (prefilter_keys[r] < sink.build_key_min || prefilter_keys[r] > sink.build_key_max)) {
            sink.range_prefilter_skips++;
            continue;
        }
        if (bf) {
            auto bh1 = h[r];
            auto bh2 = (bh1 >> 17) | (bh1 << 47);
            if (!(bf[(bh1 & bf_mask) / 64] & (1ULL << ((bh1 & bf_mask) % 64))) ||
                !(bf[(bh2 & bf_mask) / 64] & (1ULL << ((bh2 & bf_mask) % 64)))) {
                sink.bloom_prefilter_skips++;
                continue;
            }
        }
        if (r + 4 < n) sink.build_ht.Prefetch(h[r + 4]);
        auto *build = sink.build_ht.Find(h[r], [&](const BuildEntry &b) {
            return BuildEntryMatches(b, input, col.probe_key_cols, r);
        });
        if (!build) continue;
        bool has_null = false;
        for (auto ki : col.probe_key_cols) {
            if (FlatVector::IsNull(input.data[ki], r)) {
                has_null = true;
                break;
            }
        }
        if (has_null) continue;
        row_bc[r] = (double)build->count;
        row_builds[r] = build;
        auto slot = rht.FindOrCreate(
            gh[r], [&](idx_t existing_slot) { return ungrouped || ResultSlotGroupMatches(rht, existing_slot, input, col, r); },
            [&](idx_t new_slot) {
                rht.SetGroupInit(new_slot);
                for (idx_t g = 0; g < col.group_cols.size(); g++) {
                    auto gi = col.group_cols[g];
                    auto gt = input.data[gi].GetType().InternalType();
                    if (rht.GroupUsesValue(g)) {
                        rht.GroupVal(g, new_slot) = input.data[gi].GetValue(r);
                        continue;
                    }
                    switch (gt) {
                    case PhysicalType::INT32: rht.GroupInt(g, new_slot) = FlatVector::GetData<int32_t>(input.data[gi])[r]; break;
                    case PhysicalType::INT64: rht.GroupInt(g, new_slot) = FlatVector::GetData<int64_t>(input.data[gi])[r]; break;
                    case PhysicalType::INT16: rht.GroupInt(g, new_slot) = FlatVector::GetData<int16_t>(input.data[gi])[r]; break;
                    case PhysicalType::INT8: rht.GroupInt(g, new_slot) = FlatVector::GetData<int8_t>(input.data[gi])[r]; break;
                    case PhysicalType::UINT32: rht.GroupInt(g, new_slot) = FlatVector::GetData<uint32_t>(input.data[gi])[r]; break;
                    case PhysicalType::UINT16: rht.GroupInt(g, new_slot) = FlatVector::GetData<uint16_t>(input.data[gi])[r]; break;
                    case PhysicalType::UINT8: rht.GroupInt(g, new_slot) = FlatVector::GetData<uint8_t>(input.data[gi])[r]; break;
                    case PhysicalType::DOUBLE: rht.GroupDbl(g, new_slot) = FlatVector::GetData<double>(input.data[gi])[r]; break;
                    case PhysicalType::FLOAT: rht.GroupDbl(g, new_slot) = FlatVector::GetData<float>(input.data[gi])[r]; break;
                    default: rht.GroupVal(g, new_slot) = input.data[gi].GetValue(r); break;
                    }
                }
            });
        row_slots[r] = slot;
        match_count++;
    }
    sink.hash_match_rows += match_count;
    if (!match_count) {
        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    if (sink.use_native_ht && sink.agg_ht) {
        DataChunk group_chunk;
        group_chunk.Initialize(ctx.client, op.group_types);
        DataChunk payload_chunk;
        payload_chunk.Initialize(ctx.client, op.payload_types);

        SelectionVector match_sel(n);
        idx_t mc = 0;
        for (idx_t r = 0; r < n; r++) {
            if (row_bc[r] > 0) match_sel.set_index(mc++, r);
        }

        for (idx_t g = 0; g < col.group_cols.size(); g++) {
            auto &src_vec = input.data[col.group_cols[g]];
            auto &dst_type = op.group_types[g];
            if (src_vec.GetType() == dst_type) {
                group_chunk.data[g].Reference(src_vec);
                group_chunk.data[g].Slice(match_sel, mc);
            } else {
                Vector sliced(src_vec.GetType(), mc);
                VectorOperations::Copy(src_vec, sliced, match_sel, mc, 0, 0);
                VectorOperations::Cast(ctx.client, sliced, group_chunk.data[g], mc);
            }
        }
        group_chunk.SetCardinality(mc);

        for (idx_t a = 0; a < na; a++) {
            auto ai = col.agg_input_cols[a];
            if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
                auto *dst = FlatVector::GetData<int64_t>(payload_chunk.data[a]);
                for (idx_t m = 0; m < mc; m++) {
                    dst[m] = (int64_t)row_bc[match_sel.get_index(m)];
                }
                continue;
            }
            auto ptype = input.data[ai].GetType().InternalType();
            if (ptype == PhysicalType::DOUBLE) {
                auto *src = FlatVector::GetData<double>(input.data[ai]);
                auto *dst = FlatVector::GetData<double>(payload_chunk.data[a]);
                for (idx_t m = 0; m < mc; m++) {
                    auto r = match_sel.get_index(m);
                    dst[m] = src[r] * row_bc[r];
                }
            } else {
                auto *dst = FlatVector::GetData<double>(payload_chunk.data[a]);
#define NATIVE_PAYLOAD_LOOP(TYPE)                                                                        \
    {                                                                                                    \
        auto *src = FlatVector::GetData<TYPE>(input.data[ai]);                                           \
        for (idx_t m = 0; m < mc; m++) {                                                                 \
            auto r = match_sel.get_index(m);                                                             \
            dst[m] = (double)src[r] * row_bc[r];                                                         \
        }                                                                                                \
    }
                switch (ptype) {
                case PhysicalType::FLOAT: NATIVE_PAYLOAD_LOOP(float); break;
                case PhysicalType::INT64: NATIVE_PAYLOAD_LOOP(int64_t); break;
                case PhysicalType::INT32: NATIVE_PAYLOAD_LOOP(int32_t); break;
                case PhysicalType::INT16: NATIVE_PAYLOAD_LOOP(int16_t); break;
                case PhysicalType::INT8: NATIVE_PAYLOAD_LOOP(int8_t); break;
                case PhysicalType::UINT64: NATIVE_PAYLOAD_LOOP(uint64_t); break;
                case PhysicalType::UINT32: NATIVE_PAYLOAD_LOOP(uint32_t); break;
                case PhysicalType::UINT16: NATIVE_PAYLOAD_LOOP(uint16_t); break;
                case PhysicalType::UINT8: NATIVE_PAYLOAD_LOOP(uint8_t); break;
                default:
                    for (idx_t m = 0; m < mc; m++) {
                        auto r = match_sel.get_index(m);
                        dst[m] = input.data[ai].GetValue(r).GetValue<double>() * row_bc[r];
                    }
                    break;
                }
#undef NATIVE_PAYLOAD_LOOP
            }
        }
        payload_chunk.SetCardinality(mc);

        Vector group_hashes(LogicalType::HASH, mc);
        group_hashes.Flatten(mc);
        auto *ghd = FlatVector::GetData<hash_t>(group_hashes);
        for (idx_t m = 0; m < mc; m++) ghd[m] = gh[match_sel.get_index(m)];

        unsafe_vector<idx_t> filter;
        for (idx_t a = 0; a < na; a++) filter.push_back(a);
        sink.agg_ht->AddChunk(group_chunk, group_hashes, payload_chunk, filter);

        chunk.SetCardinality(0);
        return OperatorResultType::NEED_MORE_INPUT;
    }

    for (idx_t a = 0; a < na; a++) {
        auto ai = col.agg_input_cols[a];
        auto &f = col.agg_funcs[a];
        bool on_build = (col.agg_on_build.size() > a && col.agg_on_build[a]);

        if (f == "COUNT" && ai == DConstants::INVALID_INDEX && !on_build) {
            auto *sl = row_slots.data();
            if (sink.all_bc_one) {
                for (idx_t r = 0; r < n; r++) {
                    if (sl[r] != DConstants::INVALID_INDEX) rht.Sum(sl[r], a) += 1.0;
                }
            } else {
                auto *bc = row_bc.data();
                for (idx_t r = 0; r < n; r++) {
                    if (sl[r] != DConstants::INVALID_INDEX) rht.Sum(sl[r], a) += bc[r];
                }
            }
            continue;
        }

        if (on_build) {
            auto *sl = row_slots.data();
            idx_t ba = 0;
            for (idx_t i = 0; i < a; i++) {
                if (col.agg_on_build.size() > i && col.agg_on_build[i]) ba++;
            }
            for (idx_t r = 0; r < n; r++) {
                if (row_slots[r] == DConstants::INVALID_INDEX) continue;
                auto *build = row_builds[r];
                if (!build) continue;
                auto &bav = build->bav;
                if (f == "SUM") {
                    rht.Sum(sl[r], a) += bav.agg_sum[ba];
                } else if (f == "AVG") {
                    rht.Sum(sl[r], a) += bav.agg_sum[ba];
                    rht.Count(sl[r], a) += bav.agg_count[ba];
                } else if (f == "MIN") {
                    if (bav.agg_init[ba]) {
                        auto bv = bav.agg_min[ba];
                        if (!rht.GetHas(sl[r], a) || bv < rht.Min(sl[r], a)) {
                            rht.Min(sl[r], a) = bv;
                            rht.SetHas(sl[r], a, true);
                        }
                    }
                } else if (f == "MAX") {
                    if (bav.agg_init[ba]) {
                        auto bv = bav.agg_max[ba];
                        if (!rht.GetHas(sl[r], a) || bv > rht.Max(sl[r], a)) {
                            rht.Max(sl[r], a) = bv;
                            rht.SetHas(sl[r], a, true);
                        }
                    }
                } else if (f == "COUNT") {
                    rht.Sum(sl[r], a) += bav.agg_count[ba];
                }
            }
            continue;
        }

        if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;

        auto ptype = input.data[ai].GetType().InternalType();
        auto *validity = FlatVector::Validity(input.data[ai]).GetData();
        auto *bc = row_bc.data();
        auto *sl = row_slots.data();

        if (f == "SUM" || f == "AVG" || f == "COUNT") {
            bool is_avg = (f == "AVG");
            bool pkfk = sink.all_bc_one;
#define SUM_LOOP(TYPE)                                                                                   \
    {                                                                                                    \
        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]);                                          \
        if (pkfk) {                                                                                      \
            for (idx_t r = 0; r < n; r++) {                                                              \
                if (sl[r] == DConstants::INVALID_INDEX) continue;                                        \
                if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                        \
                rht.Sum(sl[r], a) += (double)vals[r];                                                    \
                if (is_avg) rht.Count(sl[r], a) += 1.0;                                                  \
            }                                                                                            \
        } else {                                                                                         \
            for (idx_t r = 0; r < n; r++) {                                                              \
                if (sl[r] == DConstants::INVALID_INDEX) continue;                                        \
                if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                        \
                rht.Sum(sl[r], a) += (double)vals[r] * bc[r];                                            \
                if (is_avg) rht.Count(sl[r], a) += bc[r];                                                \
            }                                                                                            \
        }                                                                                                \
    }
            switch (ptype) {
            case PhysicalType::DOUBLE: SUM_LOOP(double); break;
            case PhysicalType::FLOAT: SUM_LOOP(float); break;
            case PhysicalType::INT64: SUM_LOOP(int64_t); break;
            case PhysicalType::INT32: SUM_LOOP(int32_t); break;
            case PhysicalType::INT16: SUM_LOOP(int16_t); break;
            case PhysicalType::INT8: SUM_LOOP(int8_t); break;
            case PhysicalType::UINT64: SUM_LOOP(uint64_t); break;
            case PhysicalType::UINT32: SUM_LOOP(uint32_t); break;
            case PhysicalType::UINT16: SUM_LOOP(uint16_t); break;
            case PhysicalType::UINT8: SUM_LOOP(uint8_t); break;
            default:
                for (idx_t r = 0; r < n; r++) {
                    if (sl[r] == DConstants::INVALID_INDEX) continue;
                    if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;
                    rht.Sum(sl[r], a) += input.data[ai].GetValue(r).GetValue<double>() * bc[r];
                    if (is_avg) rht.Count(sl[r], a) += bc[r];
                }
                break;
            }
#undef SUM_LOOP
        } else if (f == "MIN" || f == "MAX") {
            bool is_min = (f == "MIN");
            if (rht.UsesValMinMax(a)) {
                for (idx_t r = 0; r < n; r++) {
                    if (sl[r] == DConstants::INVALID_INDEX) continue;
                    if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;
                    auto val = input.data[ai].GetValue(r);
                    if (val.IsNull()) continue;
                    auto slot = sl[r];
                    if (!rht.GetHas(slot, a)) {
                        if (is_min) rht.ValMin(slot, a) = val;
                        else rht.ValMax(slot, a) = val;
                        rht.SetHas(slot, a, true);
                    } else {
                        if (is_min) {
                            if (val < rht.ValMin(slot, a)) rht.ValMin(slot, a) = val;
                        } else {
                            if (val > rht.ValMax(slot, a)) rht.ValMax(slot, a) = val;
                        }
                    }
                }
            } else {
#define MINMAX_HASH_LOOP(TYPE)                                                                           \
    {                                                                                                    \
        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]);                                          \
        for (idx_t r = 0; r < n; r++) {                                                                  \
            if (sl[r] == DConstants::INVALID_INDEX) continue;                                            \
            if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;                            \
            double dv = (double)vals[r];                                                                 \
            auto slot = sl[r];                                                                           \
            if (is_min) {                                                                                \
                if (!rht.GetHas(slot, a) || dv < rht.Min(slot, a)) {                                     \
                    rht.Min(slot, a) = dv;                                                               \
                    rht.SetHas(slot, a, true);                                                           \
                }                                                                                        \
            } else {                                                                                     \
                if (!rht.GetHas(slot, a) || dv > rht.Max(slot, a)) {                                     \
                    rht.Max(slot, a) = dv;                                                               \
                    rht.SetHas(slot, a, true);                                                           \
                }                                                                                        \
            }                                                                                            \
        }                                                                                                \
    }
                switch (ptype) {
                case PhysicalType::DOUBLE: MINMAX_HASH_LOOP(double); break;
                case PhysicalType::FLOAT: MINMAX_HASH_LOOP(float); break;
                case PhysicalType::INT64: MINMAX_HASH_LOOP(int64_t); break;
                case PhysicalType::INT32: MINMAX_HASH_LOOP(int32_t); break;
                case PhysicalType::INT16: MINMAX_HASH_LOOP(int16_t); break;
                case PhysicalType::INT8: MINMAX_HASH_LOOP(int8_t); break;
                case PhysicalType::UINT64: MINMAX_HASH_LOOP(uint64_t); break;
                case PhysicalType::UINT32: MINMAX_HASH_LOOP(uint32_t); break;
                case PhysicalType::UINT16: MINMAX_HASH_LOOP(uint16_t); break;
                case PhysicalType::UINT8: MINMAX_HASH_LOOP(uint8_t); break;
                default:
                    for (idx_t r = 0; r < n; r++) {
                        if (sl[r] == DConstants::INVALID_INDEX) continue;
                        if (validity && !((validity[r / 64] >> (r % 64)) & 1)) continue;
                        auto dv = input.data[ai].GetValue(r).GetValue<double>();
                        auto slot = sl[r];
                        if (is_min) {
                            if (!rht.GetHas(slot, a) || dv < rht.Min(slot, a)) {
                                rht.Min(slot, a) = dv;
                                rht.SetHas(slot, a, true);
                            }
                        } else {
                            if (!rht.GetHas(slot, a) || dv > rht.Max(slot, a)) {
                                rht.Max(slot, a) = dv;
                                rht.SetHas(slot, a, true);
                            }
                        }
                    }
                    break;
                }
#undef MINMAX_HASH_LOOP
            }
        }
    }
    chunk.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
