#include "aggjoin_source_internal.hpp"

namespace duckdb {
unique_ptr<GlobalSourceState> PhysicalAggJoin::GetGlobalSourceState(ClientContext &ctx) const {
    return make_uniq<AggJoinSourceState>();
}
OperatorResultType PhysicalAggJoin::ExecuteInternal(ExecutionContext &ctx, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state) const {
        if (!sink_state) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }
        auto &sink = sink_state->Cast<AggJoinSinkState>();
        if (!sink.finalized || sink.build_ht.mask == 0) {
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }
        auto n = input.size(); if (!n) return OperatorResultType::NEED_MORE_INPUT;
        auto na = col.agg_funcs.size();
        sink.probe_rows_seen += n;

        if (TryExecuteDirectSourcePath(*this, input, chunk, sink, n, na)) {
            return OperatorResultType::NEED_MORE_INPUT;
        }

        // ═══════════════════════════════════════════════════
        // HASH MODE: fallback for non-integer or large-range keys
        // ═══════════════════════════════════════════════════

        // Flatten probe key + group + agg cols
        for (auto i : col.probe_key_cols) input.data[i].Flatten(n);
        for (auto i : col.group_cols) if (i < input.ColumnCount()) input.data[i].Flatten(n);
        for (auto i : col.agg_input_cols) if (i!=DConstants::INVALID_INDEX && i<input.ColumnCount()) input.data[i].Flatten(n);

        // Hash group cols for result HT key
        // Optimization: if group_cols == probe_key_cols, reuse probe hash
        bool same_keys = (col.group_cols.size() == col.probe_key_cols.size());
        if (same_keys) {
            for (idx_t i = 0; i < col.group_cols.size(); i++) {
                if (col.group_cols[i] != col.probe_key_cols[i]) { same_keys = false; break; }
            }
        }
        auto single_probe_key_type = input.data[col.probe_key_cols[0]].GetType().InternalType();
        if (sink.build_slot_hash_mode && same_keys && sink.build_string_lookup_mode &&
            col.probe_key_cols.size() == 1 && single_probe_key_type == PhysicalType::VARCHAR) {
            auto pki = col.probe_key_cols[0];
            auto *keys = FlatVector::GetData<string_t>(input.data[pki]);
            sink.probe_bc.resize(n);
            sink.probe_slots.resize(n);
            std::fill(sink.probe_bc.begin(), sink.probe_bc.end(), 0.0);
            std::fill(sink.probe_slots.begin(), sink.probe_slots.end(), DConstants::INVALID_INDEX);
            auto &row_bc = sink.probe_bc;
            auto &row_slots = sink.probe_slots;
            idx_t match_count = 0;
            for (idx_t r = 0; r < n; r++) {
                if (FlatVector::IsNull(input.data[pki], r)) {
                    continue;
                }
                auto it = sink.build_string_slots.find(keys[r]);
                if (it == sink.build_string_slots.end()) {
                    continue;
                }
                auto slot = it->second;
                row_slots[r] = slot;
                row_bc[r] = (double)sink.build_ht.buckets[slot].count;
                sink.build_slot_seen[slot] = 1;
                match_count++;
            }
            sink.hash_match_rows += match_count;
            if (!match_count) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }

            bool all_fused_double = true;
            for (idx_t a = 0; a < na; a++) {
                auto &fn = col.agg_funcs[a];
                if (fn == "COUNT") continue;
                if (fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX") {
                    if (a >= col.agg_is_numeric.size() || !col.agg_is_numeric[a]) {
                        all_fused_double = false;
                        break;
                    }
                } else {
                    all_fused_double = false;
                    break;
                }
            }

            if (all_fused_double) {
                auto cap = sink.build_ht.mask + 1;
                auto *sl = sink.probe_slots.data();
                auto *bc = row_bc.data();
                bool bc_all_one = sink.all_bc_one;
                for (idx_t a = 0; a < na; a++) {
                    auto &fn = col.agg_funcs[a];
                    if (fn == "COUNT") {
                        if (bc_all_one) {
                            for (idx_t r = 0; r < n; r++) {
                                if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * cap + sl[r]] += 1.0;
                            }
                        } else {
                            for (idx_t r = 0; r < n; r++) {
                                if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * cap + sl[r]] += bc[r];
                            }
                        }
                        continue;
                    }
                    auto ai = col.agg_input_cols[a];
                    auto ptype = input.data[ai].GetType().InternalType();
                    bool is_sum = (fn == "SUM");
                    bool is_avg = (fn == "AVG");
                    bool is_min = (fn == "MIN");
                    bool is_max = (fn == "MAX");
                    #define BUILD_SLOT_FUSED_NUM(TYPE) \
                        do { \
                            auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                            if (is_min || is_max) { \
                                for (idx_t r = 0; r < n; r++) { \
                                    if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                    auto off = a * cap + sl[r]; \
                                    double v = (double)vals[r]; \
                                    if (!sink.build_slot_has[off]) { \
                                        sink.build_slot_mins[off] = sink.build_slot_maxs[off] = v; \
                                        sink.build_slot_has[off] = 1; \
                                    } else { \
                                        if (is_min && v < sink.build_slot_mins[off]) sink.build_slot_mins[off] = v; \
                                        if (is_max && v > sink.build_slot_maxs[off]) sink.build_slot_maxs[off] = v; \
                                    } \
                                } \
                            } else if (bc_all_one) { \
                                for (idx_t r = 0; r < n; r++) { \
                                    if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                    sink.build_slot_sums[a * cap + sl[r]] += (double)vals[r]; \
                                    if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += 1.0; \
                                } \
                            } else { \
                                for (idx_t r = 0; r < n; r++) { \
                                    if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                    sink.build_slot_sums[a * cap + sl[r]] += (double)vals[r] * bc[r]; \
                                    if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += bc[r]; \
                                } \
                            } \
                        } while (0)
                    switch (ptype) {
                    case PhysicalType::DOUBLE: BUILD_SLOT_FUSED_NUM(double); break;
                    case PhysicalType::FLOAT: BUILD_SLOT_FUSED_NUM(float); break;
                    case PhysicalType::INT64: BUILD_SLOT_FUSED_NUM(int64_t); break;
                    case PhysicalType::INT32: BUILD_SLOT_FUSED_NUM(int32_t); break;
                    case PhysicalType::INT16: BUILD_SLOT_FUSED_NUM(int16_t); break;
                    case PhysicalType::INT8: BUILD_SLOT_FUSED_NUM(int8_t); break;
                    case PhysicalType::UINT64: BUILD_SLOT_FUSED_NUM(uint64_t); break;
                    case PhysicalType::UINT32: BUILD_SLOT_FUSED_NUM(uint32_t); break;
                    case PhysicalType::UINT16: BUILD_SLOT_FUSED_NUM(uint16_t); break;
                    case PhysicalType::UINT8: BUILD_SLOT_FUSED_NUM(uint8_t); break;
                    default:
                        all_fused_double = false;
                        break;
                    }
                    #undef BUILD_SLOT_FUSED_NUM
                    if (!all_fused_double) break;
                }
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }
        }

        Vector hv(LogicalType::HASH, n); hv.Flatten(n);
        VectorOperations::Hash(input.data[col.probe_key_cols[0]], hv, n);
        for (idx_t i=1;i<col.probe_key_cols.size();i++) VectorOperations::CombineHash(hv, input.data[col.probe_key_cols[i]], n);
        auto h = FlatVector::GetData<hash_t>(hv);
        ApplyAggJoinTestHashBits(h, n);

        if (sink.build_slot_hash_mode && same_keys) {
            auto *bf = sink.bloom_filter.empty() ? nullptr : sink.bloom_filter.data();
            auto bf_mask = sink.bloom_mask;
            auto *build_base = sink.build_ht.buckets.data();
            sink.probe_bc.resize(n);
            sink.probe_slots.resize(n);
            std::fill(sink.probe_bc.begin(), sink.probe_bc.end(), 0.0);
            std::fill(sink.probe_slots.begin(), sink.probe_slots.end(), DConstants::INVALID_INDEX);
            auto &row_bc = sink.probe_bc;
            auto &row_slots = sink.probe_slots;
            idx_t match_count = 0;
            for (idx_t r = 0; r < n; r++) {
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
                    if (FlatVector::IsNull(input.data[ki], r)) { has_null = true; break; }
                }
                if (has_null) continue;
                auto slot = (idx_t)(build - build_base);
                row_slots[r] = slot;
                row_bc[r] = (double)build->count;
                sink.build_slot_seen[slot] = 1;
                match_count++;
            }
            sink.hash_match_rows += match_count;
            if (!match_count) { chunk.SetCardinality(0); return OperatorResultType::NEED_MORE_INPUT; }

            // Fused single-pass path for the common grouped single-key same-key
            // fallback: avoids rescanning the same probe rows once per aggregate.
            bool all_fused_double = true;
            struct BuildSlotAggSlot {
                enum Kind { SUM_VAL, AVG_VAL, COUNT_STAR, COUNT_COL, MIN_VAL, MAX_VAL, SKIP } kind;
                const double *vals = nullptr;
                const uint64_t *validity = nullptr;
            };
            vector<BuildSlotAggSlot> agg_slots(na);
            for (idx_t a = 0; a < na; a++) {
                auto &slot = agg_slots[a];
                auto ai = col.agg_input_cols[a];
                auto &f = col.agg_funcs[a];
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                    slot.kind = BuildSlotAggSlot::COUNT_STAR;
                    continue;
                }
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) {
                    slot.kind = BuildSlotAggSlot::SKIP;
                    continue;
                }
                slot.validity = FlatVector::Validity(input.data[ai]).GetData();
                if (f == "COUNT") {
                    slot.kind = BuildSlotAggSlot::COUNT_COL;
                    continue;
                }
                if (input.data[ai].GetType().InternalType() != PhysicalType::DOUBLE) {
                    all_fused_double = false;
                    break;
                }
                slot.vals = FlatVector::GetData<double>(input.data[ai]);
                if (f == "SUM") slot.kind = BuildSlotAggSlot::SUM_VAL;
                else if (f == "AVG") slot.kind = BuildSlotAggSlot::AVG_VAL;
                else if (f == "MIN") slot.kind = BuildSlotAggSlot::MIN_VAL;
                else if (f == "MAX") slot.kind = BuildSlotAggSlot::MAX_VAL;
                else {
                    all_fused_double = false;
                    break;
                }
            }
            if (all_fused_double) {
                auto pkfk = sink.all_bc_one;
                auto cap = sink.build_ht.mask + 1;
                auto *sl = row_slots.data();
                auto *bc = row_bc.data();
                auto *sums = sink.build_slot_sums.data();
                auto *avg_counts = sink.has_avg ? sink.build_slot_counts.data() : nullptr;
                auto *mins = sink.has_min_max ? sink.build_slot_mins.data() : nullptr;
                auto *maxs = sink.has_min_max ? sink.build_slot_maxs.data() : nullptr;
                auto *has = sink.has_min_max ? sink.build_slot_has.data() : nullptr;
                for (idx_t r = 0; r < n; r++) {
                    auto slot_idx = sl[r];
                    if (slot_idx == DConstants::INVALID_INDEX) continue;
                    auto mult = pkfk ? 1.0 : bc[r];
                    for (idx_t a = 0; a < na; a++) {
                        auto &slot = agg_slots[a];
                        switch (slot.kind) {
                        case BuildSlotAggSlot::COUNT_STAR:
                            sums[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::COUNT_COL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::SUM_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += slot.vals[r] * mult;
                            break;
                        case BuildSlotAggSlot::AVG_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            sums[a * cap + slot_idx] += slot.vals[r] * mult;
                            avg_counts[a * cap + slot_idx] += mult;
                            break;
                        case BuildSlotAggSlot::MIN_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            if (!has[a * cap + slot_idx] || slot.vals[r] < mins[a * cap + slot_idx]) {
                                mins[a * cap + slot_idx] = slot.vals[r];
                                has[a * cap + slot_idx] = 1;
                            }
                            break;
                        case BuildSlotAggSlot::MAX_VAL:
                            if (slot.validity && !((slot.validity[r / 64] >> (r % 64)) & 1)) break;
                            if (!has[a * cap + slot_idx] || slot.vals[r] > maxs[a * cap + slot_idx]) {
                                maxs[a * cap + slot_idx] = slot.vals[r];
                                has[a * cap + slot_idx] = 1;
                            }
                            break;
                        case BuildSlotAggSlot::SKIP:
                            break;
                        }
                    }
                }
                chunk.SetCardinality(0);
                return OperatorResultType::NEED_MORE_INPUT;
            }

            for (idx_t a = 0; a < na; a++) {
                auto ai = col.agg_input_cols[a];
                auto &f = col.agg_funcs[a];
                auto *sl = row_slots.data();
                auto *bc = row_bc.data();
                if (f == "COUNT" && ai == DConstants::INVALID_INDEX) {
                    if (sink.all_bc_one) {
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * (sink.build_ht.mask + 1) + sl[r]] += 1.0;
                        }
                    } else {
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] != DConstants::INVALID_INDEX) sink.build_slot_sums[a * (sink.build_ht.mask + 1) + sl[r]] += bc[r];
                        }
                    }
                    continue;
                }
                if (ai == DConstants::INVALID_INDEX || ai >= input.ColumnCount()) continue;
                auto ptype = input.data[ai].GetType().InternalType();
                auto *validity = FlatVector::Validity(input.data[ai]).GetData();
                if (f == "SUM" || f == "AVG" || f == "COUNT") {
                    bool is_avg = (f == "AVG");
                    bool is_count = (f == "COUNT");
                    bool pkfk = sink.all_bc_one;
                    auto cap = sink.build_ht.mask + 1;
                    #define BUILD_SLOT_SUM_LOOP(TYPE) { \
                        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                        if (pkfk) { \
                            for (idx_t r = 0; r < n; r++) { \
                                if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                                sink.build_slot_sums[a * cap + sl[r]] += is_count ? 1.0 : (double)vals[r]; \
                                if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += 1.0; \
                            } \
                        } else { \
                            for (idx_t r = 0; r < n; r++) { \
                                if (sl[r] == DConstants::INVALID_INDEX) continue; \
                                if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                                sink.build_slot_sums[a * cap + sl[r]] += (is_count ? 1.0 : (double)vals[r]) * bc[r]; \
                                if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += bc[r]; \
                            } \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::DOUBLE: BUILD_SLOT_SUM_LOOP(double); break;
                    case PhysicalType::FLOAT:  BUILD_SLOT_SUM_LOOP(float); break;
                    case PhysicalType::INT64:  BUILD_SLOT_SUM_LOOP(int64_t); break;
                    case PhysicalType::INT32:  BUILD_SLOT_SUM_LOOP(int32_t); break;
                    case PhysicalType::INT16:  BUILD_SLOT_SUM_LOOP(int16_t); break;
                    case PhysicalType::INT8:   BUILD_SLOT_SUM_LOOP(int8_t); break;
                    case PhysicalType::UINT64: BUILD_SLOT_SUM_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: BUILD_SLOT_SUM_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: BUILD_SLOT_SUM_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  BUILD_SLOT_SUM_LOOP(uint8_t); break;
                    default:
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] == DConstants::INVALID_INDEX) continue;
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                            sink.build_slot_sums[a * cap + sl[r]] += input.data[ai].GetValue(r).GetValue<double>() * (is_count ? 1.0 : bc[r]);
                            if (is_avg) sink.build_slot_counts[a * cap + sl[r]] += bc[r];
                        }
                        break;
                    }
                    #undef BUILD_SLOT_SUM_LOOP
                } else if (f == "MIN" || f == "MAX") {
                    bool is_min = (f == "MIN");
                    auto cap = sink.build_ht.mask + 1;
                    #define BUILD_SLOT_MINMAX_LOOP(TYPE) { \
                        auto *vals = FlatVector::GetData<TYPE>(input.data[ai]); \
                        for (idx_t r = 0; r < n; r++) { \
                            if (sl[r] == DConstants::INVALID_INDEX) continue; \
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue; \
                            double dv = (double)vals[r]; \
                            auto off = a * cap + sl[r]; \
                            if (is_min) { \
                                if (!sink.build_slot_has[off] || dv < sink.build_slot_mins[off]) { sink.build_slot_mins[off] = dv; sink.build_slot_has[off] = 1; } \
                            } else { \
                                if (!sink.build_slot_has[off] || dv > sink.build_slot_maxs[off]) { sink.build_slot_maxs[off] = dv; sink.build_slot_has[off] = 1; } \
                            } \
                        } \
                    }
                    switch (ptype) {
                    case PhysicalType::DOUBLE: BUILD_SLOT_MINMAX_LOOP(double); break;
                    case PhysicalType::FLOAT:  BUILD_SLOT_MINMAX_LOOP(float); break;
                    case PhysicalType::INT64:  BUILD_SLOT_MINMAX_LOOP(int64_t); break;
                    case PhysicalType::INT32:  BUILD_SLOT_MINMAX_LOOP(int32_t); break;
                    case PhysicalType::INT16:  BUILD_SLOT_MINMAX_LOOP(int16_t); break;
                    case PhysicalType::INT8:   BUILD_SLOT_MINMAX_LOOP(int8_t); break;
                    case PhysicalType::UINT64: BUILD_SLOT_MINMAX_LOOP(uint64_t); break;
                    case PhysicalType::UINT32: BUILD_SLOT_MINMAX_LOOP(uint32_t); break;
                    case PhysicalType::UINT16: BUILD_SLOT_MINMAX_LOOP(uint16_t); break;
                    case PhysicalType::UINT8:  BUILD_SLOT_MINMAX_LOOP(uint8_t); break;
                    default:
                        for (idx_t r = 0; r < n; r++) {
                            if (sl[r] == DConstants::INVALID_INDEX) continue;
                            if (validity && !((validity[r/64] >> (r%64)) & 1)) continue;
                            double dv = input.data[ai].GetValue(r).GetValue<double>();
                            auto off = a * cap + sl[r];
                            if (is_min) {
                                if (!sink.build_slot_has[off] || dv < sink.build_slot_mins[off]) { sink.build_slot_mins[off] = dv; sink.build_slot_has[off] = 1; }
                            } else {
                                if (!sink.build_slot_has[off] || dv > sink.build_slot_maxs[off]) { sink.build_slot_maxs[off] = dv; sink.build_slot_has[off] = 1; }
                            }
                        }
                        break;
                    }
                    #undef BUILD_SLOT_MINMAX_LOOP
                }
            }
            chunk.SetCardinality(0);
            return OperatorResultType::NEED_MORE_INPUT;
        }
        return ExecuteResultHashSourcePath(*this, ctx, input, chunk, sink, n, na, same_keys, h);
    }
} // namespace duckdb
