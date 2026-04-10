#include "aggjoin_emit_internal.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

bool TryEmitDirectLikeResult(const PhysicalAggJoin &op, DataChunk &chunk, AggJoinSinkState &sink,
                             AggJoinSourceState &src, SourceResultType &result) {
    auto &col = op.col;

    // SEGMENTED DIRECT MODE: grouped emit from segment-backed arrays
    if (sink.segmented_multi_direct_mode) {
        if (!src.initialized) {
            src.initialized = true;
            src.direct_keys = sink.segmented_active_keys;
        }
        if (src.pos >= src.direct_keys.size()) {
            chunk.SetCardinality(0);
            result = SourceResultType::FINISHED;
            return true;
        }
        idx_t batch = std::min((idx_t)(src.direct_keys.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
        auto &ci = col.group_compress[0];
        if (ci.has_compress && !ci.is_string_compress) {
            auto compress_offset = ci.offset;
            switch (ci.compressed_type.InternalType()) {
            case PhysicalType::UINT8: {
                auto *dst = FlatVector::GetData<uint8_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (uint8_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                break;
            }
            case PhysicalType::UINT16: {
                auto *dst = FlatVector::GetData<uint16_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (uint16_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                break;
            }
            case PhysicalType::UINT32: {
                auto *dst = FlatVector::GetData<uint32_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                break;
            }
            default: {
                auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                break;
            }
            }
        } else {
            auto out_type = chunk.data[0].GetType().InternalType();
            switch (out_type) {
            case PhysicalType::INT32: {
                auto *dst = FlatVector::GetData<int32_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                break;
            }
            case PhysicalType::INT64: {
                auto *dst = FlatVector::GetData<int64_t>(chunk.data[0]);
                for (idx_t i = 0; i < batch; i++) dst[i] = (int64_t)src.direct_keys[src.pos + i] + sink.key_min;
                break;
            }
            default:
                for (idx_t i = 0; i < batch; i++) chunk.data[0].SetValue(i, Value::BIGINT((int64_t)src.direct_keys[src.pos + i] + sink.key_min));
                break;
            }
        }
        for (idx_t a = 0; a < sink.num_aggs; a++) {
            auto &f = col.agg_funcs[a];
            auto out_idx = 1 + a;
            auto out_type = chunk.data[out_idx].GetType().InternalType();
            auto *dst = (out_type == PhysicalType::INT64) ? nullptr : FlatVector::GetData<double>(chunk.data[out_idx]);
            auto &validity = FlatVector::Validity(chunk.data[out_idx]);
            for (idx_t i = 0; i < batch; i++) {
                auto k = src.direct_keys[src.pos + i];
                auto seg = k >> sink.segmented_shift;
                auto local = k & sink.segmented_mask;
                if (f == "AVG") {
                    auto sum = sink.segmented_multi_accums[seg][local * sink.segmented_accum_slots + sink.segmented_accum_index[a]];
                    auto count = sink.segmented_multi_avg_counts[seg][local * sink.segmented_avg_slots + sink.segmented_avg_index[a]];
                    auto v = count > 0 ? sum / count : 0.0;
                    if (dst) dst[i] = v;
                    else chunk.data[out_idx].SetValue(i, Value::DOUBLE(v));
                } else if (f == "MIN") {
                    auto slot = local * sink.segmented_min_slots + sink.segmented_min_index[a];
                    if (sink.segmented_multi_min_has[seg][slot]) {
                        dst[i] = sink.segmented_multi_mins[seg][slot];
                    } else {
                        validity.SetInvalid(i);
                    }
                } else if (f == "MAX") {
                    auto slot = local * sink.segmented_max_slots + sink.segmented_max_index[a];
                    if (sink.segmented_multi_max_has[seg][slot]) {
                        dst[i] = sink.segmented_multi_maxs[seg][slot];
                    } else {
                        validity.SetInvalid(i);
                    }
                } else {
                    auto sum = sink.segmented_multi_accums[seg][local * sink.segmented_accum_slots + sink.segmented_accum_index[a]];
                    if (out_type == PhysicalType::INT64) chunk.data[out_idx].SetValue(i, Value::BIGINT((int64_t)sum));
                    else dst[i] = sum;
                }
            }
        }
        src.pos += batch;
        chunk.SetCardinality(batch);
        result = batch ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
        return true;
    }

    // DIRECT MODE: emit from flat arrays — vectorized output
    if (sink.segmented_direct_mode || sink.direct_mode) {
        if (!src.initialized) {
            src.initialized = true;
            if (!col.group_cols.empty()) {
                if (sink.segmented_direct_mode) {
                    src.direct_keys = sink.segmented_active_keys;
                } else if (sink.group_is_key) {
                    if (!sink.direct_active_keys.empty()) {
                        src.direct_keys = sink.direct_active_keys;
                    } else {
                        for (idx_t k = 0; k < sink.key_range; k++) {
                            if (sink.build_counts[k] > 0) src.direct_keys.push_back(k);
                        }
                    }
                } else {
                    for (idx_t k = 0; k < sink.direct_group_init.size(); k++) {
                        if (sink.direct_group_init[k]) src.direct_keys.push_back(k);
                    }
                }
            }
        }
        if (col.group_cols.empty()) {
            if (src.pos > 0) {
                chunk.SetCardinality(0);
                result = SourceResultType::FINISHED;
                return true;
            }
            auto na = col.agg_funcs.size();
            for (idx_t a = 0; a < na; a++) {
                auto &f = col.agg_funcs[a];
                if (f == "AVG") {
                    chunk.data[a].SetValue(0, sink.ungrouped_count[a] > 0 ? Value::DOUBLE(sink.ungrouped_sum[a] / sink.ungrouped_count[a])
                                                                          : Value::DOUBLE(0.0));
                } else if (f == "MIN") {
                    if (sink.ungrouped_has[a]) chunk.data[a].SetValue(0, Value::DOUBLE(sink.ungrouped_min[a]));
                    else chunk.data[a].SetValue(0, Value());
                } else if (f == "MAX") {
                    if (sink.ungrouped_has[a]) chunk.data[a].SetValue(0, Value::DOUBLE(sink.ungrouped_max[a]));
                    else chunk.data[a].SetValue(0, Value());
                } else if (chunk.data[a].GetType().InternalType() == PhysicalType::INT64) {
                    chunk.data[a].SetValue(0, Value::BIGINT((int64_t)sink.ungrouped_sum[a]));
                } else {
                    chunk.data[a].SetValue(0, Value::DOUBLE(sink.ungrouped_sum[a]));
                }
            }
            src.pos = 1;
            chunk.SetCardinality(1);
            result = SourceResultType::HAVE_MORE_OUTPUT;
            return true;
        }
        if (src.pos >= src.direct_keys.size()) {
            chunk.SetCardinality(0);
            result = SourceResultType::FINISHED;
            return true;
        }
        idx_t batch = std::min((idx_t)(src.direct_keys.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
        for (idx_t gi = 0; gi < col.group_cols.size(); gi++) {
            auto out_type = chunk.data[gi].GetType().InternalType();
            if (sink.group_is_key && gi == 0) {
                auto &ci = col.group_compress[0];
                if (ci.has_compress && !ci.is_string_compress) {
                    auto compress_offset = ci.offset;
                    switch (ci.compressed_type.InternalType()) {
                    case PhysicalType::UINT8: {
                        auto *dst = FlatVector::GetData<uint8_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint8_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                        break;
                    }
                    case PhysicalType::UINT16: {
                        auto *dst = FlatVector::GetData<uint16_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint16_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                        break;
                    }
                    case PhysicalType::UINT32: {
                        auto *dst = FlatVector::GetData<uint32_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                        break;
                    }
                    case PhysicalType::UINT64: {
                        auto *dst = FlatVector::GetData<uint64_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint64_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min - compress_offset);
                        break;
                    }
                    default: {
                        auto *dst = FlatVector::GetData<int32_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                        break;
                    }
                    }
                } else {
                    switch (out_type) {
                    case PhysicalType::INT32: {
                        auto *dst = FlatVector::GetData<int32_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (int32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                        break;
                    }
                    case PhysicalType::INT64: {
                        auto *dst = FlatVector::GetData<int64_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (int64_t)src.direct_keys[src.pos + i] + sink.key_min;
                        break;
                    }
                    case PhysicalType::UINT32: {
                        auto *dst = FlatVector::GetData<uint32_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint32_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                        break;
                    }
                    case PhysicalType::UINT64: {
                        auto *dst = FlatVector::GetData<uint64_t>(chunk.data[gi]);
                        for (idx_t i = 0; i < batch; i++) dst[i] = (uint64_t)((int64_t)src.direct_keys[src.pos + i] + sink.key_min);
                        break;
                    }
                    default:
                        for (idx_t i = 0; i < batch; i++) chunk.data[gi].SetValue(i, Value::BIGINT((int64_t)src.direct_keys[src.pos + i] + sink.key_min));
                        break;
                    }
                }
            } else {
                for (idx_t i = 0; i < batch; i++) {
                    auto k = src.direct_keys[src.pos + i];
                    if (k < sink.direct_group_vals.size() && gi < sink.direct_group_vals[k].size()) {
                        chunk.data[gi].SetValue(i, sink.direct_group_vals[k][gi]);
                    } else {
                        chunk.data[gi].SetValue(i, Value());
                    }
                }
            }
        }
        for (idx_t a = 0; a < sink.num_aggs; a++) {
            auto &f = col.agg_funcs[a];
            auto out_idx = col.group_cols.size() + a;
            auto out_type = chunk.data[out_idx].GetType().InternalType();
            if (f == "AVG") {
                auto *sums = sink.segmented_direct_mode ? nullptr : sink.direct_sums.data() + a * sink.key_range;
                auto *counts = sink.segmented_direct_mode ? nullptr : sink.direct_counts.data() + a * sink.key_range;
                if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        if (sink.segmented_direct_mode) {
                            auto seg = k >> sink.segmented_shift;
                            auto local = k & sink.segmented_mask;
                            auto sum = sink.segmented_sums[seg][local];
                            auto cnt = sink.segmented_avg_counts[seg][local];
                            dst[i] = cnt > 0 ? sum / cnt : 0.0;
                        } else {
                            dst[i] = counts[k] > 0 ? sums[k] / counts[k] : 0.0;
                        }
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        double v = 0.0;
                        if (sink.segmented_direct_mode) {
                            auto seg = k >> sink.segmented_shift;
                            auto local = k & sink.segmented_mask;
                            auto sum = sink.segmented_sums[seg][local];
                            auto cnt = sink.segmented_avg_counts[seg][local];
                            v = cnt > 0 ? sum / cnt : 0.0;
                        } else {
                            v = counts[k] > 0 ? sums[k] / counts[k] : 0.0;
                        }
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(v));
                    }
                }
            } else if (f == "MIN" || f == "MAX") {
                bool is_min = (f == "MIN");
                auto *arr = sink.segmented_direct_mode ? nullptr
                                                       : (is_min ? sink.direct_mins.data() + a * sink.key_range
                                                                 : sink.direct_maxs.data() + a * sink.key_range);
                auto *has = sink.segmented_direct_mode ? nullptr : sink.direct_has.data() + a * sink.key_range;
                auto &validity = FlatVector::Validity(chunk.data[out_idx]);
                if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        if (sink.segmented_direct_mode) {
                            validity.SetInvalid(i);
                        } else if (has[k]) {
                            dst[i] = arr[k];
                        } else {
                            validity.SetInvalid(i);
                        }
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        if (sink.segmented_direct_mode) {
                            chunk.data[out_idx].SetValue(i, Value());
                        } else if (has[k]) {
                            chunk.data[out_idx].SetValue(i, Value::DOUBLE(arr[k]));
                        } else {
                            chunk.data[out_idx].SetValue(i, Value());
                        }
                    }
                }
            } else {
                auto *sums = sink.segmented_direct_mode ? nullptr : sink.direct_sums.data() + a * sink.key_range;
                if (out_type == PhysicalType::INT64) {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        if (sink.segmented_direct_mode) {
                            auto seg = k >> sink.segmented_shift;
                            auto local = k & sink.segmented_mask;
                            dst[i] = (int64_t)sink.segmented_sums[seg][local];
                        } else {
                            dst[i] = (int64_t)sums[k];
                        }
                    }
                } else if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        if (sink.segmented_direct_mode) {
                            auto seg = k >> sink.segmented_shift;
                            auto local = k & sink.segmented_mask;
                            dst[i] = sink.segmented_sums[seg][local];
                        } else {
                            dst[i] = sums[k];
                        }
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto k = src.direct_keys[src.pos + i];
                        double v = sink.segmented_direct_mode
                                       ? sink.segmented_sums[k >> sink.segmented_shift][k & sink.segmented_mask]
                                       : sums[k];
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(v));
                    }
                }
            }
        }
        src.pos += batch;
        chunk.SetCardinality(batch);
        result = batch ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
        return true;
    }

    if (sink.build_slot_hash_mode) {
        if (!src.initialized) {
            src.initialized = true;
            src.slot_indices.reserve(sink.build_slot_seen.size());
            for (idx_t i = 0; i < sink.build_slot_seen.size(); i++) {
                if (sink.build_slot_seen[i]) src.slot_indices.push_back(i);
            }
        }
        if (src.pos >= src.slot_indices.size()) {
            chunk.SetCardinality(0);
            result = SourceResultType::FINISHED;
            return true;
        }
        auto cap = sink.build_ht.mask + 1;
        idx_t batch = std::min((idx_t)(src.slot_indices.size() - src.pos), (idx_t)STANDARD_VECTOR_SIZE);
        for (idx_t i = 0; i < batch; i++) {
            auto slot = src.slot_indices[src.pos + i];
            auto &entry = sink.build_ht.buckets[slot];
            if (col.group_compress[0].is_string_compress) {
                FlatVector::GetData<string_t>(chunk.data[0])[i] = StringVector::AddStringOrBlob(chunk.data[0], entry.str_key);
            } else {
                auto out_type = chunk.data[0].GetType().InternalType();
                if (entry.int_key != INT64_MIN) {
                    switch (out_type) {
                    case PhysicalType::INT32:
                        FlatVector::GetData<int32_t>(chunk.data[0])[i] = (int32_t)entry.int_key;
                        break;
                    case PhysicalType::INT64:
                        FlatVector::GetData<int64_t>(chunk.data[0])[i] = entry.int_key;
                        break;
                    case PhysicalType::UINT32:
                        FlatVector::GetData<uint32_t>(chunk.data[0])[i] = (uint32_t)entry.int_key;
                        break;
                    case PhysicalType::UINT64:
                        FlatVector::GetData<uint64_t>(chunk.data[0])[i] = (uint64_t)entry.int_key;
                        break;
                    default:
                        chunk.data[0].SetValue(i, Value::BIGINT(entry.int_key));
                        break;
                    }
                } else {
                    if (out_type == PhysicalType::DOUBLE) {
                        FlatVector::GetData<double>(chunk.data[0])[i] = entry.dbl_key;
                    } else {
                        chunk.data[0].SetValue(i, Value::DOUBLE(entry.dbl_key));
                    }
                }
            }
        }
        auto na = col.agg_funcs.size();
        for (idx_t a = 0; a < na; a++) {
            auto &f = col.agg_funcs[a];
            auto out_idx = 1 + a;
            auto out_type = chunk.data[out_idx].GetType().InternalType();
            if (f == "AVG") {
                if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        auto cnt = sink.build_slot_counts[off];
                        dst[i] = cnt > 0 ? sink.build_slot_sums[off] / cnt : 0.0;
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        auto cnt = sink.build_slot_counts[off];
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(cnt > 0 ? sink.build_slot_sums[off] / cnt : 0.0));
                    }
                }
            } else if (f == "MIN" || f == "MAX") {
                bool is_min = (f == "MIN");
                auto &validity = FlatVector::Validity(chunk.data[out_idx]);
                if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        if (!sink.build_slot_has[off]) {
                            validity.SetInvalid(i);
                            continue;
                        }
                        dst[i] = is_min ? sink.build_slot_mins[off] : sink.build_slot_maxs[off];
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        if (!sink.build_slot_has[off]) {
                            chunk.data[out_idx].SetValue(i, Value());
                            continue;
                        }
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(is_min ? sink.build_slot_mins[off] : sink.build_slot_maxs[off]));
                    }
                }
            } else {
                if (out_type == PhysicalType::INT64) {
                    auto *dst = FlatVector::GetData<int64_t>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        dst[i] = (int64_t)sink.build_slot_sums[off];
                    }
                } else if (out_type == PhysicalType::DOUBLE) {
                    auto *dst = FlatVector::GetData<double>(chunk.data[out_idx]);
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        dst[i] = sink.build_slot_sums[off];
                    }
                } else {
                    for (idx_t i = 0; i < batch; i++) {
                        auto off = a * cap + src.slot_indices[src.pos + i];
                        chunk.data[out_idx].SetValue(i, Value::DOUBLE(sink.build_slot_sums[off]));
                    }
                }
            }
        }
        src.pos += batch;
        chunk.SetCardinality(batch);
        result = batch ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
        return true;
    }

    return false;
}

} // namespace duckdb
