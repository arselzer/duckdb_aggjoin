/*
 * AggJoin Optimizer Extension v5
 *
 * Post-optimize approach: walks through Projection chain between
 * Aggregate and Join to remap column indices. Two-child pipeline:
 * build side sinks, probe side feeds ExecuteInternal.
 */

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "aggjoin_optimizer_shared.hpp"

#if __has_include("duckdb/main/extension_callback_manager.hpp")
#include "duckdb/main/extension_callback_manager.hpp"
#define HAS_CALLBACK_MANAGER 1
#else
#include "duckdb/main/config.hpp"
#define HAS_CALLBACK_MANAGER 0
#endif

#include <cstdlib>
#include <atomic>

namespace duckdb {

bool AggJoinTraceEnabled() {
    static int enabled = []() {
        auto *env = std::getenv("AGGJOIN_TRACE");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return enabled != 0;
}

bool AggJoinTraceStatsEnabled() {
    static int enabled = []() {
        auto *env = std::getenv("AGGJOIN_TRACE_STATS");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return enabled != 0;
}

bool AggJoinStaticDisabledByEnv() {
    static int disabled = []() {
        auto *env = std::getenv("AGGJOIN_DISABLE_STATIC");
        if (!env || !env[0]) return 0;
        if (env[0] == '0') return 0;
        return 1;
    }();
    return disabled != 0;
}

static std::atomic<int64_t> aggjoin_test_hash_bits {-1};
static std::atomic<int64_t> aggjoin_test_ht_capacity {-1};

int64_t GetAggJoinTestHashBits() {
    return aggjoin_test_hash_bits.load();
}

void SetAggJoinTestHashBits(int64_t bits) {
    aggjoin_test_hash_bits.store(bits);
}

int64_t GetAggJoinTestHTCapacity() {
    return aggjoin_test_ht_capacity.load();
}

void SetAggJoinTestHTCapacity(int64_t capacity) {
    aggjoin_test_ht_capacity.store(capacity);
}

struct AggJoinOptimizerInfo : public OptimizerExtensionInfo {
    explicit AggJoinOptimizerInfo(bool ignore_disable_static_p) : ignore_disable_static(ignore_disable_static_p) {
    }
    bool ignore_disable_static;
};

static void AggJoinOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    bool ignore_disable_static = false;
    if (input.info) {
        auto info = dynamic_cast<AggJoinOptimizerInfo *>(input.info.get());
        if (info) {
            ignore_disable_static = info->ignore_disable_static;
        }
    }
    if (!ignore_disable_static && AggJoinStaticDisabledByEnv()) {
        if (AggJoinTraceEnabled()) {
            fprintf(stderr, "[AGGJOIN] built-in optimizer disabled by AGGJOIN_DISABLE_STATIC\n");
        }
        return;
    }
    AggJoinRewriteState state;
    WalkAndReplace(input.context, input.optimizer, plan, state, false);
    StripDecompressProjections(plan);
    if (!state.replacement_bindings.empty()) {
        ColumnBindingReplacer replacer;
        replacer.replacement_bindings = std::move(state.replacement_bindings);
        replacer.VisitOperator(*plan);
    }
}

void RegisterAggJoinOptimizer(DatabaseInstance &db, bool ignore_disable_static) {
    OptimizerExtension ext;
    ext.optimize_function = AggJoinOptimize;
    ext.optimizer_info = make_shared_ptr<AggJoinOptimizerInfo>(ignore_disable_static);
#if HAS_CALLBACK_MANAGER
    ExtensionCallbackManager::Get(db).Register(std::move(ext));
#else
    auto &config = DBConfig::GetConfig(db);
    config.optimizer_extensions.push_back(std::move(ext));
#endif
}

} // namespace duckdb
