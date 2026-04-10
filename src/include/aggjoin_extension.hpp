#pragma once

#include <cstdlib>

#include "duckdb.hpp"

namespace duckdb {

class AggjoinExtension : public Extension {
public:
    void Load(ExtensionLoader &loader) override;
    std::string Name() override {
        auto *env = std::getenv("AGGJOIN_DISABLE_STATIC");
        if (env && env[0] && env[0] != '0') {
            return "aggjoin_static_disabled";
        }
        return "aggjoin";
    }
    std::string Version() const override { return "0.1.0"; }
};

} // namespace duckdb
