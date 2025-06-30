#pragma once

#include <string>

struct SupabaseConfig {
    const std::string url;
    const std::string apiKey;
    const std::string tokenTableName;
    const std::string joinFuncName;

    SupabaseConfig(const std::string& u, const std::string& key, const std::string& tokenTable, const std::string& funcName)
        : url(u),
          apiKey(key),
          tokenTableName(tokenTable),
          joinFuncName(funcName)
    {
    }
};
