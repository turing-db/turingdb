#pragma once

#include <string>

struct SupabaseConfig {
    const std::string url;
    const std::string apiKey;
    const std::string tokenTableName;

    SupabaseConfig(const std::string& u, const std::string& key, const std::string& table)
        : url(u),
          apiKey(key),
          tokenTableName(table)
    {
    }
};
