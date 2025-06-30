#pragma once

#include <shared_mutex>
#include <unordered_set>

#include "SupabaseConfig.h"
class TokenValidator {
public:
    TokenValidator()
    {
        // Initialize with some example tokens as fallback
        // These will be replaced by tokens from Supabase
        _validTokens = {
            "valid_token_123",
            "valid_token_456",
            "valid_token_789"};
    }

    bool isValidToken(const std::string& token) const;
    void addToken(const std::string& token);
    void removeToken(const std::string& token);

    // Replace all tokens with a new set
    void replaceAllTokens(const std::unordered_set<std::string>& newTokens);
    bool fetchTokensFromSupabase(const SupabaseConfig& config);

private:
    std::unordered_set<std::string> _validTokens;
    mutable std::shared_mutex _tokenMutex;
};
