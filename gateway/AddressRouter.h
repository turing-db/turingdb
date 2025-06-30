#pragma once

#include <shared_mutex>
#include <unordered_map>

#include "BasicResult.h"
#include "SupabaseConfig.h"

class AddressRouter {
    template <typename T>
    using RouterResult = BasicResult<T, bool>;

public:
    AddressRouter() {
        // Initialize with some example tokens as fallback
        // These will be replaced by tokens from Supabase
    }

    bool isValidAddress(const std::string& instance, const std::string& token) const;
    RouterResult<std::string> getInstanceAddress(const std::string& instance);
    RouterResult<std::pair<std::string, std::string>> getRouteMapEntry(const std::string& instance);

    void addRoute(const std::string& instance, const std::string& token, const std::string& route);
    void removeRoute(const std::string& instance);

    // Replace all routes with a new set
    void replaceAllRoutes(const std::unordered_map<std::string, std::pair<std::string, std::string>>& routeMap);

    bool fetchRoutesFromSupabase(const SupabaseConfig& config);

private:
    // instance_id -> (token, local_address)
    std::unordered_map<std::string, std::pair<std::string, std::string>> _routeMap;
    mutable std::shared_mutex _routesMutex;
};
