#include "AddressRouter.h"

#include <mutex>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

template <typename T>
using RouterResult = BasicResult<T, bool>;

bool AddressRouter::isValidAddress(const std::string& instance, const std::string& token) const {
    std::shared_lock<std::shared_mutex> lock(_routesMutex);

    // Check if token exists and maps to the correct instance
    auto it = _routeMap.find(instance);
    if (it == _routeMap.end()) {
        return false; // Token not found
    }

    return (token == it->second.first);
}

void AddressRouter::addRoute(const std::string& instance, const std::string& token, const std::string& route) {
    std::unique_lock<std::shared_mutex> lock(_routesMutex);

    _routeMap[instance] = std::pair<std::string, std::string>(token, route);
}

void AddressRouter::removeRoute(const std::string& instance) {
    std::unique_lock<std::shared_mutex> lock(_routesMutex);
    _routeMap.erase(instance);
}

void AddressRouter::replaceAllRoutes(const std::unordered_map<std::string, std::pair<std::string, std::string>>& routeMap) {
    std::unique_lock<std::shared_mutex> lock(_routesMutex);

    _routeMap = routeMap;
}

RouterResult<std::string> AddressRouter::getInstanceAddress(const std::string& instance) {
    std::shared_lock<std::shared_mutex> lock(_routesMutex);

    auto it = _routeMap.find(instance);
    if (it == _routeMap.end()) {
        return BadResult<bool>(false);
    }
    return it->second.second;
}

RouterResult<std::pair<std::string, std::string>> AddressRouter::getRouteMapEntry(const std::string& instance) {
    std::shared_lock<std::shared_mutex> lock(_routesMutex);

    auto it = _routeMap.find(instance);
    if (it == _routeMap.end()) {
        return BadResult<bool>(false);
    }
    return it->second;
}

bool AddressRouter::fetchRoutesFromSupabase(const SupabaseConfig& config) {
    try {
        // Extract host and path from URL
        std::string host;
        std::string path;
        const std::string https = "https://";

        if (config.url.substr(0, https.length()) == https) {
            const size_t hostStart = https.length();
            const size_t pathStart = config.url.find('/', hostStart);

            if (pathStart != std::string::npos) {
                host = config.url.substr(hostStart, pathStart - hostStart);
                path = config.url.substr(pathStart);
            } else {
                host = config.url.substr(hostStart);
                path = "";
            }
        } else {
            std::cerr << "Invalid Supabase URL: must start with https://" << std::endl;
            return false;
        }

        // Construct the REST API endpoint
        const std::string endpoint = path + "/rest/v1/rpc/" + config.joinFuncName;

        httplib::SSLClient client(host);
        client.set_connection_timeout(10, 0);
        client.set_read_timeout(30, 0);

        httplib::Headers headers = {
            {"apikey",        config.apiKey            },
            {"Authorization", "Bearer " + config.apiKey},
            {"Content-Type",  "application/json"       }
        };

        const auto res = client.Get(endpoint, headers);

        // std::cout << "Debug: Response body: " << res->body << std::endl;

        if (!res || res->status != 200) {
            std::cerr << "Failed to fetch routes from Supabase. Status: "
                      << (res ? std::to_string(res->status) : "No response") << std::endl;
            return false;
        }

        // Parse JSON response manually (simple parser for array of objects)
        std::unordered_map<std::string, std::pair<std::string, std::string>> newRouteMap;
        auto jsonRes = json::parse(res->body);

        for (const auto& record : jsonRes) {
            const auto& token = record["token"].get<std::string>();
            const auto& instanceId = record["instance_id"].get<std::string>();
            const auto& localAddress = record["local_address"].get<std::string>();
            newRouteMap[instanceId] = std::pair<std::string, std::string>(token, localAddress);
        }

        if (!newRouteMap.empty()) {
            replaceAllRoutes(newRouteMap);
            std::cout << "Successfully loaded " << newRouteMap.size() << " routes from Supabase" << std::endl;
            return true;
        } else {
            std::cerr << "No valid routes found in Supabase response" << std::endl;
            return false;
        }

    } catch (const std::exception& e) {
        std::cerr << "Exception while fetching routes from Supabase: " << e.what() << std::endl;
        return false;
    }
}
