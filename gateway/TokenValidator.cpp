#include "TokenValidator.h"

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

bool TokenValidator::isValidToken(const std::string& token) const {
    const std::shared_lock<std::shared_mutex> lock(_tokenMutex);
    return _validTokens.find(token) != _validTokens.end();
}

void TokenValidator::addToken(const std::string& token) {
    const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
    _validTokens.insert(token);
}

void TokenValidator::removeToken(const std::string& token) {
    const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
    _validTokens.erase(token);
}

// Replace all tokens with a new set
void TokenValidator::replaceAllTokens(const std::unordered_set<std::string>& newTokens) {
    const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
    _validTokens = newTokens;
}

// Fetch tokens from Supabase
bool TokenValidator::fetchTokensFromSupabase(const SupabaseConfig& config) {
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
        const std::string endpoint = path + "/rest/v1/" + config.tokenTableName + "?select=token";

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
            std::cerr << "Failed to fetch tokens from Supabase. Status: "
                      << (res ? std::to_string(res->status) : "No response") << std::endl;
            return false;
        }

        // Parse JSON response manually (simple parser for array of objects)
        std::unordered_set<std::string> newTokens;
        auto jsonRes = json::parse(res->body);

        for (const auto& record : jsonRes) {
            newTokens.insert(record["token"].get<std::string>());
        }

        if (!newTokens.empty()) {
            replaceAllTokens(newTokens);
            std::cout << "Successfully loaded " << newTokens.size() << " tokens from Supabase" << std::endl;
            return true;
        } else {
            std::cerr << "No valid tokens found in Supabase response" << std::endl;
            return false;
        }

    } catch (const std::exception& e) {
        std::cerr << "Exception while fetching tokens from Supabase: " << e.what() << std::endl;
        return false;
    }
}
