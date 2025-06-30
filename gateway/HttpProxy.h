#pragma once

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <string>
#include <vector>

#include "LoadBalancer.h"
#include "TokenValidator.h"
#include "AddressRouter.h"

class HttpProxy {
public:
    HttpProxy() 
    {
        setupRoutes();
    }

    void setBackendServers(const std::vector<std::pair<std::string, int>>& backendList);

    void setupRoutes();

    void start(const std::string& host, int port, int numThreads);

    void stop();

    LoadBalancer& getLoadBalancer() { return _loadBalancer; }
    const LoadBalancer& getLoadBalancer() const { return _loadBalancer; }

    TokenValidator& getTokenValidator() { return _tokenValidator; }
    const TokenValidator& getTokenValidator() const { return _tokenValidator; }

    AddressRouter& getAddressRouter() { return _addressRouter; }
    const AddressRouter& getAddressRouter() const { return _addressRouter; }

private:
    httplib::Server _server;
    mutable TokenValidator _tokenValidator;
    mutable AddressRouter _addressRouter;
    mutable LoadBalancer _loadBalancer;
    mutable std::atomic<uint64_t> _requestCount {0};
    mutable std::atomic<uint64_t> _errorCount {0};

    // Extract Bearer token from Authorization header
    std::string extractBearerToken(const httplib::Request& req) const;

    std::string extractInstanceId(const httplib::Request& req) const;

    // Validate request authentication
    bool validateAuth(const std::string& token, httplib::Response& res) const;

    bool validateRoute(const std::string& token, const std::string& instance, httplib::Response& res) const;

    // Forward request to backend server
    bool forwardRequest(const httplib::Request& req, httplib::Response& res,
                        const std::string& method, const std::string& path, const std::string& token) const;
};
