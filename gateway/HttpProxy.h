#pragma once

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <string>
#include <vector>

#include "LoadBalancer.h"
#include "TokenValidator.h"
#include "AddressRouter.h"


template <typename T>
concept ServerType = std::same_as<T, httplib::Server> || std::same_as<T, httplib::SSLServer>;

class HttpProxyBase {
public:
    virtual ~HttpProxyBase() = default;

    virtual void setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) = 0;

    virtual void setupRoutes() = 0;

    virtual void start(const std::string& host, int port, int numThreads) = 0;

    virtual void stop() = 0;

    virtual LoadBalancer& getLoadBalancer() = 0;
    virtual const LoadBalancer& getLoadBalancer() const = 0;

    virtual TokenValidator& getTokenValidator() = 0;
    virtual const TokenValidator& getTokenValidator() const = 0;

    virtual AddressRouter& getAddressRouter() = 0;
    virtual const AddressRouter& getAddressRouter() const = 0;
    virtual bool isValid() const = 0;
};

template <ServerType Server>
class HttpProxy : public HttpProxyBase {
public:
    HttpProxy()
        requires std::same_as<Server, httplib::Server>
    {
        setupRoutes();
    }

    HttpProxy(const std::string& cert, const std::string& key)
        requires std::same_as<Server, httplib::SSLServer>
        :_server(cert.data(), key.data()) 
    {
        setupRoutes();
    }

    ~HttpProxy() override = default;

    void setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) override;

    void setupRoutes() override;

    void start(const std::string& host, int port, int numThreads) override;

    void stop() override;

    LoadBalancer& getLoadBalancer() override { return _loadBalancer; }
    const LoadBalancer& getLoadBalancer() const override { return _loadBalancer; }

    TokenValidator& getTokenValidator() override { return _tokenValidator; }
    const TokenValidator& getTokenValidator() const override { return _tokenValidator; }

    AddressRouter& getAddressRouter() override { return _addressRouter; }
    const AddressRouter& getAddressRouter() const override { return _addressRouter; }
    bool isValid() const override { return _server.is_valid(); }

private:
    Server _server;
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
