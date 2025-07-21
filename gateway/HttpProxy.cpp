#include "HttpProxy.h"

#include "Profiler.h"

template <ServerType server>
void HttpProxy<server>::setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) {
    _loadBalancer.setBackendServers(backendList);
}

template <ServerType server>
void HttpProxy<server>::setupRoutes() {
    // POST /query route
    _server.Post(R"(/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;

        const std::string token = extractBearerToken(req);
        if (!validateAuth(token, res)) {
            return;
        }

        forwardRequest(req, res, "POST", req.target, token);
        std::string profileLogs;

        Profiler::dump(profileLogs);

        std::cout << profileLogs << std::endl;
    });

    // GET /status route
    _server.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;

        // Status endpoint doesn't require authentication
        std::stringstream status;
        status << "{\n";
        status << "  \"proxy_status\": \"healthy\",\n";
        status << "  \"total_requests\": " << _requestCount.load() << ",\n";
        status << "  \"error_count\": " << _errorCount.load() << ",\n";
        status << "  \"backends\": [\n";

        const auto backends = _loadBalancer.getBackends();
        for (size_t i = 0; i < backends.size(); i++) {
            status << "    {\n";
            status << "      \"host\": \"" << backends[i].host << "\",\n";
            status << "      \"port\": " << backends[i].port << ",\n";
            status << "      \"healthy\": " << (backends[i].isHealthy ? "true" : "false") << "\n";
            status << "    }";

            if (i < backends.size() - 1) {
                status << ",";
            }
            status << "\n";
        }

        status << "  ]\n";
        status << "}";

        res.set_content(status.str(), "application/json");
    });

    // Default handler for unsupported routes
    _server.set_error_handler([](const httplib::Request& req, httplib::Response& res) {
        if (res.status == 404) {
            res.set_content("{\"error\": \"Not Found: Unsupported route\"}",
                            "application/json");
        }
    });
}

template <ServerType server>
void HttpProxy<server>::start(const std::string& host, const int port, const int numThreads) {
    constexpr std::string_view httpString = std::is_same_v<server, httplib::SSLServer> ? "HTTPS" : "HTTP";

    std::cout << "Starting " << httpString << " Proxy Server on " << host << ":" << port << std::endl;
    std::cout << "Using " << numThreads << " worker threads" << std::endl;

    // Configure server thread pool
    _server.new_task_queue = [numThreads] {
        return new httplib::ThreadPool(numThreads);
    };

    // Start the server
    if (!_server.listen(host, port)) {
        std::cerr << "Failed to start server" << std::endl;
    }
}

template <ServerType server>
void HttpProxy<server>::stop() {
    _server.stop();
}

// Extract Bearer token from Authorization header
template <ServerType server>
std::string HttpProxy<server>::extractBearerToken(const httplib::Request& req) const {
    Profile profile {"HttpProxy::extractBearerToken"};
    const auto auth = req.get_header_value("Authorization");
    if (auth.empty()) {
        return "";
    }

    const std::string bearerPrefix = "Bearer ";
    if (auth.substr(0, bearerPrefix.length()) != bearerPrefix) {
        return "";
    }

    return auth.substr(bearerPrefix.length());
}

// Extract Bearer token from Authorization header
template <ServerType server>
std::string HttpProxy<server>::extractInstanceId(const httplib::Request& req) const {
    Profile profile {"HttpProxy::extractInstanceId"};
    auto instance = req.get_header_value("Turing-Instance-Id");
    return instance;
}

// Validate request authentication
template <ServerType server>
bool HttpProxy<server>::validateAuth(const std::string& token, httplib::Response& res) const {
    Profile profile {"HttpProxy::validAuth"};

    if (token.empty() || !_tokenValidator.isValidToken(token)) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or missing token\"}",
                        "application/json");
        return false;
    }

    return true;
}

// Validate request authentication
template <ServerType server>
bool HttpProxy<server>::validateRoute(const std::string& token, const std::string& instance, httplib::Response& res) const {
    Profile profile {"HttpProxy::validRoute"};
    if (_addressRouter.isValidAddress(instance, token)) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or unauthorized instance id\"}",
                        "application/json");
        return false;
    }

    return true;
}

std::pair<std::string, int> parseAddress(const std::string& localAddress) {
    Profile profile {"HttpProxy::parseAddress"};
    size_t seperatorPos = localAddress.find_last_of(':');

    if (seperatorPos == std::string::npos) {
        return std::pair<std::string, int>();
    }

    std::string host = localAddress.substr(0, seperatorPos);
    int port = std::stoi(localAddress.substr(seperatorPos + 1));

    return std::pair<std::string, int>(host, port);
}

// Forward request to backend server
template <ServerType server>
bool HttpProxy<server>::forwardRequest(const httplib::Request& req,
                                       httplib::Response& res,
                                       const std::string& method,
                                       const std::string& path,
                                       const std::string& token) const {
    Profile profile {"HttpProxy::forwardRequest"};
    std::string instance = extractInstanceId(req);
    const auto& routeMapEntry = _addressRouter.getRouteMapEntry(instance);
    if (!routeMapEntry) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or unauthorized instance id\"}",
                        "application/json");
        _errorCount++;
        return false;
    }

    const auto& [instanceToken, address] = routeMapEntry.value();

    if (instanceToken != token) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or unauthorized instance id\"}",
                        "application/json");
        _errorCount++;
        return false;
    }

    const auto& [host, port] = parseAddress(address);
    httplib::Client client(host, port);
    client.set_connection_timeout(5, 0); // 5 seconds
    client.set_read_timeout(30, 0);      // 30 seconds

    // Copy headers from original request
    httplib::Headers headers;
    for (const auto& header : req.headers) {
        if (header.first != "Host" && header.first != "Connection") {
            headers.emplace(header.first, header.second);
        }
    }

    // Forward the request based on method
    httplib::Result result;

    {
        Profile fwdProf {"yum"};
        if (method == "POST") {
            result = client.Post(path, headers, req.body,
                                 req.get_header_value("Content-Type"));
        } else if (method == "GET") {
            result = client.Get(path, headers);
        }
    }

    if (!result) {
        res.status = 502;
        res.set_content("{\"error\": \"Bad Gateway: Backend connection failed\"}",
                        "application/json");
        _errorCount++;
        return false;
    }

    // Copy response from backend
    res.status = result->status;
    res.body = result->body;

    // Copy response headers
    for (const auto& header : result->headers) {
        if (header.first != "Transfer-Encoding" && header.first != "Content-Length" && header.first != "Connection") {
            res.set_header(header.first, header.second);
        }
    }

    return true;
}

template class HttpProxy<httplib::Server>;
template class HttpProxy<httplib::SSLServer>;
