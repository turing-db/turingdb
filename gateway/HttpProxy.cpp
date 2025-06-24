#include "HttpProxy.h"

void HttpProxy::setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) {
    _loadBalancer.setBackendServers(backendList);
}

void HttpProxy::setupRoutes() {
    // POST /query route
    _server.Post(R"(/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;

        if (!validateAuth(req, res)) {
            return;
        }

        forwardRequest(req, res, "POST", req.target);
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

void HttpProxy::start(const std::string& host, const int port, const int numThreads) {
    std::cout << "Starting HTTP Proxy Server on " << host << ":" << port << std::endl;
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

void HttpProxy::stop() {
    _server.stop();
}

// Extract Bearer token from Authorization header
std::string HttpProxy::extractBearerToken(const httplib::Request& req) const {
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

// Validate request authentication
bool HttpProxy::validateAuth(const httplib::Request& req, httplib::Response& res) const {
    const std::string token = extractBearerToken(req);

    if (token.empty() || !_tokenValidator.isValidToken(token)) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or missing token\"}",
                        "application/json");
        return false;
    }

    return true;
}

// Forward request to backend server
bool HttpProxy::forwardRequest(const httplib::Request& req, httplib::Response& res,
                               const std::string& method, const std::string& path) const {
    const auto* backend = _loadBalancer.getNextBackend();

    if (!backend) {
        res.status = 503;
        res.set_content("{\"error\": \"Service Unavailable: No healthy backends\"}",
                        "application/json");
        _errorCount++;
        return false;
    }

    httplib::Client client(backend->host, backend->port);
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

    if (method == "POST") {
        result = client.Post(path, headers, req.body,
                             req.get_header_value("Content-Type"));
    } else if (method == "GET") {
        result = client.Get(path, headers);
    }

    if (!result) {
        // Mark backend as unhealthy if connection failed
        _loadBalancer.markUnhealthy(backend->host, backend->port);

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
