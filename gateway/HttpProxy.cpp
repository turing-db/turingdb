#include "HttpProxy.h"

#include "Profiler.h"

template <ServerType server>
void HttpProxy<server>::setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) {
    _loadBalancer.setBackendServers(backendList);
}

void add_cors_headers(httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "https://console.turingdb.ai");
    res.set_header("Access-Control-Allow-Credentials", "true");
    res.set_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
    res.set_header("Access-Control-Max-Age", "86400");
}

template <ServerType server>
void HttpProxy<server>::setupRoutes() {
    _server.Options(R"(.*)", [](const httplib::Request& req, httplib::Response& res) {
        add_cors_headers(res);
        res.status = 204;  // No Content
        return;
    });

    _server.Get("/auth", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;
        add_cors_headers(res);

        const std::string token = extractBearerToken(req);
        if (!validateAuth(token, res)) {
            return;
        }

        const std::string encryptedToken = _encryptor.encrypt(token);
        std::string cookie = "session=" + encryptedToken + "; HttpOnly" + // Prevent JS access
            "; Secure" +                                 // HTTPS only
            "; SameSite=Lax" +                           // CSRF protection
            "; Max-Age=3600" +                           // 1 hour expiration
            "; Path=/" +                                 // Available site-wide
            "; Domain=.turingdb.ai";                   // Available site-wide

        res.set_header("Set-Cookie", cookie);

        res.set_content("Authentication successful", "text/plain");
    });

    _server.Post(R"(/sdk/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;

        const std::string instanceId = extractInstanceId(req);
        std::string token;

        token = extractBearerToken(req);

        if (!validateAuth(token, res)) {
            return;
        }
        forwardRequest(req, "POST", req.target.substr(4), token, instanceId, 6666, res);

#ifdef TURING_PROFILE
        std::string profileLogs;
        Profiler::dump(profileLogs);
        std::cout << profileLogs << std::endl;
#endif
    });

    _server.Get(R"(/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;
        add_cors_headers(res);

        const std::string session = extractSessionFromCookie(req);
        const std::string token = _encryptor.decrypt(session);
        if (!validateAuth(token, res)) {
            return;
        }

        const std::string instanceId = extractInstanceIdFromDomain(req);
        forwardRequest(req, "GET", req.target, token, instanceId, 5001, res);
    });

    _server.Post(R"(/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        _requestCount++;
        add_cors_headers(res);

        const std::string session = extractSessionFromCookie(req);
        const std::string token = _encryptor.decrypt(session);
        if (!validateAuth(token, res)) {
            return;
        }

        const std::string instanceId = extractInstanceIdFromDomain(req);
        forwardRequest(req, "POST", req.target, token, instanceId, 5001, res);
        // res.set_content("decryption successful", "text/plain");
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
std::string HttpProxy<server>::extractSessionFromCookie(const httplib::Request& req) const {
    Profile profile {"HttpProxy::extractBearerToken"};
    const auto cookie = req.get_header_value("Cookie");
    if (cookie.empty()) {
        return "";
    }

    const std::string bearerPrefix = "session=";
    if (cookie.substr(0, bearerPrefix.length()) != bearerPrefix) {
        return "";
    }

    return cookie.substr(bearerPrefix.length());
}

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

template <ServerType server>
std::string HttpProxy<server>::extractInstanceIdFromDomain(const httplib::Request& req) const {
    Profile profile {"HttpProxy::extractInstanceId"};

    auto hostHeader = req.get_header_value("Host");
    if (hostHeader.empty()) {
        return "";
    }

    // Extract subdomain: "instance-123.mysite.org" → "instance-123"
    size_t dot_pos = hostHeader.find('.');
    if (dot_pos == std::string::npos) {
        return ""; // No subdomain
    }

    return hostHeader.substr(0, dot_pos);
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
bool HttpProxy<server>::validateRoute(const std::string& token, const std::string& instanceId, httplib::Response& res) const {
    Profile profile {"HttpProxy::validRoute"};
    if (_addressRouter.isValidAddress(instanceId, token)) {
        res.status = 403;
        res.set_content("{\"error\": \"Forbidden: Invalid or unauthorized instance id\"}",
                        "application/json");
        return false;
    }

    return true;
}

// Forward request to backend server
template <ServerType server>
bool HttpProxy<server>::forwardRequest(const httplib::Request& req,
                                       const std::string& method,
                                       const std::string& path,
                                       const std::string& token,
                                       const std::string& instanceId,
                                       const int portNumber,
                                       httplib::Response& res) const {
    Profile profile {"HttpProxy::forwardRequest"};
    const auto& routeMapEntry = _addressRouter.getRouteMapEntry(instanceId);
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

    httplib::Client client(address, portNumber);
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
