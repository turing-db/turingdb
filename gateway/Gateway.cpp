#include "Gateway.h"

#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <atomic>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <sstream>
#include <algorithm>
#include <stdlib.h>
#include <signal.h>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

// Configuration structure for backend servers
struct BackendServer {
    const std::string host;
    const int port;
    bool isHealthy;
    
    BackendServer(const std::string& h, const int p) 
        : host(h), port(p), isHealthy(true) {}
};

// Configuration for Supabase
struct SupabaseConfig {
    const std::string url;
    const std::string apiKey;
    const std::string tokenTableName;
    
    SupabaseConfig(const std::string& u, const std::string& key, const std::string& table)
        : url(u), apiKey(key), tokenTableName(table) {}
};

// Thread-safe token validator with Supabase integration
class TokenValidator {
public:
    TokenValidator() {
        // Initialize with some example tokens as fallback
        // These will be replaced by tokens from Supabase
        _validTokens = {
            "valid_token_123",
            "valid_token_456",
            "valid_token_789"
        };
    }
    
    bool isValidToken(const std::string& token) const {
        const std::shared_lock<std::shared_mutex> lock(_tokenMutex);
        return _validTokens.find(token) != _validTokens.end();
    }
    
    void addToken(const std::string& token) {
        const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
        _validTokens.insert(token);
    }
    
    void removeToken(const std::string& token) {
        const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
        _validTokens.erase(token);
    }
    
    // Replace all tokens with a new set
    void replaceAllTokens(const std::unordered_set<std::string>& newTokens) {
        const std::unique_lock<std::shared_mutex> lock(_tokenMutex);
        _validTokens = newTokens;
    }
    
    // Fetch tokens from Supabase
    bool fetchTokensFromSupabase(const SupabaseConfig& config) {
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
            const std::string endpoint = path + "/rest/v1/" + config.tokenTableName + "?select=token&is_active=eq.true";
            
            httplib::Client client(host);
            client.set_connection_timeout(10, 0);
            client.set_read_timeout(30, 0);
            
            httplib::Headers headers = {
                {"apikey", config.apiKey},
                {"Authorization", "Bearer " + config.apiKey},
                {"Content-Type", "application/json"}
            };
            
            const auto res = client.Get(endpoint.c_str(), headers);
            
            if (!res || res->status != 200) {
                std::cerr << "Failed to fetch tokens from Supabase. Status: " 
                         << (res ? std::to_string(res->status) : "No response") << std::endl;
                return false;
            }
            
            // Parse JSON response manually (simple parser for array of objects)
            std::unordered_set<std::string> newTokens;
            std::string jsonStr = res->body;
            
            // Remove whitespace
            jsonStr.erase(std::remove_if(jsonStr.begin(), jsonStr.end(), ::isspace), jsonStr.end());
            
            // Simple JSON array parser for format: [{"token":"value1"},{"token":"value2"}]
            size_t pos = 0;
            while ((pos = jsonStr.find("\"token\":\"", pos)) != std::string::npos) {
                pos += 9; // Length of "token":"
                const size_t endPos = jsonStr.find("\"", pos);
                
                if (endPos != std::string::npos) {
                    const std::string token = jsonStr.substr(pos, endPos - pos);
                    if (!token.empty()) {
                        newTokens.insert(token);
                    }
                    pos = endPos;
                }
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

private:
    std::unordered_set<std::string> _validTokens;
    mutable std::shared_mutex _tokenMutex;
};

// Load balancer for backend servers
class LoadBalancer {
public:
    LoadBalancer() {
        // Initialize with backend servers
        // In production, these would be loaded from configuration
        _backends.emplace_back("localhost", 8081);
        _backends.emplace_back("localhost", 8082);
        _backends.emplace_back("localhost", 8083);
    }
    
    // Round-robin load balancing with health check
    const BackendServer* getNextBackend() const {
        const std::shared_lock<std::shared_mutex> lock(_backendMutex);
        
        if (_backends.empty()) {
            return nullptr;
        }
        
        const size_t startIndex = _currentIndex.load();
        size_t attempts = 0;
        
        while (attempts < _backends.size()) {
            const size_t index = (startIndex + attempts) % _backends.size();
            
            if (_backends[index].isHealthy) {
                _currentIndex = (index + 1) % _backends.size();
                return &_backends[index];
            }
            
            attempts++;
        }
        
        return nullptr; // No healthy backends available
    }
    
    void markUnhealthy(const std::string& host, const int port) {
        const std::unique_lock<std::shared_mutex> lock(_backendMutex);
        
        for (auto& backend : _backends) {
            if (backend.host == host && backend.port == port) {
                backend.isHealthy = false;
                break;
            }
        }
    }
    
    void markHealthy(const std::string& host, const int port) {
        const std::unique_lock<std::shared_mutex> lock(_backendMutex);
        
        for (auto& backend : _backends) {
            if (backend.host == host && backend.port == port) {
                backend.isHealthy = true;
                break;
            }
        }
    }
    
    std::vector<BackendServer> getBackends() const {
        const std::shared_lock<std::shared_mutex> lock(_backendMutex);
        return _backends;
    }

private:
    std::vector<BackendServer> _backends;
    mutable std::atomic<size_t> _currentIndex{0};
    mutable std::shared_mutex _backendMutex;
};

// HTTP Proxy Server class
class HttpProxy {
public:
    HttpProxy() {
        setupRoutes();
    }
    
    void setupRoutes() {
        // POST /query route
        _server.Post("/query", [this](const httplib::Request& req, httplib::Response& res) {
            _requestCount++;
            
            if (!validateAuth(req, res)) {
                return;
            }
            
            forwardRequest(req, res, "POST", "/query");
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
                if (i < backends.size() - 1) status << ",";
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
    
    void start(const std::string& host, const int port, const int numThreads) {
        std::cout << "Starting HTTP Proxy Server on " << host << ":" << port << std::endl;
        std::cout << "Using " << numThreads << " worker threads" << std::endl;
        
        // Configure server thread pool
        _server.new_task_queue = [numThreads] {
            return new httplib::ThreadPool(numThreads);
        };
        
        // Start the server
        if (!_server.listen(host.c_str(), port)) {
            std::cerr << "Failed to start server" << std::endl;
        }
    }
    
    void stop() {
        _server.stop();
    }
    
    LoadBalancer& getLoadBalancer() { return _loadBalancer; }
    const LoadBalancer& getLoadBalancer() const { return _loadBalancer; }
    
    TokenValidator& getTokenValidator() { return _tokenValidator; }
    const TokenValidator& getTokenValidator() const { return _tokenValidator; }

private:
    httplib::Server _server;
    mutable TokenValidator _tokenValidator;
    mutable LoadBalancer _loadBalancer;
    mutable std::atomic<uint64_t> _requestCount{0};
    mutable std::atomic<uint64_t> _errorCount{0};
    
    // Extract Bearer token from Authorization header
    std::string extractBearerToken(const httplib::Request& req) const {
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
    bool validateAuth(const httplib::Request& req, httplib::Response& res) const {
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
    bool forwardRequest(const httplib::Request& req, httplib::Response& res,
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
        client.set_connection_timeout(5, 0);  // 5 seconds
        client.set_read_timeout(30, 0);       // 30 seconds
        
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
            result = client.Post(path.c_str(), headers, req.body, 
                               req.get_header_value("Content-Type"));
        } else if (method == "GET") {
            result = client.Get(path.c_str(), headers);
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
            res.set_header(header.first.c_str(), header.second.c_str());
        }
        
        return true;
    }
};

// Health check thread for backend servers
void healthCheckThread(LoadBalancer* const lb, const std::atomic<bool>* const running) {
    while (running->load()) {
        const auto backends = lb->getBackends();
        
        for (const auto& backend : backends) {
            httplib::Client client(backend.host, backend.port);
            client.set_connection_timeout(2, 0);  // 2 seconds
            
            const auto res = client.Get("/health");
            
            if (res && res->status == 200) {
                lb->markHealthy(backend.host, backend.port);
            } else {
                lb->markUnhealthy(backend.host, backend.port);
            }
        }
        
        // Check every 10 seconds
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
}

// Token refresh thread for Supabase integration
void tokenRefreshThread(TokenValidator* const validator, const SupabaseConfig* const config, 
                       const std::atomic<bool>* const running) {
    // Initial fetch on startup
    validator->fetchTokensFromSupabase(*config);
    
    while (running->load()) {
        // Wait for 5 minutes before refreshing tokens
        for (int i = 0; i < 300 && running->load(); i++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        if (running->load()) {
            std::cout << "Refreshing tokens from Supabase..." << std::endl;
            if (!validator->fetchTokensFromSupabase(*config)) {
                std::cerr << "Failed to refresh tokens, keeping existing tokens" << std::endl;
            }
        }
    }
}

void Gateway::run() {
    // Determine number of threads based on hardware concurrency
    const unsigned int numThreads = []() {
        const unsigned int detected = std::thread::hardware_concurrency();
        return detected > 0 ? detected : 4u;  // Default to 4 if unable to detect
    }();
    
    std::cout << "Detected " << numThreads << " CPU cores" << std::endl;
    
    // Configure Supabase (in production, these would come from environment variables)
    const SupabaseConfig supabaseConfig(
        std::getenv("SUPABASE_URL") ? std::getenv("SUPABASE_URL") : "https://your-project.supabase.co",
        std::getenv("SUPABASE_API_KEY") ? std::getenv("SUPABASE_API_KEY") : "your-api-key",
        std::getenv("SUPABASE_TOKEN_TABLE") ? std::getenv("SUPABASE_TOKEN_TABLE") : "bearer_tokens"
    );
    
    // Create and start the proxy server
    HttpProxy proxy;
    
    // Start health check thread
    std::atomic<bool> healthCheckRunning{true};
    std::thread healthChecker(healthCheckThread, &proxy.getLoadBalancer(), &healthCheckRunning);
    
    // Start token refresh thread if Supabase is configured
    std::atomic<bool> tokenRefreshRunning{true};
    std::thread tokenRefresher;
    
    if (supabaseConfig.apiKey != "your-api-key") {
        std::cout << "Starting Supabase token synchronization..." << std::endl;
        tokenRefresher = std::thread(tokenRefreshThread, &proxy.getTokenValidator(), 
                                    &supabaseConfig, &tokenRefreshRunning);
    } else {
        std::cout << "Warning: Supabase not configured. Using default tokens only." << std::endl;
        std::cout << "Set SUPABASE_URL and SUPABASE_API_KEY environment variables to enable token sync." << std::endl;
    }
    
    // Handle graceful shutdown
    std::signal(SIGINT, [](int) {
        std::cout << "\nShutting down proxy server..." << std::endl;
        std::exit(0);
    });
    
    // Start the proxy server
    proxy.start("0.0.0.0", 8080, static_cast<int>(numThreads));
    
    // Cleanup
    if (healthChecker.joinable()) {
        healthChecker.join();
    }
    
    if (tokenRefresher.joinable()) {
        tokenRefresher.join();
    }
}
