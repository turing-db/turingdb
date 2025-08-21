#include <iostream>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <cstring>

#include <argparse.hpp>

#include "AddressRouter.h"
#include "HttpProxy.h"
#include "LoadBalancer.h"
#include "SupabaseConfig.h"
#include "TokenValidator.h"
#include "BackendServer.h"

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
        // Wait for 1 minute before refreshing tokens
        for (int i = 0; i < 60 && running->load(); i++) {
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

void routesRefreshThread(AddressRouter* const router, const SupabaseConfig* const config,
                         const std::atomic<bool>* const running) {
    // Initial fetch on startup
    router->fetchRoutesFromSupabase(*config);

    while (running->load()) {
        // Wait for 1 minute before refreshing tokens
        for (int i = 0; i < 60 && running->load(); i++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (running->load()) {
            std::cout << "Refreshing routes from Supabase..." << std::endl;
            if (!router->fetchRoutesFromSupabase(*config)) {
                std::cerr << "Failed to refresh routes, keeping existing routes" << std::endl;
            }
        }
    }
}

// Function to parse backend servers from file
void parseBackendServersFile(const std::string& filename, std::vector<std::pair<std::string, int>>& backends) {
    backends.clear();
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open backend servers file: " << filename << std::endl;
        return;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        // Parse host:port format
        const size_t colonPos = line.find(':');
        if (colonPos != std::string::npos) {
            const std::string host = line.substr(0, colonPos);
            const std::string portStr = line.substr(colonPos + 1);
            
            try {
                const int port = std::stoi(portStr);
                if (port > 0 && port <= 65535) {
                    backends.emplace_back(host, port);
                    std::cout << "Added backend server: " << host << ":" << port << std::endl;
                } else {
                    std::cerr << "Invalid port number in line: " << line << std::endl;
                }
            } catch (const std::exception& e) {
                std::cerr << "Invalid port format in line: " << line << std::endl;
            }
        } else {
            std::cerr << "Invalid format in line (expected host:port): " << line << std::endl;
        }
    }
    
    file.close();
}

// Function to print usage
void printUsage(const char* programName) {
    std::cout << "Usage: " << programName << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  -b, --backends <file>    File containing backend servers (one per line, format: host:port)" << std::endl;
    std::cout << "  -p, --port <port number>  Specify Port Number Of Gateway (defaults to 8080)" << std::endl;
    std::cout << "  -t, --tls <SSL Certificate File> <SSL Key File>  Run Gateway as a https server" << std::endl;
    std::cout << "  -h, --help               Show this help message" << std::endl;
}

int main(int argc, char* argv[]) {
    // Parse command line arguments
    std::string backendFile;
    std::string certFile;
    std::string keyFile;

    argparse::ArgumentParser gateway;
    gateway.add_argument("-b", "--backends")
        .help("File containing backend servers (one per line, format: host:port)")
        .metavar("FILE");

    gateway.add_argument("-p", "--port")
        .help("Specify Port Number Of Gateway")
        .default_value(8080)
        .scan<'i', int>()
        .metavar("PORT");

    gateway.add_argument("-t", "--tls")
        .help("Run Gateway as a https server")
        .nargs(2)
        .metavar("CERT_FILE KEY_FILE");

    try {
        gateway.parse_args(argc, argv);
    } catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << gateway;
        return 1;
    }

    if (gateway.is_used("--backends")) {
        std::string backendFile = gateway.get<std::string>("--backends");
    }

    int port = gateway.get<int>("--port");

    if (gateway.is_used("--tls")) {
        auto tlsFiles = gateway.get<std::vector<std::string>>("--tls");
        certFile = tlsFiles[0];
        keyFile = tlsFiles[1];
    }

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
        std::getenv("SUPABASE_TOKEN_TABLE") ? std::getenv("SUPABASE_TOKEN_TABLE") : "bearer_tokens",
        std::getenv("SUPABASE_TOKEN_INSTANCE_JOIN_FUNC") ? std::getenv("SUPABASE_TOKEN_INSTANCE_JOIN_FUNC") : "routes");

    // Create and start the proxy server
    std::unique_ptr<HttpProxyBase> proxy;

    if (!(certFile.empty() || keyFile.empty())) {
        proxy = std::make_unique<HttpProxy<httplib::SSLServer>>(certFile, keyFile);
        if (!proxy->isValid()) {
            std::cerr << "SSL server is not valid! Check certificate files." << std::endl;
            return 1;
        }
    } else {
        proxy = std::make_unique<HttpProxy<httplib::Server>>();
        if (!proxy->isValid()) {
            std::cerr << "Normal server isn't valid" << std::endl;
            return 1;
        }
    }

    if (!backendFile.empty()) {
        std::vector<std::pair<std::string, int>> backends;
        parseBackendServersFile(backendFile, backends);
        if (backends.empty()) {
            std::cerr << "Error: No valid backend servers found in file" << std::endl;
            return 1;
        }
        proxy->setBackendServers(backends);
    } else {
        std::cout << "Using default backend servers (localhost:8081, localhost:8082, localhost:8083)" << std::endl;
        std::vector<std::pair<std::string, int>> defaultBackends = {
            {"localhost", 8081},
            {"localhost", 8082},
            {"localhost", 8083}
        };
        proxy->setBackendServers(defaultBackends);
    }

    // Start health check thread
    std::atomic<bool> healthCheckRunning {true};
    std::thread healthChecker(healthCheckThread, &proxy->getLoadBalancer(), &healthCheckRunning);

    // Start token refresh thread if Supabase is configured
    std::atomic<bool> tokenRefreshRunning {true};
    std::thread tokenRefresher;

    std::atomic<bool> routesRefreshRunning {true};
    std::thread routesRefresher;

    if (supabaseConfig.apiKey != "your-api-key") {
        std::cout << "Starting Supabase token synchronization..." << std::endl;
        tokenRefresher = std::thread(tokenRefreshThread, &proxy->getTokenValidator(),
                                     &supabaseConfig, &tokenRefreshRunning);
        routesRefresher = std::thread(routesRefreshThread, &proxy->getAddressRouter(),
                                      &supabaseConfig, &routesRefreshRunning);
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
    proxy->start("0.0.0.0", port, static_cast<int>(numThreads));

    // Cleanup
    healthCheckRunning = false;
    tokenRefreshRunning = false;
    
    if (healthChecker.joinable()) {
        healthChecker.join();
    }
    
    if (tokenRefresher.joinable()) {
        tokenRefresher.join();
    }

    return 0;
}
