#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "BackendServer.h"

class LoadBalancer {
public:
    LoadBalancer() {
        // No default backend servers
    }

    void setBackendServers(const std::vector<std::pair<std::string, int>>& backendList) {
        const std::unique_lock<std::shared_mutex> lock(_backendMutex);
        _backends.clear();
        for (const auto& [host, port] : backendList) {
            _backends.emplace_back(host, port);
        }
        _currentIndex = 0;
    }

    // Round-robin load balancing with health check
    const BackendServer* getNextBackend() const {
        const std::shared_lock<std::shared_mutex> lock(_backendMutex);

        if (_backends.empty()) {
            return nullptr;
        }

        const size_t index = _currentIndex.load();
        _currentIndex = (index + 1) % _backends.size();

        return &_backends[index];
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
    mutable std::atomic<size_t> _currentIndex {0};
    mutable std::shared_mutex _backendMutex;
};
