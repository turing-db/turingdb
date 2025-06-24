#pragma once

#include <string>

struct BackendServer {
    const std::string host;
    const int port;
    bool isHealthy;

    BackendServer(const std::string& h, const int p)
        : host(h),
        port(p),
        isHealthy(true)
    {
    }
};
