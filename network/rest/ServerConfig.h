#pragma once

#include <string>

namespace net {

class ServerConfig {
public:
    ServerConfig();
    ~ServerConfig();

    const std::string& getAddress() const { return _address; }
    short unsigned getPort() const { return _port; }

    size_t getWorkersCount() const { return _workersCount; }

    void setPort(short unsigned port) { _port = port; }

private:
    const std::string _address {"127.0.0.1"};
    short unsigned _port {6666};
    size_t _workersCount {8};
};

}
