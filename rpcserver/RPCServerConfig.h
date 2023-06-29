#pragma once

#include <string>

class RPCServerConfig {
public:
    RPCServerConfig();
    ~RPCServerConfig();

    const std::string& getAddress() const { return _serverAddr; }
    unsigned getPort() const { return _port; }
    unsigned getConcurrency() const { return _concurrency; }

    void setAddress(const std::string& addr) { _serverAddr = addr; }
    void setPort(unsigned port) { _port = port; }
    void setConcurrency(unsigned concurrency) { _concurrency = concurrency; }

private:
    std::string _serverAddr {"127.0.0.1"};
    unsigned _port {6666};
    unsigned _concurrency {1};
};
