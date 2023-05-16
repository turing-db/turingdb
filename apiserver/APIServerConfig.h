#pragma once

#include <string>

class APIServerConfig {
public:
    APIServerConfig();
    ~APIServerConfig();

    const std::string& getListenAddr() const { return _listenAddr; }
    unsigned getPort() const { return _port; }
    bool isDebugEnabled() const { return _debugEnabled; }

    unsigned getThreadCount() const { return _threads; }

private:
    std::string _listenAddr {"127.0.0.1"};
    unsigned _port {6666};
    unsigned _threads {4};
    bool _debugEnabled {true};
};
