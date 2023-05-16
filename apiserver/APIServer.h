#pragma once

class APIServerConfig;

class APIServer {
public:
    APIServer(const APIServerConfig& config);
    ~APIServer();

    bool run();

private:
    const APIServerConfig& _config;
};
