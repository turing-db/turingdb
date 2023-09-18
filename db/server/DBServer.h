#pragma once

class DBServerConfig;

class DBServer {
public:
    DBServer(const DBServerConfig& serverConfig);
    ~DBServer();

    bool run();

private:
    const DBServerConfig& _config;
};
