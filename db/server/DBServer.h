#pragma once

class DBServerConfig;

class DBServer {
public:
    DBServer(const DBServerConfig& serverConfig);
    ~DBServer();

    void run();

private:
    const DBServerConfig& _config;
};
