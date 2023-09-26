#pragma once

#include <string>

#include "ServerConfig.h"

class DBServerConfig {
public:
    using ServerConfig = turing::network::ServerConfig;

    DBServerConfig();
    ~DBServerConfig();

    const ServerConfig& getServerConfig() const { return _serverConfig; }

    const std::string& getDatabasesPath() const { return _databasesPath; }

private:
    ServerConfig _serverConfig;
    std::string _databasesPath;
};
