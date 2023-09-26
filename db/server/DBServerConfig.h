#pragma once

#include <string>

#include "ServerConfig.h"

namespace db {

class DBServerConfig {
public:
    using ServerConfig = net::ServerConfig;

    DBServerConfig();
    ~DBServerConfig();

    const ServerConfig& getServerConfig() const { return _serverConfig; }

    const std::string& getDatabasesPath() const { return _databasesPath; }

private:
    ServerConfig _serverConfig;
    std::string _databasesPath;
};

}
