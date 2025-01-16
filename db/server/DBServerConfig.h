#pragma once

#include <string>

#include "RPCServerConfig.h"

class DBServerConfig {
public:
    DBServerConfig();
    ~DBServerConfig();

    const RPCServerConfig& getRPCConfig() const { return _rpcConfig; }
    RPCServerConfig& getRPCConfig() { return _rpcConfig; }

    const std::string& getDatabasesPath() const { return _databasesPath; }

private:
    RPCServerConfig _rpcConfig;
    std::string _databasesPath;
};
