#include "DBServer.h"

#include "DBServerConfig.h"
#include "RPCServer.h"

#include "DBServiceImpl.h"

DBServer::DBServer(const DBServerConfig& serverConfig)
    : _config(serverConfig)
{
}

DBServer::~DBServer() {
}

bool DBServer::run() {
    RPCServer rpcServer(_config.getRPCConfig());
    DBServiceImpl apiService(_config);

    rpcServer.addService(&apiService);
    return rpcServer.run();
}
