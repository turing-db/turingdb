#include "APIServer.h"

#include "RPCServerConfig.h"
#include "RPCServer.h"

#include "APIServiceImpl.h"

APIServer::APIServer()
{
}

APIServer::~APIServer() {
}

void APIServer::run() {
    RPCServerConfig config;

    APIServiceImpl apiService;

    RPCServer rpcServer(config);
    rpcServer.addService(&apiService);
    rpcServer.run();
}
