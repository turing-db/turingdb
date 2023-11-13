#include "RPCServer.h"

#include <grpcpp/grpcpp.h>

#include "RPCServerConfig.h"

#include "BioLog.h"
#include "MsgRPCServer.h"

using namespace Log;

RPCServer::RPCServer(const RPCServerConfig& config)
    : _config(config)
{
}

RPCServer::~RPCServer() {
}

void RPCServer::addService(grpc::Service* service) {
    _services.push_back(service);
}

bool RPCServer::run() {
    gpr_cpu_set_num_cores(_config.getConcurrency());

    grpc::ServerBuilder builder;
    builder.SetMaxSendMessageSize(-1);
    builder.SetMaxReceiveMessageSize(-1);

    // Configure server address and port
    const std::string addrStr = _config.getAddress() + ":" + std::to_string(_config.getPort());
    builder.AddListeningPort(addrStr, grpc::InsecureServerCredentials());

    // Register services
    for (auto service : _services) {
        builder.RegisterService(service);
    }

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        BioLog::log(msg::ERROR_RPC_SERVER_FAILED_TO_START() << addrStr);
        return false;
    }

    BioLog::log(msg::INFO_RPC_SERVER_STARTED() << addrStr);

    server->Wait();
    return true;
}
