#include "RPCServer.h"

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include "RPCServerConfig.h"

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
        spdlog::error("RPC server failed to start on address {}", addrStr);
        return false;
    }

    spdlog::info("RPC server started on address {}", addrStr);

    server->Wait();
    return true;
}
