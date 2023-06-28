#pragma once

#include <vector>

namespace grpc {
class Server;
class Service;
}

class RPCServerConfig;

class RPCServer {
public:
    RPCServer(const RPCServerConfig& config);
    ~RPCServer();

    void addService(grpc::Service* service);

    void run();

private:
    const RPCServerConfig& _config;
    std::vector<grpc::Service*> _services;
};
