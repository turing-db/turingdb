#pragma once

#include <memory>

namespace net {
class ServerConfig;
}

namespace workflow {

class WorkflowServerConfig {
public:
    using ServerConfig = net::ServerConfig;

    WorkflowServerConfig();
    ~WorkflowServerConfig();

    const ServerConfig& getServerConfig() const { return *_serverConfig; }

private:
    std::unique_ptr<ServerConfig> _serverConfig;
};

}
