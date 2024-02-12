#include "WorkflowServerConfig.h"

#include "ServerConfig.h"

using namespace workflow;

WorkflowServerConfig::WorkflowServerConfig()
    : _serverConfig(std::make_unique<ServerConfig>())
{
    _serverConfig->setPort(6713);
}

WorkflowServerConfig::~WorkflowServerConfig() {
}
