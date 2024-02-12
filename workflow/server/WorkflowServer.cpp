#include "WorkflowServer.h"

#include "WorkflowServerHandler.h"
#include "WorkflowServerConfig.h"
#include "Server.h"

using namespace workflow;

WorkflowServer::WorkflowServer(const WorkflowServerConfig& config)
    : _config(config)
{
}

WorkflowServer::~WorkflowServer() {
}

void WorkflowServer::start() {
    WorkflowServerHandler serverHandler;
    net::Server server(&serverHandler, _config.getServerConfig());
    server.start();
}
