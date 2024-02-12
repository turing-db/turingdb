#pragma once

namespace workflow {

class WorkflowServerConfig;

class WorkflowServer {
public:
    WorkflowServer(const WorkflowServerConfig& config);
    ~WorkflowServer();

    void start();

private:
    const WorkflowServerConfig& _config;
};

}
