#pragma once

#include <string>
#include <memory>

#include "DBServerConfig.h"

namespace db {
    class SystemManager;
    class DBServer;
}

class JobSystem;

class PipeSample {
public:
    PipeSample(const std::string& sampleName);
    ~PipeSample();

    const db::DBServerConfig& getServerConfig() const { return _serverConfig; }
    db::DBServer* getServer() const { return _server.get(); }

    std::string getTuringHome() const;

    bool loadJsonDB(const std::string& jsonDir);
    void createSimpleGraph();

    bool executeQuery(const std::string& queryStr);

    void startHttpServer();

private:
    std::string _sampleName;
    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<db::SystemManager> _system;
    db::DBServerConfig _serverConfig;
    std::unique_ptr<db::DBServer> _server;
};