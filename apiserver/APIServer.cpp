#include "APIServer.h"

#include "crow.h"

#include "APIServerConfig.h"

APIServer::APIServer(const APIServerConfig& config)
    : _config(config)
{
}

APIServer::~APIServer() {
}

bool APIServer::run() {
    crow::SimpleApp app;

    CROW_ROUTE(app, "/")([]() {
            return "Hello world!";
    });

    auto logLevel = crow::LogLevel::Warning;
    if (_config.isDebugEnabled()) {
        logLevel = crow::LogLevel::Info;
    }

    app.bindaddr(_config.getListenAddr())
       .port(_config.getPort())
       .concurrency(_config.getThreadCount())
       .loglevel(logLevel)
       .run();

    return true;
}
