#include "APIServer.h"

#include "crow.h"

#include "AuthGuard.h"
#include "AuthRegister.h"

#include "APIServerConfig.h"

APIServer::APIServer(const APIServerConfig& config)
    : _config(config)
{
}

APIServer::~APIServer() {
}

bool APIServer::run() {
    AuthRegister authRegister(_config);

    crow::App<AuthGuard> app;

    AuthGuard& authGuard = app.get_middleware<AuthGuard>();
    authGuard.setRegister(&authRegister);

    CROW_ROUTE(app, "/")
    .methods("POST"_method)
    ([](const crow::request& req, crow::response& resp) {
        resp.code = 200;
        resp.end();
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
