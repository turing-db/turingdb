#include "TuringServer.h"

#include <spdlog/spdlog.h>

#include "HTTPServer.h"
#include "HTTPParser.h"
#include "DBThreadContext.h"
#include "DBServerProcessor.h"
#include "DBURIParser.h"
#include "DBServerConfig.h"

using namespace db;

TuringServer::TuringServer(const DBServerConfig& config, TuringDB& db)
    : _config(config),
    _db(db)
{
}

TuringServer::~TuringServer() {
}

bool TuringServer::start() {
    net::HTTPServer::Functions functions {
        ._processor =
            [&](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
                DBServerProcessor processor(_db, connection);
                processor.process(threadContext);
            },
        ._createThreadContext =
            [] {
                return std::unique_ptr<net::AbstractThreadContext>(new DBThreadContext());
            },
        ._createHttpParser =
            [](net::NetBuffer* inputBuffer) {
                return std::unique_ptr<net::AbstractHTTPParser>(new net::HTTPParser<DBURIParser>(inputBuffer));
            },
    };

    auto server = std::make_unique<net::HTTPServer>(std::move(functions));
    server->setAddress(_config.getAddress().c_str());
    server->setPort(_config.getPort());
    server->setWorkerCount(_config.getWorkerCount());
    server->setMaxConnections(_config.getMaxConnections());

    spdlog::info("Server listening on address: {}:{}",
                 server->getAddress(),
                 server->getPort());

    const auto startRes = server->start();
    if (startRes != net::FlowStatus::OK) {
        spdlog::error("Failed to start server: {}", (uint32_t)startRes);
        server->terminate();
        return false;
    }

    server->terminate();

    return true;
}
