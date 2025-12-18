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
    if (_server) {
        _server->terminate();
    }
}

void TuringServer::start() {
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

    _server = std::make_unique<net::HTTPServer>(std::move(functions));
    _server->setAddress(_config.getAddress().c_str());
    _server->setPort(_config.getPort());
    _server->setWorkerCount(_config.getWorkerCount());
    _server->setMaxConnections(_config.getMaxConnections());

    const auto initRes = _server->initialize();
    if (initRes != net::FlowStatus::OK) {
        _server->terminate();
        return;
    }

    auto serverFunc = [&]() {
        const auto startRes = _server->start();
        if (startRes != net::FlowStatus::OK) {
            _server->terminate();
            return;
        }
    };

    _serverThread = std::thread(serverFunc);

    spdlog::info("Server listening on address: {}:{}",
                 _server->getAddress(),
                 _server->getPort());
}

void TuringServer::wait() {
    _serverThread.join();
    _server->terminate();
}

void TuringServer::stop() {
    _server->terminate();
    _serverThread.join();
}
