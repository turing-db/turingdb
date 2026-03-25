#include "TuringServer.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "TCPServer.h"
#include "DBThreadContext.h"
#include "DBServerConfig.h"
#include "ThreadName.h"
#include "HTTPParser.h"
#include "DBURIParser.h"
#include "DBServerProcessor.h"
#include "TuringProtoParser.h"
#include "TuringProtoServerProcessor.h"
#include "TuringProtoWriter.h"

using namespace db;

namespace {

constexpr const char* THREAD_NAME = "turingdb.server";

bool isProtoEnabled() {
    const char* val = getenv("USE_TURING_PROTO");
    return val && strcmp(val, "1") == 0;
}

}

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
    const bool useProto = isProtoEnabled();

    net::TCPServer::Functions functions;
    functions._createThreadContext =
        [] {
            return std::unique_ptr<net::AbstractThreadContext>(new DBThreadContext());
        };

    if (useProto) {
        functions._processor =
            [&](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
                TuringProtoServerProcessor processor(_db, connection);
                processor.process(threadContext);
            };
        functions._createParser =
            [](net::NetBuffer* inputBuffer) {
                return std::unique_ptr<net::AbstractTCPParser>(new net::proto::TuringProtoParser(inputBuffer));
            };
        functions._createWriter =
            [] {
                return std::make_unique<net::proto::TuringProtoWriter>();
            };
    } else {
        functions._processor =
            [&](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
                DBServerProcessor processor(_db, connection);
                processor.process(threadContext);
            };
        functions._createParser =
            [](net::NetBuffer* inputBuffer) {
                return std::unique_ptr<net::AbstractTCPParser>(new net::HTTPParser<DBURIParser>(inputBuffer));
            };
        functions._createWriter =
            [] {
                return std::make_unique<net::HTTPWriter>();
            };
    }

    _server = std::make_unique<net::TCPServer>(std::move(functions));
    _server->setAddress(_config.getAddress().c_str());
    _server->setPort(_config.getPort());
    _server->setWorkerCount(_config.getWorkerCount());
    _server->setMaxConnections(_config.getMaxConnections());

    const auto initRes = _server->initialize();
    if (initRes != net::FlowStatus::OK) {
        _server->terminate();
        return;
    }

    const auto serverFunc = [&]() {
        ThreadName::set(THREAD_NAME);

        const auto startRes = _server->start();
        if (startRes != net::FlowStatus::OK) {
            _server->terminate();
            return;
        }
    };

    _serverThread = std::thread(serverFunc);

    spdlog::info("  - Server listening on address: {}:{} ({})",
                 _server->getAddress(),
                 _server->getPort(),
                 useProto ? "proto" : "http");
}

void TuringServer::wait() {
    if (_serverThread.joinable()) {
        _serverThread.join();
    }
}

void TuringServer::stop() {
    _server->terminate();
}
