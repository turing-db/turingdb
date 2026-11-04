#include "TuringServer.h"

#include <spdlog/spdlog.h>

#include "TCPServer.h"
#include "DBThreadContext.h"
#include "DBServerConfig.h"
#include "ThreadName.h"
#include "HTTPParser.h"
#include "DBURIParser.h"
#include "TuringProtoServerProcessor.h"
#include "TuringProtoWriter.h"

using namespace db;

namespace {

constexpr const char* THREAD_NAME = "turingdb.server";

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

    if (_serverThread.joinable()) {
        _serverThread.join();
    }
}

void TuringServer::start() {
    net::TCPServer::Functions functions;
    functions._createThreadContext =
        [] {
            return std::unique_ptr<net::AbstractThreadContext>(new DBThreadContext());
        };
    functions._processor =
        [this](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
            TuringProtoServerProcessor processor(_db, connection);
            processor.process(threadContext);
        };
    functions._createParser = [](net::NetBuffer* inputBuffer) {
        return std::unique_ptr<net::AbstractTCPParser>(new net::HTTPParser<DBURIParser>(inputBuffer));
    };
    functions._createWriter = [bufferCapacity = _config.getProtoBufferCapacity()] {
        return std::make_unique<net::proto::TuringProtoWriter>(bufferCapacity);
    };

    _server = std::make_unique<net::TCPServer>(std::move(functions));
    _server->setAddress(_config.getAddress().c_str());
    _server->setPort(_config.getPort());
    _server->setWorkerCount(_config.getWorkerCount());
    _server->setMaxConnections(_config.getMaxConnections());

    _server->initialize();

    const auto serverFunc = [&]() {
        ThreadName::set(THREAD_NAME);

        const auto startRes = _server->start();
        if (startRes != net::FlowStatus::OK) {
            _server->terminate();
            return;
        }
    };

    _serverThread = std::thread(serverFunc);

    spdlog::info("  - Server listening on address: {}:{}", _server->getAddress(), _server->getPort());
}

void TuringServer::wait() {
    if (_serverThread.joinable()) {
        _serverThread.join();
    }
}

void TuringServer::stop() {
    _server->terminate();
}
