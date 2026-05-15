#include "TuringServer.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "TCPServer.h"
#include "DBThreadContext.h"
#include "DBServerConfig.h"
#include "ThreadName.h"
#include "H2ConnectionState.h"
#include "H2Parser.h"
#include "H2ProtoServerProcessor.h"
#include "H2Writer.h"
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

bool isH2Enabled() {
    const char* val = getenv("USE_TURING_H2");
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
    const bool useH2 = isH2Enabled();
    const bool useProto = !useH2 && isProtoEnabled();

    net::TCPServer::Functions functions;
    functions._createThreadContext =
        [] {
            return std::unique_ptr<net::AbstractThreadContext>(new DBThreadContext());
        };

    if (useH2) {
        functions._processor =
            [&](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
                H2ProtoServerProcessor processor(_db, connection);
                processor.process(threadContext);
            };
        functions._createParser = [](net::NetBuffer* inputBuffer, net::BaseConnectionState* state) {
            return std::unique_ptr<net::AbstractTCPParser>(new net::H2::H2Parser(inputBuffer, state));
        };
        functions._createWriter = [](net::BaseConnectionState* state) {
            return std::make_unique<net::H2::H2Writer>(state);
        };
        functions._createConnectionState = [] {
            return std::unique_ptr<net::BaseConnectionState>(new net::H2::H2ConnectionState());
        };
    } else if (useProto) {
        functions._processor =
            [&](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {
                TuringProtoServerProcessor processor(_db, connection);
                processor.process(threadContext);
            };
        functions._createParser = [](net::NetBuffer* inputBuffer, net::BaseConnectionState* /*state*/) {
            return std::unique_ptr<net::AbstractTCPParser>(new net::proto::TuringProtoParser(inputBuffer));
        };
        functions._createWriter = [bufferCapacity = _config.getProtoBufferCapacity()](net::BaseConnectionState* /*state*/) {
            return std::make_unique<net::proto::TuringProtoWriter>(bufferCapacity);
        };
    } else {
        functions._processor = [&](net::AbstractThreadContext* threadContext,
                                   net::TCPConnection& connection) {
            DBServerProcessor processor(_db, connection);
            processor.process(threadContext);
        };
        functions._createParser = [](net::NetBuffer* inputBuffer, net::BaseConnectionState* /*state*/) {
            return std::unique_ptr<net::AbstractTCPParser>(new net::HTTPParser<DBURIParser>(inputBuffer));
        };
        functions._createWriter = [](net::BaseConnectionState* /*state*/) {
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

    const char* transport = useH2 ? "h2" : (useProto ? "proto" : "http");
    spdlog::info("  - Server listening on address: {}:{} ({})",
                 _server->getAddress(),
                 _server->getPort(),
                 transport);
}

void TuringServer::wait() {
    if (_serverThread.joinable()) {
        _serverThread.join();
    }
}

void TuringServer::stop() {
    _server->terminate();
}
