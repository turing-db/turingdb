#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "AbstractTCPParser.h"
#include "BaseConnectionState.h"
#include "FlowStatus.h"
#include "ServerContext.h"
#include "SocketUtils.h"

namespace net {

class TCPConnectionStorage;
class TCPConnection;

class TCPServer {
public:
    struct Functions {
        ServerProcessor _processor;
        CreateThreadContext _createThreadContext;
        CreateAbstractTCPParserFunc _createParser;
        CreateAbstractTCPWriterFunc _createWriter;
        // Optional. When unset, TCPConnectionStorage falls back to a plain
        // BaseConnectionState — what the HTTP and binary-proto transports
        // need. Transports that require a derived state (e.g. h2's nghttp2
        // session) supply this factory.
        CreateConnectionStateFunc _createConnectionState;
    };

    explicit TCPServer(Functions&&);
    ~TCPServer();

    TCPServer(const TCPServer&) = delete;
    TCPServer(TCPServer&&) = delete;
    TCPServer& operator=(const TCPServer&) = delete;
    TCPServer& operator=(TCPServer&&) = delete;

    FlowStatus initialize();
    FlowStatus start();
    void terminate();

    void setAddress(const char* address) { _address = address; };
    void setPort(uint32_t port) { _port = port; };
    void setWorkerCount(uint32_t count) { _workerCount = count; };
    void setMaxConnections(uint32_t count) { _maxConnections = count; }
    void setName(const char* name) { _name = name; }

    std::string_view getAddress() const { return {_actualAddress.data()}; };
    uint32_t getPort() const { return _port; };

private:
    const char* _name {"server"};
    const char* _address {"127.0.0.1"};
    uint32_t _port {6666};
    uint32_t _workerCount {8};
    uint32_t _maxConnections {1024};

    utils::ServerSocket _serverSocket {0};
    utils::EpollInstance _epollInstance {0};
    utils::StringAddress _actualAddress {};
    int _shutdownPipe[2] {-1, -1};
    std::unique_ptr<TCPConnectionStorage> _connections;
    TCPConnection* _serverConnection {nullptr};
    std::atomic<FlowStatus> _status;
    std::atomic<bool> _running = false;
    Functions _functions;
    std::vector<std::thread> _threads;

    static void runThread(size_t threadID, ServerContext& ctxt);
};
}
