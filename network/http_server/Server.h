#pragma once

#include <atomic>
#include <memory>

#include "AbstractHTTPParser.h"
#include "FlowStatus.h"
#include "ServerContext.h"
#include "Utils.h"

namespace net {

class TCPConnectionStorage;
class TCPConnection;

class Server {
public:
    struct Functions {
        ServerProcessor _processor;
        CreateThreadContext _createThreadContext;
        CreateAbstractHTTPParserFunc _createHttpParser;
    };
    explicit Server(Functions&&);
    ~Server();

    Server(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(const Server&) = delete;
    Server& operator=(Server&&) = delete;

    FlowStatus initialize();
    FlowStatus start();
    void terminate();

    void setAddress(const char* address) { _address = address; };
    void setPort(uint32_t port) { _port = port; };
    void setWorkerCount(uint32_t count) { _workerCount = count; };
    void setMaxConnections(uint32_t count) { _maxConnections = count; }

    std::string_view getAddress() const { return {_actualAddress.data(), _actualAddress.size()}; };
    uint32_t getPort() const { return _port; };

private:
    const char* _address = "127.0.0.1";
    uint32_t _port = 6666;
    uint32_t _workerCount = 8;
    uint32_t _maxConnections = 1024;

    utils::ServerSocket _serverSocket {};
    utils::EpollInstance _epollInstance {};
    utils::StringAddress _actualAddress {};
    utils::EpollSignal _signalFd {};
    std::unique_ptr<TCPConnectionStorage> _connections;
    TCPConnection* _serverConnection {nullptr};
    std::atomic<FlowStatus> _status;
    std::atomic<bool> _running = false;
    Functions _functions;

    static void runThread(size_t threadID, ServerContext& ctxt);
};

}
