#include "HTTPServer.h"

#include <thread>
#include <signal.h>
#include <netinet/in.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

#include "AbstractThreadContext.h"
#include "TCPConnection.h"
#include "TCPConnectionManager.h"
#include "TCPConnectionStorage.h"
#include "TCPListener.h"

using namespace net;

HTTPServer::HTTPServer(Functions&& functions)
    : _functions(std::move(functions))
{
}

HTTPServer::~HTTPServer() {
}

FlowStatus HTTPServer::initialize() {
    _serverSocket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (_serverSocket == -1) {
        utils::logError("Create socket");
        return FlowStatus::CREATE_ERROR;
    }

    if (!utils::setNonBlock(_serverSocket)) {
        utils::logError("SetNonBlock server socket");
        return FlowStatus::OPT_NONBLOCK_ERR;
    }

    if (!utils::setReuseAddress(_serverSocket)) {
        utils::logError("ReuseAddress");
        return FlowStatus::OPT_REUSEADDR_ERR;
    }

    if (!utils::bind(_serverSocket, _address, _port)) {
        utils::logError("Bind");
        return FlowStatus::BIND_ERROR;
    }

    if (!utils::listen(_serverSocket)) {
        utils::logError("Listen");
        return FlowStatus::LISTEN_ERROR;
    }

    _epollInstance = utils::createEventInstance();
    _connections = std::make_unique<TCPConnectionStorage>(_maxConnections);
    _connections->initialize(std::move(_functions._createHttpParser));
    _serverConnection = _connections->alloc(_serverSocket);

    // Registering server socket in epoll list
    utils::EpollEvent event;
    event.events = utils::EVENT_IN | utils::EVENT_ET | utils::EVENT_ONESHOT;
    event.data = _serverConnection;

    if (!utils::epollAdd(_epollInstance, _serverSocket, event)) {
        utils::logError("EpollAdd root");
        return FlowStatus::CTL_ERROR;
    }

    // Registering sigint and sigterm as signals to listen to in the event loop
    _signalFd = utils::createSignalFd(_epollInstance);

#ifndef __APPLE__
    // On Linux, signalfd returns a real fd that needs to be registered
    event.events = utils::EVENT_IN;
    event.data = &_signalFd;
    if (!utils::epollAdd(_epollInstance, _signalFd, event)) {
        utils::logError("EpollAdd server terminate");
        _status.store(FlowStatus::CTL_ERROR);
    }
#endif

    // Storing actual address
    sockaddr_in actualAddr {};
    socklen_t actualAddrLen = sizeof(actualAddr);

    if (getsockname(_serverSocket, (struct sockaddr*)&actualAddr, &actualAddrLen) == -1) {
        utils::logError("GetServerAddress");
        return FlowStatus::CREATE_ERROR;
    }

    _actualAddress = utils::getStringAddress(actualAddr.sin_addr.s_addr);
    return FlowStatus::OK;
}

FlowStatus HTTPServer::start() {
    _running.store(true);

    ServerContext ctxt {
        ._socket = _serverSocket,
        ._instance = _epollInstance,
        ._signalFd = _signalFd,
        ._connections = *_connections,
        ._serverConnection = *_serverConnection,
        ._status = _status,
        ._running = _running,
        ._process = _functions._processor,
        ._createThreadContext = _functions._createThreadContext,
    };

    _threads.reserve(_workerCount);

    for (size_t i = 0; i < _workerCount; i++) {
        _threads.emplace_back([&ctxt, i] {
            runThread(i + 1, ctxt);
        });
    }

    for (auto& thread : _threads) {
        thread.join();
    }

    _threads.clear();

    ::close(_serverSocket);

    return FlowStatus::OK;
}

void HTTPServer::terminate() {
    if (!_running.load()) {
        return;
    }

    spdlog::info("Terminating server");
    _running.store(false);

    // Terminate all threads
    for (auto& thread : _threads) {
        pthread_kill(thread.native_handle(), SIGTERM);
    }
}

void HTTPServer::runThread(size_t threadID, ServerContext& ctxt) {
    constexpr size_t eventCount = 5;
    std::vector<utils::EpollEvent> events(eventCount);

    const utils::ServerSocket server = ctxt._socket;
    const utils::EpollInstance instance = ctxt._instance;

    TCPListener listener(ctxt);
    TCPConnectionManager connectionManager(ctxt);

    auto threadContext = ctxt._createThreadContext();
    bioassert(threadContext, "createThreadContext function was not set");
    threadContext->setThreadID(threadID);

    for (;;) {
        const int nfds = utils::eventWait(instance, events.data(), eventCount, -1);
        if (nfds <= 0) {
            utils::logError("EpollWait");
            ctxt._status.store(FlowStatus::WAIT_ERROR);
        }

        for (int i = 0; i < nfds; i++) {
            utils::EpollEvent& ev = events[i];

            // On macOS, signals come with nullptr data; on Linux, they come with &signalFd
            if (!ctxt._running.load() || ev.data == nullptr || ev.data == &ctxt._signalFd) {
                return;
            }

            auto* connection = static_cast<TCPConnection*>(ev.data);

            if (!connection) {
                // Terminate requested, re-trigger the event to stop other threads
                // until they are all stopped
                if (!utils::epollMod(ctxt._instance, 0, ev)) {
                    utils::logError("EpollMod server terminate");
                    ctxt.encounteredError(FlowStatus::CTL_ERROR);
                }
                return;
            }

            if (connection->getSocket() == server) {
                // Accept Connection
                listener.accept(ev);
                continue;
            }

            connectionManager.process(threadContext.get(), ev);
        }
    }
}
