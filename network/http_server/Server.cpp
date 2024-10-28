#include "Server.h"

#include <thread>
#include <csignal>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <spdlog/spdlog.h>

#include "AbstractThreadContext.h"
#include "TCPConnection.h"
#include "TCPConnectionManager.h"
#include "TCPConnectionStorage.h"
#include "TCPListener.h"

using namespace net;

Server::Server(Functions&& functions)
    : _functions(std::move(functions))
{
}

Server::~Server() = default;

FlowStatus Server::initialize() {
    _serverSocket = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);

    if (_serverSocket == -1) {
        utils::logError("Create socket");
        return FlowStatus::CREATE_ERROR;
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

    _epollInstance = ::epoll_create1(0);
    _connections = std::make_unique<TCPConnectionStorage>(_maxConnections);
    _connections->initialize(std::move(_functions._createHttpParser));
    _serverConnection = _connections->alloc(_serverSocket);

    // Registering server socket in epoll list
    utils::EpollEvent event {
        .events = EPOLLIN | EPOLLET | EPOLLONESHOT,
        .data = {.ptr = _serverConnection},
    };

    if (!utils::epollAdd(_epollInstance, _serverSocket, event)) {
        utils::logError("EpollAdd root");
        return FlowStatus::CTL_ERROR;
    }

    // Registering sigint and sigterm as signals to listen to in the epoll list
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);

    if (sigprocmask(SIG_BLOCK, &mask, 0)) {
        utils::logError("Sig block setting");
    }

    _signalFd = signalfd(-1, &mask, 0);

    event.events = EPOLLIN;
    event.data.ptr = &_signalFd;
    if (!utils::epollAdd(_epollInstance, _signalFd, event)) {
        utils::logError("EpollAdd server terminate");
        _status.store(FlowStatus::CTL_ERROR);
    }

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

FlowStatus Server::start() {
    _running.store(true);

    std::vector<std::thread> threads;
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

    threads.reserve(_workerCount);

    for (size_t i = 0; i < _workerCount; i++) {
        threads.emplace_back([&ctxt, i] {
            runThread(i + 1, ctxt);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ::close(_serverSocket);

    return FlowStatus::OK;
}

void Server::terminate() {
    spdlog::info("Terminating server");
    _running.store(false);
}

void Server::runThread(size_t threadID, ServerContext& ctxt) {
    constexpr size_t eventCount = 5;
    std::vector<utils::EpollEvent> events(eventCount);

    utils::ServerSocket server = ctxt._socket;
    utils::EpollInstance instance = ctxt._instance;

    TCPListener listener(ctxt);
    TCPConnectionManager connectionManager(ctxt);

    auto threadContext = ctxt._createThreadContext();
    msgbioassert(threadContext, "createThreadContext function was not set");
    threadContext->setThreadID(threadID);

    for (;;) {
        const int nfds = epoll_wait(instance, events.data(), eventCount, -1);
        if (nfds <= 0) {
            utils::logError("EpollWait");
            ctxt._status.store(FlowStatus::WAIT_ERROR);
        }

        for (int i = 0; i < nfds; i++) {
            utils::EpollEvent& ev = events[i];

            if (!ctxt._running.load() || ev.data.ptr == &ctxt._signalFd) {
                return;
            }

            auto* connection = static_cast<TCPConnection*>(ev.data.ptr);

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
