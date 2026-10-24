#include "TCPServer.h"

#include <exception>
#include <thread>
#include <netinet/in.h>
#include <unistd.h>
#include <spdlog/spdlog.h>

#include "AbstractThreadContext.h"
#include "TCPConnection.h"
#include "TCPConnectionManager.h"
#include "TCPConnectionStorage.h"
#include "TCPListener.h"
#include "SocketUtils.h"
#include "NetException.h"

#include "BioAssert.h"
#include "ThreadName.h"

using namespace net;

namespace {

constexpr const char* THREAD_NAME = "turingdb.http";

void closeFailedConnection(TCPConnection& connection, const char* reason) {
    spdlog::error("Closing connection after an unhandled exception: {}", reason);

    // The connection goes back to the storage pool on close and is handed out
    // again as it stands, so its writer and input buffer are reset the way the
    // connection manager's own error path leaves them; close() takes the parser.
    connection.getWriter().reset();
    connection.getInputBuffer().getWriter().reset();

    if (connection.getSocket() != 0) {
        connection.close();
    }
}

}

TCPServer::TCPServer(Functions&& functions)
    : _functions(std::move(functions))
{
}

TCPServer::~TCPServer() {
    if (_shutdownPipe[0] != -1) {
        ::close(_shutdownPipe[0]);
    }

    if (_shutdownPipe[1] != -1) {
        ::close(_shutdownPipe[1]);
    }
}

void TCPServer::initialize() {
    _serverSocket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (_serverSocket == -1) {
        throwInitializeError("create the socket");
    }

    if (!utils::setNonBlock(_serverSocket)) {
        throwInitializeError("set the socket non-blocking");
    }

    if (!utils::setReuseAddress(_serverSocket)) {
        throwInitializeError("set the socket to reuse its address");
    }

    if (!utils::bind(_serverSocket, _address, _port)) {
        throwInitializeError("bind");
    }

    if (!utils::listen(_serverSocket)) {
        throwInitializeError("listen");
    }

    _epollInstance = utils::createEventInstance();
    _connections = std::make_unique<TCPConnectionStorage>(_maxConnections);
    _connections->initialize(_functions._createParser,_functions._createWriter);
    _serverConnection = _connections->alloc(_serverSocket);

    // Registering server socket in epoll list
    utils::EpollEvent event;
    event._events = utils::EVENT_IN | utils::EVENT_ET | utils::EVENT_ONESHOT;
    event._data = _serverConnection;

    if (!utils::epollAdd(_epollInstance, _serverSocket, event)) {
        throwInitializeError("register the socket");
    }

    // Create shutdown pipe for reliable worker wakeup on terminate()
    if (::pipe(_shutdownPipe) < 0) {
        throwInitializeError("create the shutdown pipe");
    }

    if (!utils::setNonBlock(_shutdownPipe[0])) {
        throwInitializeError("set the shutdown pipe non-blocking");
    }

    if (!utils::setNonBlock(_shutdownPipe[1])) {
        throwInitializeError("set the shutdown pipe non-blocking");
    }

    event._events = utils::EVENT_IN;
    event._data = nullptr;
    if (!utils::epollAdd(_epollInstance, _shutdownPipe[0], event)) {
        throwInitializeError("register the shutdown pipe");
    }

    // Storing actual address
    sockaddr_in actualAddr {0};
    memset(&actualAddr, 0, sizeof(actualAddr));
    socklen_t actualAddrLen = sizeof(actualAddr);

    if (getsockname(_serverSocket, (struct sockaddr*)&actualAddr, &actualAddrLen) == -1) {
        throwInitializeError("read the bound address");
    }

    _actualAddress = utils::getStringAddress(actualAddr.sin_addr.s_addr);
    _running.store(true);
}

FlowStatus TCPServer::start() {
    ServerContext ctxt {
        ._socket = _serverSocket,
        ._instance = _epollInstance,
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
            db::ThreadName::set(THREAD_NAME);
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

void TCPServer::terminate() {
    if (!_running.load()) {
        return;
    }

    spdlog::info("Terminating {}", _name);
    _running.store(false);

    // Write to the shutdown pipe to wake all workers blocked in
    // epoll_wait/kevent. This is reliable on all platforms, unlike
    // SIGUSR1 which can be consumed before the thread enters the
    // event wait.
    if (_shutdownPipe[1] != -1) {
        const char buf = 1;
        if (::write(_shutdownPipe[1], &buf, 1) != sizeof(buf)) {
            utils::logError("shutdownPipe write");
        }
    }
}

void TCPServer::throwInitializeError(const char* step) const {
    const int error = errno;
    throw NetException(fmt::format("Could not start the {} on {}:{}: {}: {}", _name, _address, _port, step, strerror(error)));
}

void TCPServer::runThread(size_t threadID, ServerContext& ctxt) {
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
        if (!ctxt._running.load()) {
            return;
        }

        const int nfds = utils::eventWait(instance, events.data(), eventCount, 1000);

        if (!ctxt._running.load()) {
            return;
        }

        if (nfds <= 0) {
            continue;
        }

        for (int i = 0; i < nfds; i++) {
            utils::EpollEvent& ev = events[i];

            // Shutdown pipe
            if (!ctxt._running.load()) {
                return;
            }

            auto* connection = static_cast<TCPConnection*>(ev._data);

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

            try {
                connectionManager.process(threadContext.get(), ev);
            } catch (const std::exception& e) {
                closeFailedConnection(*connection, e.what());
            } catch (...) {
                closeFailedConnection(*connection, "unknown exception");
            }
        }
    }
}
