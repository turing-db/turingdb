#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "FlowStatus.h"
#include "Utils.h"

namespace net {

class AbstractThreadContext;
class TCPConnectionStorage;
class TCPConnection;
using ServerProcessor = std::function<void(AbstractThreadContext*, TCPConnection&)>;
using CreateThreadContext = std::function<std::unique_ptr<AbstractThreadContext>()>;

struct ServerContext {
    utils::ServerSocket _socket {};
    utils::EpollInstance _instance {};
    utils::EpollSignal& _signalFd;
    TCPConnectionStorage& _connections;
    TCPConnection& _serverConnection;
    std::atomic<FlowStatus>& _status;
    std::atomic<bool>& _running;
    const ServerProcessor& _process;
    const CreateThreadContext& _createThreadContext;

    void encounteredError(FlowStatus err) {
        _status.store(err);
        _running.store(false);
    }
};

}
