#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "IOContextThreadPool.h"
#include "Listener.h"
#include "ServerConfig.h"

#include "BioLog.h"

namespace net {

template <class ServerContextT, class SessionT>
class Server {
public:
    using ServerEndpoint = boost::asio::ip::tcp::endpoint;

    Server(ServerContextT* serverContext,
           const ServerConfig& serverConfig)
        : _endpoint(
            boost::asio::ip::address::from_string(serverConfig.getAddress()),
            serverConfig.getPort()
        ),
        _threadPool(serverConfig.getWorkersCount()),
        _listener(_threadPool.getIOContext(), _endpoint, serverContext)
    {
    }

    void start() {
        _listener.start();
        Log::BioLog::echo("Server started");
        _threadPool.run();
        _threadPool.waitForShutdown();
    }

    void shutdown() {
        _threadPool.shutdown();
    }

private:
    ServerEndpoint _endpoint;
    IOContextThreadPool _threadPool;
    Listener<ServerContextT, SessionT> _listener;
};

}


