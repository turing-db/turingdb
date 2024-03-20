#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include "SessionHandler.h"

#include "BioAssert.h"

namespace net {

template <class ServerContextT, class SessionT>
class Listener {
public:
    using IOContext = boost::asio::io_context;
    using ServerEndpoint = boost::asio::ip::tcp::endpoint;

    Listener(IOContext& ioContext,
             ServerEndpoint& endpoint,
             ServerContextT* serverContext)
        : _ioContext(ioContext),
        _signals(_ioContext, SIGINT, SIGTERM),
        _acceptor(ioContext),
        _serverContext(serverContext)
    {
        // Open acceptor
        boost::system::error_code error;
        _acceptor.open(endpoint.protocol(), error);
        bioassert(!error);

        // Enable address reuse
        _acceptor.set_option(boost::asio::socket_base::reuse_address(true), error);
        bioassert(!error);

        // Bind to server address
        _acceptor.bind(endpoint, error);
        bioassert(!error); 

        _acceptor.listen(boost::asio::socket_base::max_listen_connections, error);
        bioassert(!error);
    }

    void start() {
        doSignalWait();
        doAccept();
    }

private:
    IOContext& _ioContext;
    boost::asio::signal_set _signals;
    boost::asio::ip::tcp::acceptor _acceptor;
    ServerContextT* _serverContext {nullptr};

    void doSignalWait() {
        _signals.async_wait(
            [this](const boost::system::error_code& error , int signal) {
                _ioContext.stop();
        });
    }

    void doAccept() {
        _acceptor.async_accept(_ioContext,
            [this](auto error, boost::asio::ip::tcp::socket&& socket) {
                onAccept(error, std::move(socket));
        });
    }

    void onAccept(boost::system::error_code error,
                  boost::asio::ip::tcp::socket socket) {
        if (error) {
            return;
        }

        auto sessionHandler = SessionHandler<ServerContextT, SessionT>::create(std::move(socket),
                                                                               _serverContext);
        sessionHandler->start();
        doAccept();
    }
};

}

