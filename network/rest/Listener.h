#pragma once

#include <iostream>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "SessionHandler.h"

#include "BioAssert.h"

namespace net {

template <class ServerContextT>
class Listener {
public:
    using IOContext = boost::asio::io_context;
    using ServerEndpoint = boost::asio::ip::tcp::endpoint;
    using Session = SessionHandler<ServerContextT>;

    Listener(IOContext& ioContext,
             ServerEndpoint& endpoint,
             ServerContextT* serverContext)
        : _ioContext(ioContext),
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
        doAccept();
    }

private:
    IOContext& _ioContext;
    boost::asio::ip::tcp::acceptor _acceptor;
    ServerContextT* _serverContext {nullptr};

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

        auto session = Session::create(std::move(socket), _serverContext);
        session->start();
        doAccept();
    }
};

}

