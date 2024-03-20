#pragma once

#include <memory>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/streambuf.hpp>

#include "Buffer.h"
#include "NetWriter.h"
#include "HTTPParser.h"

#include "BioLog.h"

namespace net {

template <class ServerContextT, class SessionT>
class SessionHandler : public std::enable_shared_from_this<SessionHandler<ServerContextT, SessionT>> {
public:
    using TCPSocket = boost::asio::ip::tcp::socket;
    using std::enable_shared_from_this<SessionHandler<ServerContextT, SessionT>>::shared_from_this;

    ~SessionHandler() {
        Log::BioLog::echo("Session destroyed");
    }

    template <typename... Args>
    static std::shared_ptr<SessionHandler> create(Args&& ...args) {
        return std::shared_ptr<SessionHandler>(new SessionHandler(std::forward<Args>(args)...));
    }

    void start() {
        Log::BioLog::echo("Session started");

        boost::asio::dispatch(_strand,
            [shared_this=shared_from_this()]{ return shared_this->doRead(); }); 
    }

private:
    TCPSocket _socket;
    boost::asio::strand<TCPSocket::executor_type> _strand;
    ServerContextT* _serverContext {nullptr};
    Buffer _inputBuffer;
    NetWriter _writer;
    HTTPParser _parser;
    SessionT _session;

    SessionHandler(boost::asio::ip::tcp::socket&& socket, ServerContextT* serverContext)
        : _socket(std::move(socket)),
        _strand(boost::asio::make_strand(_socket.get_executor())),
        _serverContext(serverContext),
        _writer(_socket),
        _parser(&_inputBuffer),
        _session(_serverContext, &_writer)
    {
        _socket.lowest_layer().set_option(boost::asio::ip::tcp::no_delay(true));
        _socket.lowest_layer().set_option(boost::asio::socket_base::keep_alive(true));
        _socket.lowest_layer().non_blocking(false);
    }

    void doRead() {
        if (!isConnected()) {
            return;
        }

        Buffer::Writer bufWriter = _inputBuffer.getWriter();
        _socket.async_read_some(
            boost::asio::buffer(bufWriter.getBuffer(), bufWriter.getBufferSize()),
            boost::asio::bind_executor(_strand,
                std::bind_front(&SessionHandler::onRead, shared_from_this())));
    }

    void onRead(const boost::system::error_code& error,
                size_t bytesRead) {
        if (error) {
            return onError(error);
        }

        // Update input buffer on the number of bytes read
        _inputBuffer.getWriter().setWrittenBytes(bytesRead);

        // Analyse the request with the HTTP parser
        const bool analyzeComplete = _parser.analyze();

        if (analyzeComplete) {
            process();
        } else {
            doRead();
        }
    }


    void process() {
        _session.process(_parser.getURI(), _parser.getPayload());
        doShutdown();
    }

    void onError(const boost::system::error_code& error) {
        if (error == boost::asio::error::operation_aborted) {
            return;
        }

        doShutdown();
    }

    void doShutdown() {
        if (!isConnected()) {
            return;
        }

        boost::system::error_code error;

        auto& lowest_layer = _socket.lowest_layer();
        lowest_layer.shutdown(boost::asio::ip::tcp::socket::shutdown_both, error);
        if (error) {
            Log::BioLog::echo("Session error while shutdown");
        }
        lowest_layer.close();
    }

    bool isConnected() {
        return _socket.lowest_layer().is_open();
    }
};

}

