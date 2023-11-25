#pragma once

#include <memory>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "Buffer.h"
#include "HTTPParser.h"

#include "BioLog.h"

namespace net {

template <class ServerContextT>
class SessionHandler : public std::enable_shared_from_this<SessionHandler<ServerContextT>> {
public:
    using TCPSocket = boost::asio::ip::tcp::socket;
    using std::enable_shared_from_this<SessionHandler<ServerContextT>>::shared_from_this;

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
    Buffer _outBuffer;
    HTTPParser _parser;
    boost::asio::deadline_timer _timer;

    SessionHandler(boost::asio::ip::tcp::socket&& socket, ServerContextT* serverContext)
        : _socket(std::move(socket)),
        _strand(boost::asio::make_strand(_socket.get_executor())),
        _serverContext(serverContext),
        _parser(&_inputBuffer),
        _timer(_socket.get_executor())
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

        _inputBuffer.getWriter().setWrittenBytes(bytesRead);
        const bool completed = process();
        if (completed) {
            const Buffer::Reader reader = _outBuffer.getReader();
            write(reader.getData(), reader.getSize());
            doShutdown();
        } else {
            doRead();
        }
    }

    bool write(const char* data, size_t size) {
        if (!isConnected()) {
            return false;
        }

        boost::system::error_code error;

        const char* dataPtr = data;
        size_t remainingBytes = size;
        while (remainingBytes > 0) {
            const auto sent = _socket.send(boost::asio::buffer(dataPtr, remainingBytes),
                                           MSG_NOSIGNAL,
                                           error);
            if (error) {
                onError(error);
                return false;
            }

            dataPtr += sent;
            remainingBytes -= sent;
        }

        return true;
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

    bool process() {
        const bool analyzeComplete = _parser.analyze();
        if (!analyzeComplete) {
            return false;
        }

        _serverContext->process(&_outBuffer, _parser.getURI(), _parser.getPayload());
        return true;
    }
};

}

