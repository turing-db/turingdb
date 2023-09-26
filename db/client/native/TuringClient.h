#pragma once

#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "TuringConfig.h"

namespace turing::db::client {

class TuringConfig;

class TuringClient {
public:
    TuringClient();
    ~TuringClient();

    TuringConfig& getConfig() { return _config; }
    
    bool exec(const std::string& query);

private:
    TuringConfig _config;
    boost::asio::io_context _ioContext;
    boost::asio::ip::tcp::resolver _resolver;
    boost::beast::tcp_stream _stream;
    boost::beast::flat_buffer _buffer;
};

}
