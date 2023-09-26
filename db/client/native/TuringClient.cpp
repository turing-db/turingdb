#include "TuringClient.h"

#include <iostream>

using namespace turing::db::client;
namespace http = boost::beast::http;

TuringClient::TuringClient()
    : _resolver(_ioContext),
    _stream(_ioContext)
{
}

TuringClient::~TuringClient() {
}

bool TuringClient::exec(const std::string& query) {
    boost::system::error_code error;

    const auto dnsResults = _resolver.resolve(_config.getAddress(),
                                              std::to_string(_config.getPort()),
                                              error);
    if (error) {
        return false;
    }

    _stream.connect(dnsResults, error);
    if (error) {
        return false;
    }

    http::request<http::string_body> req{http::verb::get, "/query", 11};
    http::write(_stream, req, error);
    if (error) {
        return false;
    }

    http::response<http::dynamic_body> res; 
    http::read(_stream, _buffer, res, error);
    if (error) {
        return false;
    }
    
    std::cout << res << '\n';

    _stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, error);
    if (error) {
        return false;
    }

    return true;
}
