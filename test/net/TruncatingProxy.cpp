#include "TruncatingProxy.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <string_view>
#include <system_error>

#include "HTTPUtils.h"

using namespace turing::test;

namespace {

constexpr int RELAY_TIMEOUT_MILLISECONDS = 10000;

constexpr std::string_view HEADERS_END = "\r\n\r\n";

bool sendAll(int socket, const char* data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        const ssize_t bytes = ::send(socket, data + sent, size - sent, MSG_NOSIGNAL);
        if (bytes <= 0) {
            return false;
        }
        sent += static_cast<size_t>(bytes);
    }

    return true;
}

int connectToPort(uint16_t port) {
    const int connection = ::socket(AF_INET, SOCK_STREAM, 0);
    if (connection < 0) {
        return -1;
    }

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(connection, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(connection);
        return -1;
    }

    return connection;
}

// Relays one connection to the server. Past responseLimit bytes the client sees the
// connection close, while the server's response is still read whole into response
void relay(int client, int upstream, size_t responseLimit, std::string& response) {
    std::array<pollfd, 2> sockets {{{client, POLLIN, 0}, {upstream, POLLIN, 0}}};
    std::array<char, 4096> buffer {};

    while (true) {
        if (::poll(sockets.data(), sockets.size(), RELAY_TIMEOUT_MILLISECONDS) <= 0) {
            return;
        }

        if (sockets[0].revents != 0) {
            const ssize_t bytes = ::recv(client, buffer.data(), buffer.size(), 0);
            if (bytes <= 0 || !sendAll(upstream, buffer.data(), static_cast<size_t>(bytes))) {
                return;
            }
        }

        if (sockets[1].revents != 0) {
            const ssize_t bytes = ::recv(upstream, buffer.data(), buffer.size(), 0);
            if (bytes <= 0) {
                return;
            }

            const size_t forwardedSoFar = std::min(response.size(), responseLimit);
            response.append(buffer.data(), static_cast<size_t>(bytes));

            const size_t forwarded = std::min(response.size(), responseLimit) - forwardedSoFar;
            if (forwarded > 0 && !sendAll(client, response.data() + forwardedSoFar, forwarded)) {
                return;
            }

            const bool cut = response.size() >= responseLimit;
            if (cut && sockets[0].fd >= 0) {
                ::shutdown(client, SHUT_RDWR);
                sockets[0].fd = -1;
            }

            if (cut && response.ends_with(net::http::CHUNK_TERMINATOR)) {
                return;
            }
        }
    }
}

}

TruncatingProxy::TruncatingProxy()
{
}

TruncatingProxy::~TruncatingProxy() {
    stop();
}

void TruncatingProxy::start(uint16_t serverPort, size_t responseSize) {
    _responseSize = responseSize;

    _listener = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(_listener, 0);

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(_listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
    ASSERT_EQ(::listen(_listener, 4), 0);

    socklen_t addressSize = sizeof(address);
    ASSERT_EQ(::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &addressSize), 0);
    _port = ntohs(address.sin_port);

    _thread = std::thread(&TruncatingProxy::run, this, serverPort);
}

void TruncatingProxy::stop() {
    if (_thread.joinable()) {
        _stopping = true;

        // A shutdown of a listening socket does not wake accept on macOS; a connection does
        const int wakeup = connectToPort(_port);
        if (wakeup >= 0) {
            ::close(wakeup);
        }

        _thread.join();
    }

    if (_listener >= 0) {
        ::close(_listener);
        _listener = -1;
    }
}

void TruncatingProxy::findTruncatedPacket(TruncatedPacket& packet) const {
    const std::lock_guard<std::mutex> lock(_mutex);
    const std::string_view response = _firstResponse;

    const size_t headersEnd = response.find(HEADERS_END);
    if (headersEnd == std::string_view::npos) {
        return;
    }

    size_t offset = headersEnd + HEADERS_END.size();
    while (offset + net::http::CHUNK_HEADER_LINE_SIZE <= response.size()) {
        const char* digits = response.data() + offset;
        uint32_t chunkSize = 0;
        const std::from_chars_result parsed = std::from_chars(digits,
                                                              digits + net::http::CHUNK_HEX_DIGITS,
                                                              chunkSize,
                                                              16);
        if (parsed.ec != std::errc() || chunkSize == 0) {
            return;
        }

        const size_t bodyStart = offset + net::http::CHUNK_HEADER_LINE_SIZE;
        const size_t bodyEnd = bodyStart + chunkSize;
        if (bodyEnd > response.size()) {
            return;
        }

        const net::proto::MessageTypes type = static_cast<net::proto::MessageTypes>(response[bodyStart]);
        if (_responseSize >= bodyStart && _responseSize < bodyEnd) {
            packet._found = true;
            packet._type = type;
            return;
        } else if (type == net::proto::MessageTypes::CHUNK) {
            packet._chunkPacketsBefore++;
        }

        offset = bodyEnd + net::http::CHUNK_TRAILER_SIZE;
    }
}

void TruncatingProxy::run(uint16_t serverPort) {
    size_t responseLimit = _responseSize;

    while (true) {
        const int client = ::accept(_listener, nullptr, nullptr);
        if (client < 0) {
            return;
        } else if (_stopping) {
            ::close(client);
            return;
        }

        std::string response;
        const int upstream = connectToPort(serverPort);
        if (upstream >= 0) {
            relay(client, upstream, responseLimit, response);
            ::close(upstream);
        }
        ::close(client);

        if (responseLimit != SIZE_MAX) {
            const std::lock_guard<std::mutex> lock(_mutex);
            _firstResponse = response;
        }

        responseLimit = SIZE_MAX;
    }
}
