#include "RemoteTestUtils.h"

#include <cstdlib>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>

#include "FatalException.h"
#include "TuringTime.h"

namespace turing::test {

namespace {

int connectTo(uint16_t port) {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket < 0) {
        throw FatalException("Could not create the client socket");
    }

    timeval timeout {};
    timeout.tv_sec = 30;
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);

    if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(socket);
        throw FatalException("Could not connect to the test server");
    }

    return socket;
}

void sendAll(int socket, const std::string& data) {
    size_t sent = 0;

    while (sent < data.size()) {
        const ssize_t bytes = ::send(socket, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (bytes <= 0) {
            throw FatalException(std::string("Request send failed: ") + strerror(errno));
        }

        sent += bytes;
    }
}

void receiveUntil(int socket, std::string& received, size_t size) {
    char buffer[4096];

    while (received.size() < size) {
        const ssize_t bytes = ::recv(socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            throw FatalException("Response ended after " + std::to_string(received.size())
                                 + " bytes: " + received);
        }

        received.append(buffer, bytes);
    }
}

size_t receiveLine(int socket, std::string& received, size_t position) {
    while (true) {
        const size_t lineEnd = received.find("\r\n", position);
        if (lineEnd != std::string::npos) {
            return lineEnd;
        }

        receiveUntil(socket, received, received.size() + 1);
    }
}

void sendPost(int socket, const HttpRequest& request) {
    std::string bytes = "POST " + request.uri + " HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: ";
    bytes += std::to_string(request.body.size());
    bytes += "\r\n\r\n";
    bytes += request.body;

    sendAll(socket, bytes);
}

// Reads the chunked response that starts at position, and leaves position past its last chunk
void receiveChunkedResponse(int socket, std::string& received, size_t& position, HttpResponse& response) {
    const size_t statusLineEnd = receiveLine(socket, received, position);
    response.statusLine = received.substr(position, statusLineEnd - position);

    size_t headerEnd = received.find("\r\n\r\n", position);
    while (headerEnd == std::string::npos) {
        receiveUntil(socket, received, received.size() + 1);
        headerEnd = received.find("\r\n\r\n", position);
    }

    const size_t headersBegin = statusLineEnd + 2;
    response.headers = received.substr(headersBegin, headerEnd + 2 - headersBegin);

    position = headerEnd + 4;
    while (true) {
        const size_t sizeLineEnd = receiveLine(socket, received, position);
        const size_t chunkSize = std::stoul(received.substr(position, sizeLineEnd - position), nullptr, 16);
        const size_t chunkBegin = sizeLineEnd + 2;
        receiveUntil(socket, received, chunkBegin + chunkSize + 2);
        position = chunkBegin + chunkSize + 2;

        if (chunkSize == 0) {
            break;
        }

        response.body.append(received, chunkBegin, chunkSize);
    }
}

}

uint16_t reserveFreePort() {
    const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        throw FatalException("Failed to create socket while reserving a test port");
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(sock, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        throw FatalException("Failed to bind test socket while reserving a test port");
    }

    sockaddr_in boundAddr {};
    socklen_t boundAddrLen = sizeof(boundAddr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&boundAddr), &boundAddrLen) != 0) {
        ::close(sock);
        throw FatalException("Failed to read back reserved test port");
    }

    ::close(sock);
    return ntohs(boundAddr.sin_port);
}

bool waitUntilListening(uint16_t port, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;

    while (Clock::now() < deadline) {
        const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sock >= 0) {
            sockaddr_in addr {};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);

            const int connectRes =
                ::connect(sock, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
            ::close(sock);

            if (connectRes == 0) {
                return true;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    return false;
}

void postHttpBody(uint16_t port, const std::string& uri, const std::string& body, HttpResponse& response) {
    std::vector<HttpResponse> responses;
    postHttpBodies(port, {HttpRequest {uri, body}}, responses);
    response = responses.front();
}

void postHttpBodies(uint16_t port, const std::vector<HttpRequest>& requests, std::vector<HttpResponse>& responses) {
    const int socket = connectTo(port);

    std::string received;
    size_t position = 0;
    try {
        for (const HttpRequest& request : requests) {
            sendPost(socket, request);
            receiveChunkedResponse(socket, received, position, responses.emplace_back());
        }
    } catch (...) {
        ::close(socket);
        throw;
    }

    ::close(socket);
}

void sendHttpRequest(uint16_t port, const std::string& request, HttpResponse& response) {
    const int socket = connectTo(port);

    std::string received;
    size_t position = 0;
    try {
        sendAll(socket, request);
        receiveChunkedResponse(socket, received, position, response);
    } catch (...) {
        ::close(socket);
        throw;
    }

    ::close(socket);
}

}
