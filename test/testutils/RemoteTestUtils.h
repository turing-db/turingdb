#pragma once

#include <chrono>
#include <stdint.h>
#include <string>
#include <vector>

namespace turing::test {

// A loopback port held by a socket that is bound but does not listen: a connection to it is
// refused, and no other process can take the port while this object lives.
class RefusedPort {
public:
    RefusedPort();
    explicit RefusedPort(uint16_t port);
    ~RefusedPort();

    RefusedPort(const RefusedPort&) = delete;
    RefusedPort& operator=(const RefusedPort&) = delete;

    uint16_t getPort() const { return _port; }

private:
    int _socket {-1};
    uint16_t _port {0};
};

// TuringServer::start() returns before the accept loop is guaranteed to be
// reachable. Poll-connect to the port until either a connect() succeeds or
// the timeout elapses. Returns true if the listener is reachable.
[[nodiscard]] bool waitUntilListening(uint16_t port, std::chrono::milliseconds timeout);

struct HttpRequest {
    std::string uri;
    std::string body;
};

struct HttpResponse {
    std::string statusLine;
    std::string headers;
    std::string body;
};

// Posts the body on a fresh connection and reads a chunked response.
void postHttpBody(uint16_t port, const std::string& uri, const std::string& body, HttpResponse& response);

// Posts the requests one after the other on one connection, reading each chunked response
// before sending the next.
void postHttpBodies(uint16_t port, const std::vector<HttpRequest>& requests, std::vector<HttpResponse>& responses);

// Sends a whole request, as written, on a fresh connection and reads a chunked response.
void sendHttpRequest(uint16_t port, const std::string& request, HttpResponse& response);

}
