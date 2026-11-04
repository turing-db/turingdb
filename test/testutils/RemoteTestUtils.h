#pragma once

#include <chrono>
#include <stdint.h>
#include <string>
#include <vector>

namespace turing::test {

// Reserve an ephemeral loopback port and immediately release it. The caller
// is responsible for binding it again before another process grabs it; there
// is a small TOCTOU window. Used by tests that spin up a real TuringServer
// to avoid hard-coded port collisions across parallel test runs.
[[nodiscard]] uint16_t reserveFreePort();

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
