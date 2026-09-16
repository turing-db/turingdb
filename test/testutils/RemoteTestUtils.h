#pragma once

#include <chrono>
#include <stdint.h>
#include <string>

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

// USE_TURING_PROTO is read once during TuringServer::start() to choose the
// binary protocol over HTTP. RAII scope that sets it for the lifetime of the
// scope and restores any prior value on destruction so tests don't leak the
// setting to whatever runs next in the same process.
class ProtoEnvScope {
public:
    ProtoEnvScope();
    ~ProtoEnvScope();

    ProtoEnvScope(const ProtoEnvScope&) = delete;
    ProtoEnvScope(ProtoEnvScope&&) = delete;
    ProtoEnvScope& operator=(const ProtoEnvScope&) = delete;
    ProtoEnvScope& operator=(ProtoEnvScope&&) = delete;

private:
    bool _hadPrior {false};
    std::string _prior;
};

}
