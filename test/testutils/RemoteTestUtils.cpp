#include "RemoteTestUtils.h"

#include <cstdlib>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "FatalException.h"
#include "TuringTime.h"

namespace turing::test {

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

ProtoEnvScope::ProtoEnvScope() {
    const char* existing = ::getenv("USE_TURING_PROTO");
    _hadPrior = existing != nullptr;
    if (_hadPrior) {
        _prior = existing;
    }
    ::setenv("USE_TURING_PROTO", "1", 1);
}

ProtoEnvScope::~ProtoEnvScope() {
    if (_hadPrior) {
        ::setenv("USE_TURING_PROTO", _prior.c_str(), 1);
    } else {
        ::unsetenv("USE_TURING_PROTO");
    }
}

}
