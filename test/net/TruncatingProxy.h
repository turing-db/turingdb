#pragma once

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <mutex>
#include <string>
#include <thread>

#include "TuringProtoHeaders.h"

namespace turing::test {

// The proto packet whose HTTP chunk body holds the first byte the client did not receive
struct TruncatedPacket {
    bool _found {false};
    net::proto::MessageTypes _type {};
    size_t _chunkPacketsBefore {0};
};

// Relays each connection to a server in turn. The client of the first connection sees it
// close once responseSize bytes of the response have reached it
class TruncatingProxy {
public:
    TruncatingProxy();
    ~TruncatingProxy();

    void start(uint16_t serverPort, size_t responseSize);
    void stop();

    uint16_t getPort() const { return _port; }

    // The first response is whole only once a second connection has been relayed
    void findTruncatedPacket(TruncatedPacket& packet) const;

private:
    int _listener {-1};
    uint16_t _port {0};
    size_t _responseSize {0};
    std::atomic<bool> _stopping {false};
    std::thread _thread;
    mutable std::mutex _mutex;
    std::string _firstResponse;

    void run(uint16_t serverPort);
};

}
