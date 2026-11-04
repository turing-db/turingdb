#pragma once

#include <string_view>

namespace net::proto {

// The status line and headers of a response, read before its chunked body. A bad
// request gets a status other than 200 with a PROTOCOL_ERROR packet as its body.
class TuringResponseHead {
public:
    TuringResponseHead();
    ~TuringResponseHead();

    static void parse(std::string_view headerBlock, TuringResponseHead& head);

    void throwIfNoPackets() const;

private:
    int _statusCode {0};
    bool _carriesPackets {false};
};

}
