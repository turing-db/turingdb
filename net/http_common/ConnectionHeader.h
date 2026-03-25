#pragma once

namespace net {

enum class ConnectionHeader {
    KEEP_ALIVE = 0,
    CLOSE
};

inline constexpr ConnectionHeader getConnectionHeader(bool closeConnection) {
    static_assert(static_cast<bool>(ConnectionHeader::KEEP_ALIVE) == false);
    static_assert(static_cast<bool>(ConnectionHeader::CLOSE) == true);

    return static_cast<ConnectionHeader>(closeConnection);
}

}
