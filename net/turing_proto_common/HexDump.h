#pragma once

#include <stddef.h>
#include <string>

namespace net::proto {

[[nodiscard]] inline std::string hexDump(const char* data, size_t len) {
    static constexpr char kHex[] = "0123456789abcdef";

    std::string out;
    out.reserve(len * 3);
    for (size_t i = 0; i < len; ++i) {
        const unsigned char byte = static_cast<unsigned char>(data[i]);
        if (i > 0) {
            out.push_back(' ');
        }
        out.push_back(kHex[byte >> 4]);
        out.push_back(kHex[byte & 0x0f]);
    }
    return out;
}

}
