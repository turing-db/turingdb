#pragma once

#include <string.h>
#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "HexDump.h"
#include "TuringProtoHeaders.h"

namespace net::proto {

class TuringProtoOutBuf {
public:
    explicit TuringProtoOutBuf(size_t capacity);
    ~TuringProtoOutBuf();

    char* data() { return _data.data(); }
    char* end() { return _data.data() + _offset; }
    const char* data() const { return _data.data(); }
    const char* end() const { return _data.data() + _offset; }

    size_t size() const { return _offset; }
    size_t capacity() const { return _data.size(); }
    size_t remaining() { return _data.size() - _offset; }

    void reset() { _offset = 0; }
    void increaseOffset(size_t increase) { _offset += increase; }

    std::string dump() const { return net::proto::hexDump(_data.data(), _offset); }

    void setOnBufferFullCallBack(std::function<void()> callBack);

    void copyHeader(const ProtoHeader* header);
    void copyHeader(const ColumnWireHeader* header);
    void copyHeader(const QueryWireHeader* header);

    void copyVarLenData(const void* data, size_t len);
    void copyFixedLenData(const void* data, size_t len);

    void checkRemainingAndFlush(size_t size);

    template <typename T>
    void copyVector(const void* data, size_t len) {
        size_t offset = 0;

        while (offset != len) {
            if (remaining() < sizeof(T)) {
                _onBufferFull();
            }

            const size_t sendableLen = sizeof(T) * (std::min(remaining(), len - offset) / sizeof(T));
            copy(static_cast<const char*>(data) + offset, sendableLen);

            offset += sendableLen;
        }
    }

private:
    std::function<void()> _onBufferFull;
    std::vector<char> _data;
    size_t _offset {0};

    void copy(const void* data, const size_t len) {
        memcpy(end(), data, len);
        _offset += len;
    }

    // Default onBufferFull for the capacity-only constructor: asserts instead of
    // silently firing an empty std::function (which would raise bad_function_call).
    // A buffer-full with no callback set is always a server programmer error.
    static void assertOnBufferFull();
};

}
