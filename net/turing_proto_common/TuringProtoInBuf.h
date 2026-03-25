#pragma once

#include <string.h>
#include <string>
#include <vector>

#include "HexDump.h"

namespace net::proto {

struct ProtoHeader;
struct ColumnWireHeader;
struct QueryWireHeader;

class TuringProtoInBuf {
public:
    explicit TuringProtoInBuf(size_t capacity);

    char* data() { return _data.data(); }
    char* end() { return _data.data() + _writeOffset; }
    const char* data() const { return _data.data(); }
    const char* end() const { return _data.data() + _writeOffset; }

    size_t size() const { return _writeOffset; }
    size_t capacity() const { return _data.size(); }
    size_t remaining() const { return _data.size() - _writeOffset; }
    size_t readable() const { return _writeOffset - _readOffset; }
    size_t readOffset() const { return _readOffset; }
    const char* readPtr() const { return _data.data() + _readOffset; }

    void reset() {
        _writeOffset = 0;
        _readOffset = 0;
    }
    void increaseWriteOffset(size_t increase) { _writeOffset += increase; }
    void increaseReadOffset(size_t increase) {
        ensureReadable(increase);
        _readOffset += increase;
    }

    std::string dump() const { return net::proto::hexDump(_data.data(), _writeOffset); }

    void readHeader(ProtoHeader* header);
    void readHeader(ColumnWireHeader* header);
    void readHeader(QueryWireHeader* header);

    void readData(void* dest, size_t len);
    void ensureReadable(size_t len) const;

private:
    std::vector<char> _data;
    size_t _writeOffset {0};
    size_t _readOffset {0};
};

}
