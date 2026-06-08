#pragma once

#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <sys/uio.h>
#include <limits>
#include <span>
#include <string_view>
#include <type_traits>

namespace net::proto {

class TuringProtoOutBuf;

inline constexpr size_t DEFAULT_BUFFER_CAPACITY = 1024ul * 1024;

// Integer type used on the wire for every variable-length framing field of the binary protocol:
// the message payload length, column name lengths, per-value byte sizes, and element / row /
// column counts. Encoder and decoder both frame each such field as exactly sizeof(WireSize)
// bytes, so this single alias fixes the protocol's maximum size for any one field. To raise the
// limit, widen this alias here — and nowhere else.
using WireSize = uint32_t;

// Largest value any wire framing field can hold; checked before narrowing a size_t down to a
// WireSize when encoding.
inline constexpr size_t MAX_WIRE_SIZE = std::numeric_limits<WireSize>::max();

enum class MessageTypes : uint8_t {
    CHUNK_HEADER = 0,
    CHUNK,
    END_CHUNK,
    END,
    ERROR,
    PROTOCOL_ERROR,
    _SIZE
};

struct ProtoHeader {
    MessageTypes _type {};
    WireSize _dataLen {0};

    static consteval size_t wireSize() { return sizeof(_type) + sizeof(_dataLen); }
    static ProtoHeader decode(const char* data, size_t len);
    static bool isValidMessageType(MessageTypes type);
};

void frameMessage(MessageTypes type,
                  std::string_view payload,
                  TuringProtoOutBuf* outBuf);
void frameMessage(MessageTypes type,
                  std::span<char, ProtoHeader::wireSize()> headerSpan,
                  TuringProtoOutBuf* dataBuf,
                  std::span<iovec, 2> iovecs);

enum class ColumnInternalKind : uint32_t {
    UINT64 = 0,
    INT64,
    DOUBLE,
    STRING,
    BOOL,
    PATH,
    EMBEDDING,
    ENTITY_LIST,
    LIST_VIEW,
    VALUE_TYPE,
    NULL_VALUE,
    LIST_ELEMENT_VIEW,
    NODE_ID,
    EDGE_ID,
    EDGE_TYPE_ID,
    PROPERTY_TYPE_ID,
    LABEL_ID,
    LABEL_SET_ID,
    COMMIT_HASH,
    CHANGE_ID,
    PROPERTY_NULL,
};

enum class ColumnKind : uint8_t {
    VECTOR = 0,
    OPTIONAL_VECTOR,
    CONSTANT,
    OPTIONAL_CONSTANT,
};

// Only exists for the first chunk.
struct ColumnWireHeader {
    WireSize _nameLen {0};
    uint32_t _typeCode {0};
    uint8_t _encoding {0};

    [[nodiscard]] static consteval size_t wireSize() {
        return sizeof(_nameLen) + sizeof(_typeCode) + sizeof(_encoding);
    }

    void copyToBuffer(char* const buffer, size_t& offset) const {
        static_assert(std::is_trivially_copyable_v<ColumnWireHeader>);

        memcpy(buffer + offset, &_nameLen, sizeof(_nameLen));
        offset += sizeof(_nameLen);
        memcpy(buffer + offset, &_typeCode, sizeof(_typeCode));
        offset += sizeof(_typeCode);
        memcpy(buffer + offset, &_encoding, sizeof(_encoding));
        offset += sizeof(_encoding);
    }

    void copyFromBuffer(const char* const buffer, size_t& offset) {
        static_assert(std::is_trivially_copyable_v<ColumnWireHeader>);

        memcpy(&_nameLen, buffer + offset, sizeof(_nameLen));
        offset += sizeof(_nameLen);
        memcpy(&_typeCode, buffer + offset, sizeof(_typeCode));
        offset += sizeof(_typeCode);
        memcpy(&_encoding, buffer + offset, sizeof(_encoding));
        offset += sizeof(_encoding);
    }
};

}
