#pragma once

#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <sys/uio.h>
#include <span>
#include <string_view>
#include <type_traits>

namespace net::proto {

class TuringProtoOutBuf;

inline constexpr size_t DEFAULT_BUFFER_CAPACITY = 1024ul * 1024;

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
    uint32_t _dataLen {0};

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
    uint32_t _nameLen {0};
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
