#include "TuringProtoHeaders.h"

#include <string.h>

#include "TuringProtoOutBuf.h"

#include "BioAssert.h"

namespace net::proto {

bool ProtoHeader::isValidMessageType(MessageTypes type) {
    return type >= MessageTypes::CHUNK_HEADER && type < MessageTypes::_SIZE;
}

ProtoHeader ProtoHeader::decode(const char* data, size_t len) {
    bioassert(len >= wireSize(), "Not enough bytes to decode protocol header");

    ProtoHeader header {};
    size_t readOffset = 0;
    memcpy(&header._type, data + readOffset, sizeof(header._type));
    readOffset += sizeof(header._type);
    memcpy(&header._dataLen, data + readOffset, sizeof(header._dataLen));
    return header;
}

void frameMessage(MessageTypes type,
                  std::string_view payload,
                  TuringProtoOutBuf* outBuf) {
    bioassert(payload.size() <= std::numeric_limits<uint32_t>::max(), "Message payload exceeds uint32 maximum");
    const ProtoHeader header {
        ._type = type,
        ._dataLen = static_cast<uint32_t>(payload.size())};
    outBuf->copyHeader(&header);
    outBuf->copyVarLenData(payload.data(), payload.size());
}

void frameMessage(MessageTypes type,
                  std::span<char, ProtoHeader::wireSize()> headerSpan,
                  TuringProtoOutBuf* dataBuf,
                  std::span<iovec, 2> iovecs) {
    bioassert(dataBuf->size() <= std::numeric_limits<uint32_t>::max(), "Message data buffer exceeds uint32 maximum");
    const ProtoHeader header {
        ._type = type,
        ._dataLen = static_cast<uint32_t>(dataBuf->size())};

    const size_t sizeOfHeaderType = sizeof(header._type);

    memcpy(headerSpan.data(), &header._type, sizeOfHeaderType);
    memcpy(headerSpan.data() + sizeOfHeaderType, &header._dataLen, sizeof(header._dataLen));

    iovecs[0] = {headerSpan.data(), headerSpan.size()};
    iovecs[1] = {dataBuf->data(), dataBuf->size()};
}

}
