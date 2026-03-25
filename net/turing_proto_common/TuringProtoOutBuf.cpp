#include "TuringProtoOutBuf.h"

#include "TuringProtoHeaders.h"

#include "BioAssert.h"

using namespace net::proto;

TuringProtoOutBuf::TuringProtoOutBuf(size_t capacity)
    : _onBufferFull(&TuringProtoOutBuf::assertOnBufferFull),
      _data(capacity)
{
}

TuringProtoOutBuf::~TuringProtoOutBuf() {
}

void TuringProtoOutBuf::setOnBufferFullCallBack(std::function<void()> callBack) {
    bioassert(callBack, "TuringProtoOutBuf callback must be non-empty");
    _onBufferFull = callBack;
}

void TuringProtoOutBuf::assertOnBufferFull() {
    bioassert(false, "TuringProtoOutBuf: onBufferFull triggered but no callback was configured");
}

void TuringProtoOutBuf::checkRemainingAndFlush(size_t size) {
    if (remaining() < size) {
        _onBufferFull();
    }
}

void TuringProtoOutBuf::copyHeader(const ProtoHeader* header) {
    if (ProtoHeader::wireSize() > remaining()) {
        _onBufferFull();
    }

    copy(&header->_type, sizeof(header->_type));
    copy(&header->_dataLen, sizeof(header->_dataLen));
}

void TuringProtoOutBuf::copyHeader(const net::proto::ColumnWireHeader* header) {
    if (ColumnWireHeader::wireSize() > remaining()) {
        _onBufferFull();
    }

    copy(&header->_nameLen, sizeof(header->_nameLen));
    copy(&header->_typeCode, sizeof(header->_typeCode));
    copy(&header->_encoding, sizeof(header->_encoding));
}

void TuringProtoOutBuf::copyHeader(const net::proto::QueryWireHeader* header) {
    if (QueryWireHeader::wireSize() > remaining()) {
        _onBufferFull();
    }

    copy(&header->_commitHash, sizeof(header->_commitHash));
    copy(&header->_changeID, sizeof(header->_changeID));
    copy(&header->_graphNameLen, sizeof(header->_graphNameLen));
    copy(&header->_queryLen, sizeof(header->_queryLen));
}

void TuringProtoOutBuf::copyVarLenData(const void* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        const size_t toCopy = std::min(len - offset, remaining());
        copy(static_cast<const char*>(data) + offset, toCopy);

        if (remaining() == 0) {
            _onBufferFull();
        }

        offset += toCopy;
    }
}

/*
 * When copying over fixed length data we don't want to partially copy over the data if the
 * buffer is full - so we flush and continue the encoding.
 */
void TuringProtoOutBuf::copyFixedLenData(const void* data, size_t len) {
    bioassert(len < _data.size(), "Sending Fixed length data that is more than buffer size");

    if (len > remaining()) {
        _onBufferFull();
    }

    copy(data, len);
}
