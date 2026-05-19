#include "TuringProtoInBuf.h"

#include "TuringProtoHeaders.h"
#include "TuringException.h"

using namespace net::proto;

TuringProtoInBuf::TuringProtoInBuf(size_t capacity)
    : _data(capacity)
{
}

void TuringProtoInBuf::ensureReadable(size_t len) const {
    if (_readOffset + len > _writeOffset) {
        throw TuringException("TuringProtoInBuf: read past end of received data");
    }
}

void TuringProtoInBuf::readHeader(ProtoHeader* header) {
    ensureReadable(ProtoHeader::wireSize());

    memcpy(&header->_type, readPtr(), sizeof(header->_type));
    _readOffset += sizeof(header->_type);
    memcpy(&header->_dataLen, readPtr(), sizeof(header->_dataLen));
    _readOffset += sizeof(header->_dataLen);
}

void TuringProtoInBuf::readHeader(ColumnWireHeader* header) {
    ensureReadable(ColumnWireHeader::wireSize());

    memcpy(&header->_nameLen, readPtr(), sizeof(header->_nameLen));
    _readOffset += sizeof(header->_nameLen);
    memcpy(&header->_typeCode, readPtr(), sizeof(header->_typeCode));
    _readOffset += sizeof(header->_typeCode);
    memcpy(&header->_encoding, readPtr(), sizeof(header->_encoding));
    _readOffset += sizeof(header->_encoding);
}

void TuringProtoInBuf::readData(void* dest, size_t len) {
    ensureReadable(len);

    memcpy(dest, readPtr(), len);
    _readOffset += len;
}
