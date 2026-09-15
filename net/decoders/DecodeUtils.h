#pragma once

#include <stddef.h>
#include <string>

#include "DecodeContext.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"

namespace net::proto {

template <typename T>
inline size_t checkedElementCount(WireSize numBytes, const char* valueType) {
    if (numBytes % sizeof(T) != 0) {
        throw TuringException(std::string("Invalid ") + valueType + " byte size");
    }
    return numBytes / sizeof(T);
}

// Copies numBytes into the (stable) dest, queueing a resume through the
// context's buffer state if the payload is split across the buffer boundary.
inline bool readVarLenPayload(DecodeContext* context, char* dest, WireSize numBytes) {
    if (numBytes <= context->_inBuf->readable()) {
        context->_inBuf->readData(dest, numBytes);
        return true;
    }

    const size_t numBytesToRead = context->_inBuf->readable();
    context->_inBuf->readData(dest, numBytesToRead);

    context->_bufferState._start = dest;
    context->_bufferState._len = numBytes;
    context->_bufferState._offset = numBytesToRead;
    return false;
}

}
