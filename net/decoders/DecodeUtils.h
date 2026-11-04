#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "DecodeContext.h"
#include "GraphPath.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"

namespace net::proto {

inline void readPathNodeCount(DecodeContext* context, db::GraphPath& path) {
    WireSize nodeCount = 0;
    context->_inBuf->readData(&nodeCount, sizeof(nodeCount));

    constexpr size_t MAX_NODE_COUNT = (MAX_WIRE_SIZE / sizeof(uint64_t) + 1) / 2;
    if (nodeCount > MAX_NODE_COUNT) {
        throw TuringException("Invalid path node count " + std::to_string(nodeCount));
    }

    path.setNodeCount(nodeCount);
}

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
