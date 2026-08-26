#include "StringBuffer.h"

#include <string.h>

using namespace db;

std::string_view StringBuffer::concatenate(std::string_view a, std::string_view b) {
    const size_t totalSize = a.size() + b.size();

    _buf.reserveContiguous(totalSize);

    char* start = _buf.nextPtr();

    memcpy(start, a.data(), a.size());
    memcpy(start + a.size(), b.data(), b.size());

    _buf.commit(totalSize);

    return {start, totalSize};
}
