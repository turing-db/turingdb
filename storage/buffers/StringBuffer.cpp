#include "StringBuffer.h"

#include <algorithm>

using namespace db;

std::string_view StringBuffer::concatenate(std::string_view a, std::string_view b) {
    const size_t totalSize = a.size() + b.size();

    _buf.reserveContiguous(totalSize);

    char* start = _buf.nextPtr();

    std::ranges::copy(a, start);
    std::ranges::copy(b, start + a.size());

    _buf.commit(totalSize);

    return {start, totalSize};
}

std::string_view StringBuffer::insert(std::string_view str) {
    return SpanBuffer::insert(std::span<const char> {str.data(), str.size()});
}


std::span<char> StringBuffer::allocate(size_t size) {
    return {allocUninit(size), size};
}
