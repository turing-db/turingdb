#include "StringBuffer.h"

using namespace db;

std::string_view StringBuffer::concatenate(std::string_view a, std::string_view b) {
    const size_t stringSize = a.size() + b.size();

    _buf.reserveContiguous(stringSize);

    const char* aPtr = a.data();
    const std::span aSpan(aPtr, a.size());

    const char* bPtr = b.data();
    const std::span bSpan(bPtr, b.size());

    std::string_view aSV = insert(aSpan);
    insert(bSpan);

    const char* stringStart = aSV.data();

    return {stringStart, stringSize};
}
