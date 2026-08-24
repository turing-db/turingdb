#include "ListWriteCursor.h"

#include <cstring>

#include "list/ListBuffer.h"
#include "metadata/PropertyType.h"

using namespace db;

ListElementView ListWriteCursor::writeRaw(const void* data, size_t numBytes) {

    std::byte* start = _elementWritePtr;

    std::memcpy(start, data, numBytes);
    _elementWritePtr += numBytes;

    return recordView(start);
}

template <typename T>
ListElementView ListWriteCursor::writeValue(ListBufferTypeTag tag, const T& value) {
    constexpr size_t tagSize = sizeof(ListBufferTypeTag);

    std::byte* start = _elementWritePtr;

    std::memcpy(start, &tag, tagSize);
    std::memcpy(start + tagSize, &value, sizeof(T));
    _elementWritePtr += tagSize + sizeof(T);

    return recordView(start);
}

ListElementView ListWriteCursor::recordView(const std::byte* elementStart) {
    const ListElementView view(elementStart);

    //Copy the ListElementView we have constructed from the preallocated list buffer into
    //the ListElement view buffer
    *_viewWritePtr = view;
    ++_viewWritePtr;
    ++_written;

    return view;
}

namespace db {
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::String::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::Embedding::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const ListView&);

// The fixed-width elements. A decoder reading a wire whose layout already matches the
// stored one copies these through writeRaw instead; a caller holding the value itself,
// with no such buffer to copy from, writes it through here.
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::Int64::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::UInt64::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::Double::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const types::Bool::Primitive&);
template ListElementView ListWriteCursor::writeValue(ListBufferTypeTag, const PropertyNull&);
}
