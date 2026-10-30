#include "ListWriteCursor.h"

#include <cstring>

#include "PathView.h"
#include "list/ListBuffer.h"
#include "metadata/PropertyType.h"

using namespace db;

CellView ListWriteCursor::writeRaw(const void* data, size_t numBytes) {

    std::byte* start = _elementWritePtr;

    std::memcpy(start, data, numBytes);
    _elementWritePtr += numBytes;

    return recordView(start);
}

template <typename T>
CellView ListWriteCursor::writeValue(CellTag tag, const T& value) {
    constexpr size_t tagSize = sizeof(CellTag);

    std::byte* start = _elementWritePtr;

    std::memcpy(start, &tag, tagSize);
    std::memcpy(start + tagSize, &value, sizeof(T));
    _elementWritePtr += tagSize + sizeof(T);

    return recordView(start);
}

template <typename T>
CellView ListWriteCursor::writeValueAt(size_t index, CellTag tag, const T& value) {
    constexpr size_t tagSize = sizeof(CellTag);
    constexpr size_t elementSize = tagSize + sizeof(T);

    std::byte* start = _elementWritePtr + index * elementSize;

    std::memcpy(start, &tag, tagSize);
    std::memcpy(start + tagSize, &value, sizeof(T));

    const CellView view(start);
    _viewWritePtr[index] = view;
    ++_written;

    return view;
}

CellView ListWriteCursor::recordView(const std::byte* elementStart) {
    const CellView view(elementStart);

    //Copy the CellView we have constructed from the preallocated list buffer into
    //the cell view buffer
    *_viewWritePtr = view;
    ++_viewWritePtr;
    ++_written;

    return view;
}

namespace db {
template CellView ListWriteCursor::writeValue(CellTag, const types::String::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const types::Embedding::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const ListView&);
template CellView ListWriteCursor::writeValue(CellTag, const MapView&);
template CellView ListWriteCursor::writeValue(CellTag, const PathView&);

// The fixed-width elements. A decoder reading a wire whose layout already matches the
// stored one copies these through writeRaw instead; a caller holding the value itself,
// with no such buffer to copy from, writes it through here.
template CellView ListWriteCursor::writeValue(CellTag, const types::Int64::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const types::UInt64::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const types::Double::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const types::Bool::Primitive&);
template CellView ListWriteCursor::writeValue(CellTag, const PropertyNull&);

template CellView ListWriteCursor::writeValueAt(size_t, CellTag, const EdgeID&);
template CellView ListWriteCursor::writeValueAt(size_t, CellTag, const NodeID&);
}
