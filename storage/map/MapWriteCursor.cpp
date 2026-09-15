#include "MapWriteCursor.h"

#include <cstring>

#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

constexpr size_t keySize = sizeof(std::string_view);
constexpr size_t tagSize = sizeof(db::MapBufferTypeTag);

}

void MapWriteCursor::writeKey(std::string_view key) {
    std::memcpy(_entryWritePtr, &key, keySize);

    _keyPending = true;
}

template <typename T>
MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag tag, const T& value) {
    std::byte* valueStart = _entryWritePtr + keySize;

    std::memcpy(valueStart, &tag, tagSize);
    std::memcpy(valueStart + tagSize, &value, sizeof(T));

    return recordEntry(keySize + tagSize + sizeof(T));
}

MapEntryView MapWriteCursor::writeValueBytes(const void* data, size_t numBytes) {
    std::memcpy(_entryWritePtr + keySize, data, numBytes);

    return recordEntry(keySize + numBytes);
}

MapEntryView MapWriteCursor::recordEntry(size_t entryBytes) {
    const MapEntryView view(_entryWritePtr);

    *_viewWritePtr = view;
    ++_viewWritePtr;
    ++_written;

    _entryWritePtr += entryBytes;
    _keyPending = false;

    return view;
}

namespace db {
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::String::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::Embedding::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const ListView&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const MapView&);

// The fixed-width values. A decoder reading a wire whose layout already matches the stored one
// copies these through writeValueBytes instead; a caller holding the value itself, with no such
// buffer to copy from, writes it through here.
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::Int64::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::UInt64::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::Double::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const types::Bool::Primitive&);
template MapEntryView MapWriteCursor::writeValue(MapBufferTypeTag, const PropertyNull&);
}
