#include "MapBuffer.h"

#include <type_traits>

#include "MapBufferTypeTag.h"
#include "MapByteBuffer.h"
#include "MapEntryView.h"
#include "MapUtils.h"

using namespace db;

template <size_t N>
MapView MapBuffer<N>::insert(std::span<const MapKeyValuePair> entries) {
    // For each entry, calculate the bytes taken by the key + tag + raw bytes of the value
    size_t numBytes = 0;
    {
        const auto sizeOf = [](auto&& typed) -> size_t { return sizeof(typed); };
        for (const MapKeyValuePair& entry : entries) {
            numBytes += sizeof(std::string_view);
            numBytes += MapByteBuffer<N>::tagSize();
            numBytes += std::visit(sizeOf, entry.value);
        }
    }

    const size_t numEntries = entries.size();

    // Ensure all the entries we are about to write are stored contiguously
    _entries.reserveContiguous(numBytes);
    // Ensure all the views we are about to write are stored contiguously
    _views.reserveContiguous(numEntries);

    // Before writing, calculate the address at which this map will start
    const MapEntryView* mapStart = _views.nextPtr();

    for (const MapKeyValuePair& entry : entries) {
        std::visit([this, &entry](auto&& typed) -> void {
            using T = std::decay_t<decltype(typed)>;

            const MapBufferTypeTag tag = TypeToMapBufferTag<T>::Tag;
            const MapEntryView view = _entries.write(entry.key, tag, typed);
            _views.write(view);
        }, entry.value);
    }

    return MapView {mapStart, numEntries};
}

template <size_t N>
MapWriteCursor MapBuffer<N>::reserveMap(size_t numEntries, size_t valueBytes) {
    constexpr size_t entryOverhead = sizeof(std::string_view) + MapByteBuffer<N>::tagSize();
    const size_t numBytes = numEntries * entryOverhead + valueBytes;

    // Reserve and commit both stores up front so the raw writes that follow never relocate the
    // bytes or the views: the cursor's pointers and MapView stay valid as it is filled in, and
    // any later reservation lands after this region rather than inside it.
    std::byte* entryWritePtr = _entries.reserveAndCommit(numBytes);
    MapEntryView* viewWritePtr = _views.reserveAndCommit(numEntries);

    return MapWriteCursor {
        MapView {viewWritePtr, numEntries},
        entryWritePtr,
        viewWritePtr,
    };
}

template <size_t N>
void MapBuffer<N>::clear() {
    _entries.clear();
    _views.clear();
}

namespace db {
template class MapBuffer<>;
}
