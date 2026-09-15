#pragma once

#include <cstddef>
#include <stdint.h>
#include <string_view>

#include "MapBufferTypeTag.h"
#include "MapEntryView.h"
#include "MapView.h"

namespace db {

/**
 * @brief Write cursor into a contiguous map region reserved by @ref MapBuffer::reserveMap.
 *
 * Owns no storage: it holds the region's @ref MapView (for the caller to store) alongside raw
 * write pointers into the entry-byte and view storage the reservation committed. Each entry is
 * written in two steps — @ref writeKey, then one of the value writers — because a decoder reads
 * an entry's key off the wire before the value that follows it. The entry's view is recorded,
 * and the cursor advances, only once that value lands.
 */
class MapWriteCursor {
public:
    MapWriteCursor() = default;
    MapWriteCursor(const MapView& view, std::byte* entryWritePtr, MapEntryView* viewWritePtr)
        : _view(view),
        _entryWritePtr(entryWritePtr),
        _viewWritePtr(viewWritePtr)
    {
    }

    /// The view over the reserved region.
    const MapView& getView() const { return _view; }

    /// Number of entries fully written into the region so far.
    uint64_t getWritten() const { return _written; }

    /// True once every reserved entry slot has been written.
    bool isComplete() const { return _written == _view.size(); }

    /// True when a key has been written and the value completing its entry has not.
    bool expectsValue() const { return _keyPending; }

    /**
     * @brief Writes the bytes of @param key into the open entry, leaving it open for the
     * corresponding value bytes.
     */
    void writeKey(std::string_view key);

    /**
     * @brief Writes [tag][value] into the open entry where the key has been written. Records the
     * entry's view, advances the cursor, returns the view.
     */
    template <typename T>
    MapEntryView writeValue(MapBufferTypeTag tag, const T& value);

    /**
     * @brief Copies @param numBytes bytes from a pre-formed [tag][value] straight into the open
     * entry. Records the entry's view, advances the cursor, and returns the view. Intended for
     * fixed-width types coming off the wire format whose layout already matches the stored layout.
     */
    MapEntryView writeValueBytes(const void* data, size_t numBytes);

private:
    MapView _view;
    std::byte* _entryWritePtr {nullptr};
    MapEntryView* _viewWritePtr {nullptr};
    uint64_t _written {0};
    bool _keyPending {false};

    /// Records the view of the entry starting at @ref _entryWritePtr, advances the write
    /// pointers past its @param entryBytes and the written count, and returns it.
    MapEntryView recordEntry(size_t entryBytes);
};

}
