#pragma once

#include <cstddef>
#include <stdint.h>

#include "ListView.h"
#include "ListElementView.h"
#include "ListBufferTypeTag.h"

namespace db {

/**
 * @brief Write cursor into a contiguous list region reserved by @ref ListBuffer::reserveList.
 *
 * Owns no storage: it holds the region's @ref ListView (for the caller to store) alongside
 * raw write pointers into the element-byte and view storage the reservation committed. Each
 * write copies one element straight into the region through those pointers and advances
 * them, so the caller fills the region without going through an append API that tracks the
 * write position internally.
 */
class ListWriteCursor {
public:
    ListWriteCursor() = default;
    ListWriteCursor(const ListView& view, std::byte* elementWritePtr, ListElementView* viewWritePtr)
        : _view(view),
        _elementWritePtr(elementWritePtr),
        _viewWritePtr(viewWritePtr)
    {
    }

    /// The view over the reserved region, for the caller to store on its column.
    const ListView& getView() const { return _view; }

    /// Number of elements written into the region so far.
    uint64_t getWritten() const { return _written; }

    /// True once every reserved element slot has been written.
    bool isComplete() const { return _written == _view.size(); }

    /**
     * @brief Copies a pre-formed [tag][value] element of @param numBytes bytes straight into
     * the region, records its view, advances the cursor, and returns the view. For
     * fixed-width types, whose wire layout already matches the stored layout.
     */
    ListElementView writeRaw(const void* data, size_t numBytes);

    /**
     * @brief Writes [tag][value] into the region, where @param value is the stored value
     * object (e.g. the string_view / span view of a variable-length element, whose payload
     * lives in a separate buffer), records its view, advances the cursor, returns the view.
     */
    template <typename T>
    ListElementView writeValue(ListBufferTypeTag tag, const T& value);

private:
    ListView _view;
    std::byte* _elementWritePtr {nullptr};
    ListElementView* _viewWritePtr {nullptr};
    uint64_t _written {0};

    /// Records the view of the element written at @param elementStart, advancing the view
    /// pointer and the written count, and returns it.
    ListElementView recordView(const std::byte* elementStart);
};

}
