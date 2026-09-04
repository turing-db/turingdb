#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "ListBufferTypeTag.h"

namespace db {

/**
 * @brief Non-owning view of an element in a @ref ListByteBuffer.
 */
class ListElementView {
public:
    ListElementView() = default;

    explicit ListElementView(const std::byte* data)
        : _tag(data)
    {
    }

    /// Returns the type tag of the viewed element.
    ListBufferTypeTag getTag() const {
        static_assert(sizeof(ListBufferTypeTag) == 1,
                      "TypeTag changed size: function may need modifying.");

        const auto tagValue = std::to_integer<uint8_t>(*_tag);

        return ListBufferTypeTag {tagValue};
    }

    /// Returns a pointer to the raw element bytes: the tag, followed by the value bytes.
    const std::byte* getData() const { return _tag; }

    /**
     * @brief Attempts to read the viewed element as a @param T.
     * @warn Does not verify that the viewed element is in fact a @param T.
     * @warn No bounds checking is performed on the read.
     */
    template <typename T>
    T getAs() const;

private:
    /// Pointer to the tag of this element in a ListByteBuffer
    const std::byte* _tag {nullptr};
};

template <typename T>
T ListElementView::getAs() const {
    static_assert(std::is_trivially_copyable_v<T>);

    constexpr size_t tagSize = sizeof(ListBufferTypeTag);
    static_assert(tagSize == 1, "Tag size changed: function may need modifying.");

    // @ref _tag points to the tag; the data is contiguous after the tag: read from there
    const std::byte* dataPtr = _tag + tagSize;

    T out {};

    {
        T* outAddr = &out;
        constexpr size_t sizeOfT = sizeof(T);
        std::memcpy(outAddr, dataPtr, sizeOfT);
    }

    return out;
}

}
