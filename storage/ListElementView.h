#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "ListBufferByteTag.h"

namespace db {

class ListElementView {
public:
    ListElementView() = default;

    explicit ListElementView(const std::byte* data)
        : _tag(data)
    {
    }

    ListBufferTypeTag getTag() const {
        static_assert(sizeof(ListBufferTypeTag) == 1,
                      "TypeTag changed size: function may need modifying.");

        const auto tagValue = std::to_integer<uint8_t>(*_tag);

        return ListBufferTypeTag {tagValue};
    }

    template <typename T>
    T getAs() const;

private:
    /// Pointer to the tag of this element in a ListByteBuffer
    const std::byte* _tag {};
};

template <typename T>
T ListElementView::getAs() const {
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(sizeof(ListBufferTypeTag) == 1,
                  "Tag size changed: function may need modifying.");

    const std::byte* dataPtr = _tag + sizeof(ListBufferTypeTag);

    T out {};

    {
        T* outAddr = &out;
        constexpr size_t sizeOfT = sizeof(T);
        std::memcpy(outAddr, dataPtr, sizeOfT);
    }

    return out;
}

}
