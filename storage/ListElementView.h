#pragma once

#include <cstddef>
#include <cstring>
#include <type_traits>

#include "ListByteBuffer.h"

namespace db {

class ListElementView {
public:
    ListBufferTypeTag getTag();

    template <typename T>
    T getAs();

private:
    /// Pointer to the tag of this element in a ListByteBuffer
    std::byte* _tag {};
};


template <typename T>
T ListElementView::getAs() {
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(sizeof(ListBufferTypeTag) == 1,
                  "Tag size changed: function may need modifying.");

    const std::byte* dataPtr = _tag + sizeof(ListBufferTypeTag);

    T out {};

    {
        T* outAddr = &out;
        constexpr size_t sizeOfT = sizeof(T);
        std::memcpy(out, dataPtr, sizeOfT);
    }

    return out;
}

}
