#include "ListBuffer.h"

#include <type_traits>

#include "ListBufferTypeTag.h"
#include "ListByteBuffer.h"
#include "ListElementView.h"

using namespace db;

template <size_t N>
ListView ListBuffer<N>::insert(std::span<const ListItemVariant> elements) {
    // For each element, calculate the bytes taken by the tag + the raw bytes of the type
    size_t numBytes = 0;
    {
        const auto sizeOf = [](auto&& typed) -> size_t { return sizeof(typed); };
        for (const ListItemVariant& item : elements) {
            numBytes += ListByteBuffer<N>::tagSize();
            numBytes += std::visit(sizeOf, item);
        }
    }

    const size_t numElements = elements.size();

    // Ensure all the elements we are about to write are stored contigously
    _elements.reserveContiguous(numBytes);
    // Ensure all the views we are about to write are stored contigously
    _views.reserveContiguous(numElements);

    // Before writing, calculate the address at which this list will start
    const ListElementView* listStart = _views.nextPtr();

    const auto write = [this](auto&& typed) -> void {
        using T = std::decay_t<decltype(typed)>;

        const ListBufferTypeTag tag = TypeToListBufferTag<T>::Tag;
        const ListElementView view = _elements.write(tag, typed);
        _views.write(view);
    };

    for (const ListItemVariant& item : elements) {
        std::visit(write, item);
    }

    return ListView {listStart, numElements};
}

template <size_t N>
void ListBuffer<N>::clear() {
    _elements.clear();
    _views.clear();
}

namespace db {
template class ListBuffer<>;
}
