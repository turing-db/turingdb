#include "ListBuffer.h"

#include <type_traits>

#include "ListBufferTypeTag.h"
#include "ListByteBuffer.h"
#include "ListElementView.h"
#include "ListUtils.h"

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
ListView ListBuffer<N>::reserveList(size_t numElements, size_t valueBytes) {
    const size_t numBytes = numElements * ListByteBuffer<N>::tagSize() + valueBytes;

    // Reserve both stores up front so the appends that follow never relocate the bytes
    // or the views: the returned ListView stays valid as it is filled in.
    _elements.reserveContiguous(numBytes);
    _views.reserveContiguous(numElements);

    return ListView {_views.nextPtr(), numElements};
}

template <size_t N>
ListElementView ListBuffer<N>::appendElement(const ListItemVariant& element) {
    ListElementView appended;

    const auto write = [this, &appended](auto&& typed) -> void {
        using T = std::decay_t<decltype(typed)>;

        const ListBufferTypeTag tag = TypeToListBufferTag<T>::Tag;
        appended = _elements.write(tag, typed);
        _views.write(appended);
    };

    std::visit(write, element);

    return appended;
}

template <size_t N>
ListElementView ListBuffer<N>::appendRawElement(const void* data, size_t numBytes) {
    const ListElementView view = _elements.writeRaw(data, numBytes);
    _views.write(view);
    return view;
}

template <size_t N>
void ListBuffer<N>::clear() {
    _elements.clear();
    _views.clear();
}

namespace db {
template class ListBuffer<>;
}
