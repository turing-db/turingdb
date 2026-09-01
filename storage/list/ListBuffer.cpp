#include "ListBuffer.h"

#include <type_traits>

#include "ListBufferTypeTag.h"
#include "ListByteBuffer.h"
#include "ListElementView.h"
#include "ListUtils.h"

using namespace db;

namespace {

struct ElementValueSize {
    template <typename T>
    size_t operator()(const ListElementView&) const {
        return sizeof(T);
    }
};

size_t elementValueSize(const ListElementView element) {
    const ListTagDispatcher dispatcher {element.getTag()};

    return dispatcher.execute(ElementValueSize {}, element);
}

}

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
ListWriteCursor ListBuffer<N>::reserveList(size_t numElements, size_t valueBytes) {
    const size_t numBytes = numElements * ListByteBuffer<N>::tagSize() + valueBytes;

    // Reserve and commit both stores up front so the raw writes that follow never relocate
    // the bytes or the views: the cursor's pointers and ListView stay valid as it is filled
    // in, and any later reservation lands after this region rather than inside it.
    std::byte* elementWritePtr = _elements.reserveAndCommit(numBytes);
    ListElementView* viewWritePtr = _views.reserveAndCommit(numElements);

    return ListWriteCursor {
        ListView {viewWritePtr, numElements},
        elementWritePtr,
        viewWritePtr,
    };
}

template <size_t N>
ListView ListBuffer<N>::concatenate(ListView a, ListView b) {
    const size_t numElements = a.size() + b.size();

    size_t valueBytes = 0;
    for (const ListElementView element : a) {
        valueBytes += elementValueSize(element);
    }
    for (const ListElementView element : b) {
        valueBytes += elementValueSize(element);
    }

    ListWriteCursor cursor = reserveList(numElements, valueBytes);

    for (const ListElementView element : a) {
        const size_t numBytes = ListByteBuffer<N>::tagSize() + elementValueSize(element);
        cursor.writeRaw(element.getData(), numBytes);
    }
    for (const ListElementView element : b) {
        const size_t numBytes = ListByteBuffer<N>::tagSize() + elementValueSize(element);
        cursor.writeRaw(element.getData(), numBytes);
    }

    return cursor.getView();
}

template <size_t N>
void ListBuffer<N>::clear() {
    _elements.clear();
    _views.clear();
}

namespace db {
template class ListBuffer<>;
}
