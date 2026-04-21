#pragma once

#include <stddef.h>

#include "ListByteBuffer.h"
#include "ListElementViewBuffer.h"

#include "metadata/PropertyType.h"

#include "TypeUtils.h"

namespace db {

template <size_t N = 4096>
class ListBuffer {
public:
    template <typename... Elements>
    void insert(const Elements&... elements);

private:
    ListByteBuffer<N> _buf;
    ListElementViewBuffer<N> _views;
};

template <size_t N>
template <typename... Elements>
void ListBuffer<N>::insert(const Elements&... elements) {
    constexpr size_t numBytes = (... + (ListByteBuffer<N>::tagSize() + sizeof(Elements)));
    constexpr size_t numElements = sizeof...(elements);

    _buf.reserveContiguous(numBytes);
    _views.reserveContiguous(numElements);

    (_views.write(_buf.write(tagFor<Elements>(), elements)), ...);
}

namespace {
constexpr std::tuple<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, types::String::Primitive,
                     types::Bool::Primitive, types::Embedding::Primitive> ListableTypes;
}

template <typename T>
concept Listable = TypeConcepts::InTuple<T, decltype(ListableTypes)>;
}


