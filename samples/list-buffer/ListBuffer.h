#pragma once

#include "metadata/PropertyType.h"
#include <deque>
#include <vector>

#include <stddef.h>
#include <stdint.h>

namespace db {

class ListBuffer {
public:
    static constexpr std::tuple<types::Int64::Primitive, types::Double::Primitive>
        ListableTypes;

    enum ListBufferTag : uint8_t {
        Int = 0,
        Double,

        INVALID,
    };

    static_assert(sizeof(std::byte) == sizeof(ListBufferTag));

    ListBufferTag getTag(size_t i);

    template<typename T>
    T get(size_t i, ListBufferTag tag);

    static constexpr size_t tagSize = sizeof(ListBufferTag);
    static_assert(tagSize == 1);

private:
    std::deque<std::byte> _buf;
};

template <typename T, typename... Ts>
concept AnyOf = (std::same_as<T, Ts> or ...);

template <typename T, typename Tuple>
concept InTuple = []<typename... Ts>(std::type_identity<std::tuple<Ts...>>) {
    return AnyOf<T, Ts...>;
}(std::type_identity<Tuple>{});

template <typename T>
concept Listable = InTuple<T, decltype(ListBuffer::ListableTypes)>;

class ListBufferElementView {
public:
    ListBufferElementView(std::byte* data, size_t size);

    ListBufferElementView() = default;

    template <Listable L>
    L get();

private:
    // Pointer to the beginning of the data in the owning ListBuffer (including tag)
    std::byte* _data {nullptr};
    // Size of the data in the owning ListBuffer (excluding tag)
    size_t _size {0};

    ListBuffer::ListBufferTag _tag {ListBuffer::ListBufferTag::INVALID};
};

class ListView {
public:

private:
    std::vector<ListBufferElementView> _elems;
};

}
