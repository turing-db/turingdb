#pragma once

#include <deque>
#include <ranges>

#include <stddef.h>
#include <stdint.h>

#include "metadata/PropertyType.h"

#include "TypeUtils.h"

namespace db {

static constexpr std::tuple<types::Int64::Primitive, types::Double::Primitive> ListableTypes;

template <typename T>
concept Listable = TypeConcepts::InTuple<std::decay_t<T>, decltype(ListableTypes)>;

class ListBufferElementView;
class ListView;

class ListBuffer {
public:
    using Storage = std::deque<std::byte>;

    using iterator = Storage::iterator;
    using const_iterator = Storage::const_iterator;

    enum ListBufferTag : uint8_t {
        Int = 0,
        Double,

        INVALID,
    };

    static_assert(sizeof(std::byte) == sizeof(ListBufferTag));

    static constexpr size_t tagSize = sizeof(ListBufferTag);
    static_assert(tagSize == 1);

    template <Listable L>
    ListBufferElementView insert(const L& listItem);

    template <std::ranges::forward_range R>
    ListView insert(const R& list);

private:
    Storage _buf;

    static constexpr size_t _tagSize = sizeof(ListBufferTag);
};

}
