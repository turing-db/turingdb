#pragma once

#include <deque>
#include <ranges>
#include <vector>

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

class ListBufferElementView {
public:
    ListBufferElementView(std::byte* data, size_t size);

    ListBufferElementView(ListBuffer::iterator begin,
                          ListBuffer::iterator end);

    ListBufferElementView() = default;

    template <Listable L>
    L getAs() const;

    ListBuffer::ListBufferTag getTag() const;

private:
    // Pointer to the beginning of the data in the owning ListBuffer (including tag)
    std::byte* _data {nullptr};
    // Size of the data in the owning ListBuffer (excluding tag)
    size_t _size {0};

    static constexpr size_t _listBufferTagSize = sizeof(ListBuffer::ListBufferTag);
};

template <typename T>
struct TypeToListBufferTag;

template <>
struct TypeToListBufferTag<types::Int64::Primitive> {
    static constexpr ListBuffer::ListBufferTag Tag = ListBuffer::ListBufferTag::Int;
};

template <>
struct TypeToListBufferTag<types::Double::Primitive> {
    static constexpr ListBuffer::ListBufferTag Tag = ListBuffer::ListBufferTag::Double;
};

class ListView {
public:
    void push_back(const ListBufferElementView& element) { _elems.push_back(element); }

    auto begin() { return std::begin(_elems); }
    auto end() { return std::end(_elems); }

private:
    std::vector<ListBufferElementView> _elems;
};

}
