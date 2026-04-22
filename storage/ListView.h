#pragma once

#include <span>

#include "ListElementView.h"

namespace db {

class ListView {
public:
    std::span<const ListElementView> elements() const { return _elements; }

    auto begin() { return std::begin(_elements); }
    auto end() { return std::end(_elements); }

    bool empty() const { return _elements.empty(); }

    size_t size() const { return _elements.size(); }

private:
    template <size_t N>
    friend class ListBuffer;

    explicit ListView(const ListElementView* data, size_t size)
        : _elements({data, size})
    {
    }

    std::span<const ListElementView> _elements;
};

}
