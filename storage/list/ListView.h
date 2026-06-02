#pragma once

#include <span>

#include "ListElementView.h"

namespace db {

/**
 * @brief Non-owning view into a series of contiguously-stored @ref ListElementViews.
 */
class ListView {
public:
    ListView() = default;

    std::span<const ListElementView> elements() const { return _elements; }

    auto begin() { return std::begin(_elements); }
    auto end() { return std::end(_elements); }

    auto begin() const { return std::begin(_elements); }
    auto end() const { return std::end(_elements); }

    bool empty() const { return _elements.empty(); }

    size_t size() const { return _elements.size(); }

    ListElementView front() const { return _elements.front(); }
    ListElementView back() const { return _elements.back(); }

    explicit operator bool() { return _elements.data() != nullptr; }

private:
    template <size_t N>
    friend class ListBuffer;

    ListView(const ListElementView* data, size_t size)
        : _elements({data, size})
    {
    }

    std::span<const ListElementView> _elements;
};
}
