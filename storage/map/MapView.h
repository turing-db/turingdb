#pragma once

#include <span>

#include "MapEntryView.h"

namespace db {

/**
 * @brief Non-owning view into a series of contiguously-stored @ref MapEntryViews.
 */
class MapView {
public:
    MapView() = default;

    std::span<const MapEntryView> entries() const { return _entries; }

    auto begin() { return std::begin(_entries); }
    auto end() { return std::end(_entries); }

    auto begin() const { return std::begin(_entries); }
    auto end() const { return std::end(_entries); }

    bool empty() const { return _entries.empty(); }

    size_t size() const { return _entries.size(); }

    MapEntryView front() const { return _entries.front(); }
    MapEntryView back() const { return _entries.back(); }

    explicit operator bool() { return _entries.data() != nullptr; }

private:
    template <size_t N>
    friend class MapBuffer;

    MapView(const MapEntryView* data, size_t size)
        : _entries({data, size})
    {
    }

    std::span<const MapEntryView> _entries;
};

}
