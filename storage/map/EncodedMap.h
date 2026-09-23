#pragma once

#include <stddef.h>
#include <cstddef>
#include <span>
#include <vector>

#include "MapView.h"

namespace db {

class MapContainer;

/**
 * @brief Owning, self-describing encoding of a map value, the map counterpart of
 * @ref EncodedList.
 *
 * Entries are encoded sorted by key, whatever order the view held them in.
 */
class EncodedMap {
public:
    EncodedMap();
    explicit EncodedMap(MapView map);
    explicit EncodedMap(std::span<const std::byte> bytes);
    ~EncodedMap();

    EncodedMap(const EncodedMap& other);
    EncodedMap(EncodedMap&& other) noexcept;
    EncodedMap& operator=(const EncodedMap& other);
    EncodedMap& operator=(EncodedMap&& other) noexcept;

    /// Rebuilds the map in @param container, which owns everything the returned view sees
    MapView decodeInto(MapContainer& container) const;

    std::span<const std::byte> bytes() const { return _bytes; }
    size_t byteSize() const { return _bytes.size(); }

private:
    std::vector<std::byte> _bytes;
};

bool operator==(const EncodedMap& lhs, const EncodedMap& rhs);

}
