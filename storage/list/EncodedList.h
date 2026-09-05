#pragma once

#include <stddef.h>
#include <cstddef>
#include <span>
#include <vector>

#include "ListView.h"

namespace db {

class ListContainer;

/**
 * @brief Owning, self-describing encoding of a list value.
 *
 * A @ref ListView only points into the buffers that built it, so a list cannot be carried
 * out of them - past the end of the query that produced it, or onto disk - as a view. This
 * holds the whole list instead, nested lists and string and embedding payloads included,
 * as one byte sequence that copies like any value and decodes back into any
 * @ref ListContainer.
 */
class EncodedList {
public:
    EncodedList();
    explicit EncodedList(ListView list);
    explicit EncodedList(std::span<const std::byte> bytes);
    ~EncodedList();

    EncodedList(const EncodedList& other);
    EncodedList(EncodedList&& other) noexcept;
    EncodedList& operator=(const EncodedList& other);
    EncodedList& operator=(EncodedList&& other) noexcept;

    /// Rebuilds the list in @param container, which owns everything the returned view sees
    ListView decodeInto(ListContainer& container) const;

    std::span<const std::byte> bytes() const { return _bytes; }
    size_t byteSize() const { return _bytes.size(); }

private:
    std::vector<std::byte> _bytes;
};

}
